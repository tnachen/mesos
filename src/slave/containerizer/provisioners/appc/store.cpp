/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <list>

#include <stout/os.hpp>

#include <process/defer.hpp>
#include <process/dispatch.hpp>

#include <glog/logging.h>

#include "slave/containerizer/fetcher.hpp"

#include "slave/containerizer/provisioners/appc.hpp"

#include "slave/containerizer/provisioners/appc/hash.hpp"
#include "slave/containerizer/provisioners/appc/store.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

Try<Owned<Store>> Store::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  Try<Owned<StoreProcess>> process = StoreProcess::create(flags, fetcher);
  if (process.isError()) {
    return Error("Failed to create store: " + process.error());
  }

  return Owned<Store>(new Store(process.get()));
}


Store::Store(Owned<StoreProcess> _process)
  : process(_process)
{
  process::spawn(CHECK_NOTNULL(process.get()));
}


Store::~Store()
{
  process::terminate(process.get());
  process::wait(process.get());
}


Future<Nothing> Store::recover(
    const list<mesos::slave::ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  return dispatch(process.get(), &StoreProcess::recover, states, orphans);
}


Future<StoredImage> Store::put(const string& source)
{
  return dispatch(process.get(), &StoreProcess::put, source);
}


Future<vector<StoredImage>> Store::get(const string& name)
{
  return dispatch(process.get(), &StoreProcess::get, name);
}


Future<Option<StoredImage>> Store::get(const string& name, const string& id)
{
  return dispatch(process.get(), &StoreProcess::get, name, id);
}


Try<Owned<StoreProcess>> StoreProcess::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  string staging = path::join(flags.appc_store_dir, "staging");
  Try<Nothing> mkdir = os::mkdir(staging);
  if (mkdir.isError()) {
    return Error("Failed to create staging directory: " + mkdir.error());
  }

  string storage = path::join(flags.appc_store_dir, "storage");
  mkdir = os::mkdir(storage);
  if (mkdir.isError()) {
    return Error("Failed to create storage directory: " + mkdir.error());
  }

  Owned<StoreProcess> store =
    Owned<StoreProcess>(new StoreProcess(flags, staging, storage, fetcher));

  return store;
}


StoreProcess::StoreProcess(
    const Flags& _flags,
    const string& _staging,
    const string& _storage,
    Fetcher* _fetcher)
  : flags(_flags),
    staging(_staging),
    storage(_storage),
    fetcher(_fetcher) {}


Future<StoredImage> StoreProcess::put(const string& uri)
{
  Try<string> stage = os::mkdtemp(path::join(staging, "XXXXXX"));
  if (stage.isError()) {
    return Failure("Failed to create staging directory: " + stage.error());
  }

  return fetch(uri, stage.get())
    .then(defer(self(), [=]() { return decrypt(stage.get()); }))
    .then(defer(self(), [=]() { return decompress(stage.get()); }))
    .then(defer(self(), [=]() { return untar(stage.get()); }))
    .then(defer(self(), [=]() { return hash(stage.get()); }))
    .then(defer(self(), [=](const string& hash) {
      return _put(stage.get(), hash);
    }))
    .onAny([stage]() { os::rmdir(stage.get()); });
}


Future<Nothing> StoreProcess::fetch(const string& uri, const string& stage)
{
  // Use the random staging name for the containerId
  ContainerID containerId;
  containerId.set_value(os::basename(stage).get());

  // Disable caching because this is effectively done by the store.
  CommandInfo::URI uri_;
  uri_.set_value(uri);
  uri_.set_extract(false);
  uri_.set_cache(false);

  CommandInfo commandInfo;
  commandInfo.add_uris()->CopyFrom(uri_);

  // The slaveId is only used for caching, which we disable, so just
  // use "store" for it.
  SlaveID slaveId;
  slaveId.set_value("store");

  return fetcher->fetch(
      containerId,
      commandInfo,
      stage,
      None(),
      slaveId,
      flags)
    .then([=]() -> Future<Nothing> {
      // Rename the fetched object to image.aci.
      Try<string> name = Fetcher::basename(uri);
      if (name.isError()) {
        return Failure("Failed to get basename for uri '" +
                       uri + "': " + name.error());
      }

      Try<Nothing> rename = os::rename(
        path::join(stage, name.get()),
        path::join(stage, "image.aci"));

      if (rename.isError()) {
        return Failure("Failed to rename fetched image");
      }

      LOG(INFO) << "Fetched image '" + uri + "'";
      return Nothing();
    });
}


Future<Nothing> StoreProcess::decrypt(const string& stage)
{
  LOG(WARNING) << "Decryption not yet implemented,"
               << " assuming image is not encrypted";

  return Nothing();
}


Future<Nothing> StoreProcess::decompress(const string& stage)
{
  Future<Result<string>> chain = None();

  vector<string> formats{"gz", "bzip2", "xz"};

  foreach (const string& format, formats) {
    chain = chain
      .then([=](const Result<string>& decompressed) -> Future<Result<string>> {
        if (decompressed.isSome()) {
          // Decompression succeeded.
          return decompressed;
        }

        return decompress(stage, format);
      });
  }

  return chain
    .then([stage](const Result<string>& decompressed) -> Future<Nothing> {
      // Assume the image is not compressed.
      if (decompressed.isNone()) {
        return Nothing();
      }

      if (decompressed.isError()) {
        LOG(WARNING) << "Error decompressing image in " << stage
                     << ", attempting to continue with image";
      }

      if (decompressed.isSome()) {
        // Rename the decompressed image back to image.aci.
        Try<Nothing> rename = os::rename(
          decompressed.get(), path::join(stage, "image.aci"));

        if (rename.isError()) {
          return Failure("Failed to rename decompressed image: " +
                          rename.error());
        }
      }

      return Nothing();
    });
}


Future<Result<string>> StoreProcess::decompress(
    const string& stage,
    const string& format)
{
  const string path = path::join(stage, "image.aci");

  string command;
  vector<string> argv;
  // Name of the decompression output, if it succeeds.
  string output;

  if (format == "gz") {
    command = "gzip";
    argv = {"gzip", "-S", ".aci", "-d", path};
    output = path::join(stage, "image");
  } else if (format == "bzip2") {
    command = "bzip2";
    argv = {"bzip2", "-d", path};
    output = path::join(stage, "image.aci.out");
  } else if (format == "xz") {
    command = "xz";
    argv = {"xz", "-d", "-S", ".aci", path};
    output = path::join(stage, "image");
  } else {
    return Failure("Unsupported compression format '" + format + "'");
  }

  Try<Subprocess> s = subprocess(
      command,
      argv,
      Subprocess::PATH("/dev/null"),
      Subprocess::PATH("/dev/null"),
      Subprocess::PATH("/dev/null"));

  if (s.isError()) {
    return Failure("Failed to create " + format +
                   " decompression subprocess: " + s.error());
  }

  return s.get().status()
    .then([=](const Option<int>& status) -> Future<Result<string>> {
      if (status.isSome() && status.get() == 0) {
        LOG(INFO) << "Decompressed " << format << " image in " << stage;
        return output;
      }

      if (status.isNone()) {
        return Failure("Failed to reap " + format +
                       " decompression subprocess");
      }

      return None();
    });
}


Future<string> StoreProcess::hash(const string& stage)
{
  const string path = path::join(stage, "image.aci");

  CHECK(os::exists(path));

  return SHA512::hash(path);
}


Future<Nothing> StoreProcess::untar(const string& stage)
{
  // Untar stage/image.aci into stage/.
  vector<string> argv = {
    "tar",
    "-C",
    stage,
    "-x",
    "-f",
    path::join(stage, "image.aci")};

  Try<Subprocess> s = subprocess(
      "tar",
      argv,
      Subprocess::PATH("/dev/null"),
      Subprocess::PATH("/dev/null"),
      Subprocess::PATH("/dev/null"));

  if (s.isError()) {
    return Failure("Failed to create tar subprocess: " + s.error());
  }


  return s.get().status()
    .then([=](const Option<int>& status) -> Future<Nothing> {
      if (status.isNone()) {
        return Failure("Failed to reap status for tar subprocess in " +
                        stage);
      }

      if (status.isSome() && status.get() != 0) {
        return Failure("Non-zero exit for tar subprocess: " +
                       stringify(status.get()) + " in " + stage);
      }

      return Nothing();
    });
}


static Try<StoredImage> entry(const string& store)
{
  Result<string> realpath = os::realpath(store);
  if (realpath.isError()) {
    return Error("Error in checking store path: " + realpath.error());
  } else if (realpath.isNone()) {
    return Error("StoreProcess path not found");
  }

  Try<string> hash = os::basename(realpath.get());
  if (hash.isError()) {
    return Error("Failed to determine id for stored image: " + hash.error());
  }

  Try<string> read = os::read(path::join(store, "manifest"));
  if (read.isError()) {
    return Error("Failed to read manifest: " + read.error());
  }

  Try<AppcImageManifest> manifest = parse(read.get());
  if (manifest.isError()) {
    return Error("Failed to parse manifest: " + manifest.error());
  }

  return StoredImage(manifest.get(), "sha512-" + hash.get(), store);
}


Future<StoredImage> StoreProcess::_put(const string& stage, const string& hash)
{
  // Rename the stage/XXX to store/hash.
  // Only rename if the store directory doesn't exist.
  string store = path::join(storage, hash);

  if (os::exists(store)) {
    LOG(INFO) << "Image store '" << store << "' exists, skipping rename";
  } else {
    Try<Nothing> rename = os::rename(stage, store);
    if (rename.isError()) {
      return Failure("Failed to rename staged image directory: " +
                     rename.error());
    }
  }

  Try<string> read = os::read(path::join(store, "manifest"));
  if (read.isError()) {
    // XXX rm store.
    return Failure("Failed to read manifest: " + read.error());
  }

  Try<AppcImageManifest> manifest = parse(read.get());
  if (manifest.isError()) {
    return Failure(manifest.error());
  }

  StoredImage image(manifest.get(), "sha512-" + hash, store);

  images[manifest.get().name()].put(hash, image);

  LOG(INFO) << "Stored image with name '" << manifest.get().name() << "'"
            << " and hash '" << hash << "'";

  return image;
}


Future<vector<StoredImage>> StoreProcess::get(const string& name)
{
  if (!images.contains(name)) {
    return vector<StoredImage>();
  }

  vector<StoredImage> images_;
  foreach (const StoredImage& image, images[name].values()) {
    images_.push_back(image);
  }

  return images_;
}


Future<Option<StoredImage>> StoreProcess::get(
    const string& name,
    const string& id)
{
  if (!images.contains(name)) {
    return None();
  }

  return images[name].get(id);
}


Future<Nothing> StoreProcess::recover(
    const list<mesos::slave::ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  // Remove anything in staging.
  Try<list<string>> entries = os::ls(staging);
  if (entries.isError()) {
    return Failure("Failed to list storage entries: " + entries.error());
  }

  foreach (const string& entry, entries.get()) {
    const string path = path::join(staging, entry);

    Try<Nothing> rm = (os::stat::isdir(path) ? os::rmdir(path) : os::rm(path));
    if (rm.isError()) {
      LOG(WARNING) << "Failed to remove " << path;
    }
  }

  // Recover everything in storage.
  entries = os::ls(storage);
  if (entries.isError()) {
    return Failure("Failed to list storage entries: " + entries.error());
  }

  foreach (const string& entry_, entries.get()) {
    string path = path::join(storage, entry_);
    if (!os::stat::isdir(path)) {
      LOG(WARNING) << "Unexpected entry in storage: " << entry_;
      continue;
    }

    Try<StoredImage> image = entry(path);
    if (image.isError()) {
      LOG(WARNING) << "Unexpected entry in storage: " << image.error();
      continue;
    }

    LOG(INFO) << "Restored image '" << image.get().manifest.name() << "'";

    images[image.get().manifest.name()].put(image.get().id, image.get());
  }

  return Nothing();
}

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
