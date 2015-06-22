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
#include <utility>

#include <stout/os.hpp>
#include <stout/json.hpp>

#include <process/defer.hpp>
#include <process/dispatch.hpp>

#include <glog/logging.h>

#include "slave/containerizer/fetcher.hpp"

#include "slave/containerizer/provisioners/docker/store.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;
using std::pair;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {


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


Future<DockerImage> Store::put(const string& uri, const string& name)
{
  return dispatch(process.get(), &StoreProcess::put, uri, name);
}


Future<Option<DockerImage>> Store::get(const string& name)
{
  return dispatch(process.get(), &StoreProcess::get, name);
}


Future<Option<Shared<DockerLayer>>> Store::getLayer(const string& hash)
{
  return dispatch(process.get(), &StoreProcess::getLayer, hash);
}


Try<Owned<StoreProcess>> StoreProcess::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  string staging = path::join(flags.provisioner_store_dir, "staging");
  Try<Nothing> mkdir = os::mkdir(staging);
  if (mkdir.isError()) {
    return Error("Failed to create staging directory: " + mkdir.error());
  }

  string storage = path::join(flags.provisioner_store_dir, "storage");
  mkdir = os::mkdir(storage);
  if (mkdir.isError()) {
    return Error("Failed to create storage directory: " + mkdir.error());
  }

  Owned<StoreProcess> store =
    Owned<StoreProcess>(new StoreProcess(flags, staging, storage, fetcher));

  Try<Nothing> restore = store->restore();
  if (restore.isError()) {
    return Error("Failed to restore store: " + restore.error());
  }

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


Future<DockerImage> StoreProcess::put(const string& uri, const string& name)
{
  Try<bool> isDir = os::stat::isdir(uri);
  if (!isDir.isSome() || isDir.get() == false) {
    return Failure("Failure image uri is not a directory " + uri);
  }

  // Read repository json
  Try<string> repoPath = path::join("file:///", uri, "repositories");
  if (repoPath.isError()) {
    return Failure("Failure to create path to repository: " + repoPath.error());
  }

  Try<string> value = os::read(repoPath.get());
  if (value.isError()) {
    return Failure("Failed to read repository JSON: " + value.error());
  }

  Try<JSON::Object> json = JSON::parse<JSON::Object>(value.get());
  if (json.isError()) {
    return Failure("Failed to parse JSON: " + json.error());
  }

  Try<pair<string, string>> repoTag = DockerImage::parseTag(name);
  if (repoTag.isError()) {
    return Failure("Failed to parse Docker image name: " + repoTag.error());
  }

  string repository = repoTag.get().first;
  string tag = repoTag.get().second;

  Result<JSON::String> layerId =
    json.get().find<JSON::String>(repository + "." + tag);
  if (layerId.isError()) {
    return Failure(
      "Failed to find layer id of image layer: " + layerId.error());
  }

  Try<string> layerUri = path::join("file:///", uri, layerId.get().value);
  if (layerUri.isError()) {
    return Failure("Failed to create path to image layer: " + layerUri.error());
  }

  return putLayer(layerUri.get())
    .then([=](const Shared<DockerLayer>& layer) -> Future<DockerImage> {
      DockerImage image = DockerImage(name, layer);
      images[name] = image;
      return image;
    });
}


Future<Shared<DockerLayer>> StoreProcess::putLayer(const string& uri)
{
  Try<string> stage = os::mkdtemp(path::join(staging, "XXXXXX"));
  if (stage.isError()) {
    return Failure("Failed to create staging directory: " + stage.error());
  }

  return fetchLayer(uri, stage.get())
    .then(defer(self(),
                &Self::untarLayer,
                stage.get(),
                uri))
    .then(defer(self(),
                &Self::storeLayer,
                stage.get(),
                uri))
    .onAny([stage]() { os::rmdir(stage.get()); });
}


Future<Nothing> StoreProcess::fetchLayer(const string& stage, const string& uri)
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
        LOG(INFO) << "Fetched layer '" + uri + "'";
        return Nothing();
        });
}


Future<Nothing> StoreProcess::untarLayer(
    const string& stage,
    const string& uri)
{
  Try<string> hash = os::basename(uri);
  if (hash.isError()) {
    return Failure("Failed to determine hash from layer uri: " + hash.error());
  }
  // Untar stage/hash/layer.tar into stage/layer/.
  vector<string> argv = {
    "tar",
    "-C",
    path::join(stage, hash.get(), "layer"),
    "-x",
    "-f",
    path::join(stage, hash.get(), "layer.tar")};

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
        LOG(INFO) << "Untarred layer in " + stage;
        return Nothing();
      });
}


Future<Shared<DockerLayer>> StoreProcess::storeLayer(
    const string& stage,
    const string& uri)
{
  Try<string> hash = os::basename(uri);
  if (hash.isError()) {
    return Failure("Failed to determine hash for stored layer: " +
                    hash.error());

  }

  // Rename the stage/XXX to store/hash.
  // Only rename if the store directory doesn't exist.
  string store = path::join(storage, hash.get());

  if (os::exists(store)) {
    LOG(INFO) << "Layer store '" << store << "' exists, skipping rename";
  } else {
    Try<Nothing> rename = os::rename(stage, store);
    if (rename.isError()) {
      return Failure("Failed to rename staged layer directory: " +
                    rename.error());
    }
  }

  return entry(store, uri)
    .then([=](const Shared<DockerLayer>& layer) {
      LOG(INFO) << "Stored layer with hash: " << hash.get();

      layers[hash.get()] = layer;

      return layer;
    });
}

Future<Shared<DockerLayer>> StoreProcess::entry(
    const string& store,
    const string& uri)
{
  Result<string> realpath = os::realpath(store);
  if (realpath.isError()) {
    return Failure("Error in checking store path: " + realpath.error());
  } else if (realpath.isNone()) {
    return Failure("StoreProcess path not found");
  }

  Try<string> hash = os::basename(realpath.get());
  if (hash.isError()) {
    return Failure(
      "Failed to determine hash for stored image: " + hash.error());
  }

  Try<string> version = os::read(path::join(store, "VERSION"));
  if(version.isError()) {
    return Failure("Failed to determine version of json: " + version.error());
  }

  Try<string> manifest = os::read(path::join(store, "manifest"));
  if (manifest.isError()) {
    return Failure("Failed to read manifest: " + manifest.error());
  }

  Try<JSON::Object> json = JSON::parse<JSON::Object>(manifest.get());
  if (json.isError()) {
    return Failure("Failed to parse manifest: " + json.error());
  }

  Result<JSON::String> parentId = json.get().find<JSON::String>("parent");
  if (parentId.isNone()) {
    return Shared<DockerLayer> (new DockerLayer(
        hash.get(),
        json.get(),
        realpath.get(),
        version.get(),
        None()));
  } else if (parentId.isError()) {
    return Failure("Failed to read parent of layer: " + parentId.error());
  }

  Try<string> uriDir = os::dirname(uri);
  if (uriDir.isError()) {
    return Failure("Failed to obtain layer directory: " + uriDir.error());
  }

  Try<string> parentUri = path::join(uriDir.get(), parentId.get().value);
  if (parentUri.isError()) {
    return Failure("Failed to create parent layer uri: " + parentUri.error());
  }

  return putLayer(parentUri.get())
    .then([=](const Shared<DockerLayer>& parent) -> Shared<DockerLayer> {
        return Shared<DockerLayer> (new DockerLayer(
            hash.get(),
            json.get(),
            uri,
            version.get(),
            parent));
    });
}


Future<Option<DockerImage>> StoreProcess::get(const string& name)
{
  if (!images.contains(name)) {
    return None();
  }

  return images[name];
}


Future<Option<Shared<DockerLayer>>> StoreProcess::getLayer(const string& hash)
{
  if (!layers.contains(hash)) {
    return None();
  }

  return layers[hash];
}


Try<Nothing> StoreProcess::restore()
{
  // TODO(chenlily): implement restore
  return Nothing();
}

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {