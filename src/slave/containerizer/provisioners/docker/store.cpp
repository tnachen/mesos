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


Future<DockerImage> Store::put(
    const string& uri,
    const string& name,
    const string& directory)
{
  return dispatch(process.get(), &StoreProcess::put, uri, name, directory);
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


Future<DockerImage> StoreProcess::put(
    const string& uri,
    const string& name,
    const string& directory)
{
  string imageUri = uri;
  if (strings::startsWith(imageUri, "file://")) {
    imageUri = imageUri.substr(7);
  }

  Try<bool> isDir = os::stat::isdir(imageUri);
  if (isDir.isError()) {
    return Failure("Failed to check directory for uri '" +imageUri + "':"
                   + isDir.error());
  } else if (!isDir.get()) {
    return Failure("Docker image uri '" + imageUri + "' is not a directory");
  }

  // Read repository json
  Try<string> repoPath = path::join(imageUri, "repositories");
  if (repoPath.isError()) {
    return Failure("Failed to create path to repository: " + repoPath.error());
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

  Try<string> layerUri = path::join(imageUri, layerId.get().value);
  if (layerUri.isError()) {
    return Failure("Failed to create path to image layer: " + layerUri.error());
  }

  return putLayer(layerUri.get(), directory)
    .then([=](const Shared<DockerLayer>& layer) -> Future<DockerImage> {
      DockerImage image(name, layer);
      images[name] = image;
      return image;
    });
}


Future<Shared<DockerLayer>> StoreProcess::putLayer(
    const string& uri,
    const string& directory)
{
  Try<string> hash = os::basename(uri);
  if (hash.isError()) {
    return Failure("Failed to determine hash for stored layer: " +
                    hash.error());
  }

  string store = path::join(storage, hash.get());

  return storeLayer(hash.get(), store, uri, directory)
    .then(defer(self(),
                &Self::untarLayer,
                store,
                uri,
                lambda::_1));
}


Future<Shared<DockerLayer>> StoreProcess::untarLayer(
    const string& store,
    const string& uri,
    const Shared<DockerLayer>& layer)
{
  string rootFs = path::join(store, "rootfs");

  if (os::exists(rootFs)) {
    return layer;
  }

  // Untar stage/hash/layer.tar into stage/hash/rootfs.
  vector<string> argv = {
    "tar",
    "-C",
    rootFs,
    "-x",
    "-f",
    path::join(store, "layer.tar")};

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
    .then([=](const Option<int>& status) -> Future<Shared<DockerLayer>> {
        if (status.isNone()) {
          return Failure("Failed to reap status for tar subprocess in " +
                          store);
        }

        if (status.isSome() && status.get() != 0) {
          return Failure("Non-zero exit for tar subprocess: " +
                         stringify(status.get()) + " in " + store);
        }

        return layer;
      });
}


Future<Shared<DockerLayer>> StoreProcess::storeLayer(
    const string& hash,
    const string& store,
    const string& uri,
    const string& directory)
{
  // Copy the layer from uri to store/hash.
  // Only copy if the store directory doesn't exist.
  Future<Option<int>> status;
  if (os::exists(store)) {
    LOG(INFO) << "Layer store '" << store << "' exists, skipping rename";
    status = 0;
  } else {
    Try<int> out = os::open(
        path::join(directory, "stdout"),
        O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    if (out.isError()) {
      return Failure("Failed to create 'stdout' file: " + out.error());
    }

    // Repeat for stderr.
    Try<int> err = os::open(
        path::join(directory, "stderr"),
        O_WRONLY | O_CREAT | O_TRUNC | O_NONBLOCK | O_CLOEXEC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    if (err.isError()) {
      os::close(out.get());
      return Failure("Failed to create 'stderr' file: " + err.error());
    }

    vector<string> argv{
      "cp",
      "--archive",
      path::join(uri, "rootfs"),
      store
    };

    VLOG(1) << "Copying image with command: " << strings::join(" ", argv);

    Try<Subprocess> s = subprocess(
      "cp",
      argv,
      Subprocess::PATH("/dev/null"),
      Subprocess::FD(out.get()),
      Subprocess::FD(err.get()));

    if (s.isError()) {
      return Failure("Failed to create 'cp' subprocess: " + s.error());
    }

    status = s.get().status();
  }

  return status
    .then([=](const Option<int>& status) -> Future<Shared<DockerLayer>> {
      if (status.isNone()) {
        return Failure("Failed to reap subprocess to copy image");
      } else if (status.get() != 0) {
        return Failure("Non-zero exit from subprocess to copy image: " +
                       stringify(status.get()));
      }

      return entry(store, uri, directory);
    })
    .then([=](const Shared<DockerLayer>& layer) {
      LOG(INFO) << "Stored layer with hash: " << hash;
      layers[hash] = layer;

      return layer;
    });
}

Future<Shared<DockerLayer>> StoreProcess::entry(
    const string& store,
    const string& uri,
    const string& directory)
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
    return Shared<DockerLayer>(new DockerLayer(
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

  return putLayer(parentUri.get(), directory)
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
