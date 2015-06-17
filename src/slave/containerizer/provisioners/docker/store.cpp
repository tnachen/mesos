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

#include "slave/containerizer/provisioners/docker/hash.hpp"
#include "slave/containerizer/provisioners/docker/store.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace slave {


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


Future<vector<DockerImage>> Store::get(const string& name)
{
  return dispatch(process.get(), &StoreProcess::get, name);
}


Future<Option<DockerImage>> Store::get(const string& name, const string& hash)
{
  return dispatch(process.get(), &StoreProcess::get, name, hash);
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
    return Error("Failure image uri is not a directory " + uri);
  }

  // Read repository json
  Try<string> repoPath = path::join("file:///", uri, "repositories");
  if (path.isError()) {
    return Error("Failure to create path to repository " + path.error());
  }

  Try<string> value = os::read(repoPath.get());
  if (value.isError()) {
    return Error("Failed to read repository JSON " + value.error());
  }

  Try<JSON::Object> json = JSON::parse<JSON::Object>(value.get());
  if (json.isError()) {
    return Error("Failed to parse JSON " + json.error());
  }

  Try<pair<string, string>> repoTag = parseTag(name);
  if (repoTag.isError()) {
    return Error("Failed to parse name into repo and tag " + repoTag.error());
  }

  Result<JSON::String> layerId =
    json.find<JSON::String>(repository + "." + tag);
  Try<string> layerUri = path::join("file:///", uri, layerId);
  if (layerUri.isError()) {
    return Error("Failed to create path to image layer" + layerUri.error());
  }

  return putImageLayer(layerUri)
    .then([=](const DockerLayer& layer) {
      Try<DockerImage> image = DockerImage(name, layer);
      if (image.isError()) {
        return Error("Docker Image constructor failed " + image.error());
      }

      images[name] = image;
      return image;
    });
}


Future<DockerLayer> StoreProcess::putLayer(const string& uri)
{
  Try<string> stage = os::mkdtemp(path::join(staging, "XXXXXX"));
  if (stage.isError()) {
    return Failure("Failed to create staging directory: " + stage.error());
  }

  return fetchLayer(uri, stage.get())
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


Future<DockerLayer> StoreProcess::storeLayer(const string& stage, const string& uri)
{
  Try<string> hash = os::basename(uri);
  if (hash.isError()) {
    return Error("Failed to determine hash for stored layer: " + hash.error());
  }

  // Rename the stage/XXX to store/hash.
  // Only rename if the store directory doesn't exist.
  string store = path::join(storage, hash);

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
    .then([=](const DockerLayer& layer) {
      if (layer.isError()) {
        Try<Nothing> rmdir = os::rmdir(store);
        if (rmdir.isError()) {
          LOG(WARNING) << "Failed to remove invalid layer in store "
                      << store << ": " << rmdir.error();
        }
        return Failure("Failed to identify layer in store: " + layer.error());
      }

      LOG(INFO) << "Stored layer with hash " << hash;

      layers[hash] = layer.get();

      return layer.get();
    });
}

Future<DockerLayer> entry(const string& store, const string& uri)
{
  Result<string> realpath = os::realpath(store);
  if (realpath.isError()) {
    return Error("Error in checking store path: " + realpath.error());
  } else if (realpath.isNone()) {
    return Error("StoreProcess path not found");
  }

  Try<string> hash = os::basename(realpath.get());
  if (hash.isError()) {
    return Error("Failed to determine hash for stored image: " + hash.error());

  Try<string> version = os::read(path::join(store, "VERSION"));
  if(version.isError()) {
    return Error ("Failed to determine version of json: " + version.error());
  }

  Try<string> manifest = os::read(path::join(store, "manifest"));
  if (manifest.isError()) {
    return Error("Failed to read manifest: " + manifest.error());
  }

  Try<JSON::Object> json = parse(manifest);
  if (json.isError()) {
    return Error("Failed to parse manifest");
  }

  Result<string> parentId = json.find("parent");
  if (parentId.isNone()) {
    return DockerLayer(hash, json.get(), path, version.get(), Nothing());
  } else if (parentId.isError()) {
    return Error("Failed to read parent of layer " + parentId.error));
  } else {
    Try<string> parentUri = os::join(os::dirname(uri), parentId);
    if (parentUri.isError()) {
      return Error("Failed to create parent layer uri " + parentUri.error());
    }

    return putLayer(parentUri)
      .onAny([=](const Future<DockerLayer>& parent) {
        if(parent.isReady()) {
          return DockerLayer(hash, json.get(), uri, version.get(), parent.get());
        } else if (parent.isFailed()) {
          return Error("Parent layer failed with failure " + parent.failure());
        } else if (parent.isDiscarded()) {
          return Error("Parent layer was discarded");
        }
      });
  }
}


Future<Option<DockerImage>> StoreProcess::get(const string& name)
{
  if (!images.contains(name)) {
    return None();
  }

  return images[name];
}


Future<Option<DockerLayer>> StoreProcess::get(const string& hash)
{
  if(!layers.contains(hash)) {
    return None();
  }

  return layers[hash];
}


Try<Nothing> StoreProcess::restore()
{
  // Remove anything in staging.
  Try<list<string>> entries = os::ls(staging);
  if (entries.isError()) {
    return Error("Failed to list storage entries: " + entries.error());
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
    return Error("Failed to list storage entries: " + entries.error());
  }

  foreach (const string& entry_, entries.get()) {
    string path = path::join(storage, entry_);
    if (!os::stat::isdir(path)) {
      LOG(WARNING) << "Unexpected entry in storage: " << entry_;
      continue;
    }
    Try<DockerLayer> layer = putLayer(path);
    if (image.isError()) {
      LOG(WARNING) << "Unexpected entry in storage: " << layer.error();
      continue;
    }

    LOG(INFO) << "Restored layer '" << layer.get().hash << "'";
    layers[layer.get().hash] = layer.get(); 
  }

  return Nothing();
}
} // namespace slave {
} // namespace internal {
} // namespace mesos {