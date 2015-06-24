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

#ifndef __MESOS_DOCKER_STORE__
#define __MESOS_DOCKER_STORE__

#include <string>
#include <vector>

#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

#include <process/future.hpp>
#include <process/process.hpp>
#include <process/shared.hpp>

#include "slave/containerizer/provisioners/docker.hpp"
#include "slave/flags.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace docker {

// Forward declaration.
class StoreProcess;

class Store
{
public:
  static Try<process::Owned<Store>> create(
      const Flags& flags,
      Fetcher* fetcher);

  ~Store();

  // Put an image into to the store. Returns the DockerImage containing
  // the manifest, hash of the image, and the path to the extracted
  // image.
  process::Future<DockerImage> put(
      const std::string& uri,
      const std::string& name,
      const std::string& directory);

  // Get image by name.
  process::Future<Option<DockerImage>> get(const std::string& name);

  // Get a specific layer matched by hash.
  process::Future<Option<process::Shared<DockerLayer>>> getLayer(
      const std::string& hash);

private:
  Store(process::Owned<StoreProcess> process);

  Store(const Store&); // Not copyable.
  Store& operator=(const Store&); // Not assignable.

  process::Owned<StoreProcess> process;
};


class StoreProcess : public process::Process<StoreProcess>
{
public:
  ~StoreProcess() {}

  static Try<process::Owned<StoreProcess>> create(
      const Flags& flags,
      Fetcher* fetcher);

  process::Future<DockerImage> put(
      const std::string& uri,
      const std::string& name,
      const std::string& directory);

  process::Future<Option<DockerImage>> get(const std::string& name);

  process::Future<Option<process::Shared<DockerLayer>>> getLayer(
      const std::string& hash);

private:
  StoreProcess(
      const Flags& flags,
      const std::string& staging,
      const std::string& storage,
      Fetcher* fetcher);

  Try<Nothing> restore();

  process::Future<process::Shared<DockerLayer>> putLayer(
      const std::string& uri,
      const std::string& directory);

  process::Future<process::Shared<DockerLayer>> untarLayer(
      const std::string& store,
      const std::string& uri,
      const process::Shared<DockerLayer>& layer);

  process::Future<process::Shared<DockerLayer>> storeLayer(
      const std::string& hash,
      const std::string& store,
      const std::string& uri,
      const std::string& directory);

  process::Future<process::Shared<DockerLayer>> entry(
      const std::string& store,
      const std::string& uri,
      const std::string& directory);

  const Flags flags;
  const std::string staging;
  const std::string storage;

  // name -> DockerImage
  hashmap<std::string, DockerImage> images;
  // hash -> DockerLayer
  hashmap<std::string, process::Shared<DockerLayer>> layers;

  Fetcher* fetcher;
};

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_DOCKER_STORE__
