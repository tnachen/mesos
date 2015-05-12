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

#ifndef __MESOS_APPC_STORE__
#define __MESOS_APPC_STORE__

#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/result.hpp>
#include <stout/try.hpp>

#include <process/future.hpp>
#include <process/process.hpp>

#include <string>
#include <vector>

#include <mesos/resources.hpp>

#include "slave/flags.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

struct StoredImage {
  StoredImage(
      const AppcImageManifest& _manifest,
      const std::string& _id,
      const std::string& _path)
    : manifest(_manifest), id(_id), path(_path) {}

  AppcImageManifest manifest;
  std::string id;
  std::string path;
};


// Forward declaration.
class StoreProcess;

class Store
{
public:
  static Try<process::Owned<Store>> create(
      const Flags& flags,
      Fetcher* fetcher);

  ~Store();

  process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);


  // Put an image into to the store. Returns the AppcImage containing
  // the manifest, id of the image, and the path to the extracted
  // image.
  process::Future<StoredImage> put(const std::string& uri);

  // Get all images matched by name.
  process::Future<std::vector<StoredImage>> get(const std::string& name);

  // Get a specific image matched by name and id.
  process::Future<Option<StoredImage>> get(
      const std::string& name,
      const std::string& id);

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

  process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  process::Future<StoredImage> put(const std::string& uri);

  process::Future<std::vector<StoredImage>> get(const std::string& name);

  process::Future<Option<StoredImage>> get(
      const std::string& name,
      const std::string& id);

private:
  StoreProcess(
      const Flags& flags,
      const std::string& staging,
      const std::string& storage,
      Fetcher* fetcher);

  Try<Nothing> restore();

  process::Future<Nothing> fetch(
      const std::string& uri,
      const std::string& stage);

  process::Future<Nothing> decrypt(const std::string& stage);

  process::Future<Nothing> decompress(const std::string& stage);

  process::Future<Result<std::string>> decompress(
      const std::string& stage,
      const std::string& format);

  process::Future<std::string> hash(const std::string& stage);

  process::Future<StoredImage> _put(
      const std::string& stage,
      const std::string& hash);

  process::Future<Nothing> untar(const std::string& stage);

  const Flags flags;
  const std::string staging;
  const std::string storage;

  // name -> id -> image.
  hashmap<std::string, hashmap<std::string, StoredImage>> images;

  Fetcher* fetcher;
};

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC_STORE__
