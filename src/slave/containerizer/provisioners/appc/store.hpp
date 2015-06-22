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

#include "slave/flags.hpp"

#include "slave/containerizer/provisioners/appc.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

struct StoredImage {
  StoredImage(
      const std::string& _hash,
      const JSON::Object& _manifest,
      const std::string& _path)
    : hash(_hash), manifest(_manifest), path(_path) {}

  std::string hash;
  JSON::Object manifest;
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

  // Put an image into to the store. Returns the AppcImage containing
  // the manifest, hash of the image, and the path to the extracted
  // image.
  process::Future<AppcImage> put(const std::string& uri);

  // Get all images matched by name.
  process::Future<std::vector<AppcImage>> get(const std::string& name);

  // Get a specific image matched by name and hash.
  process::Future<Option<AppcImage>> get(
      const std::string& name,
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

  process::Future<AppcImage> put(const std::string& uri);

  process::Future<std::vector<AppcImage>> get(const std::string& name);

  process::Future<Option<AppcImage>> get(
      const std::string& name,
      const std::string& hash);

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

  process::Future<AppcImage> _put(
      const std::string& stage,
      const std::string& store);

  process::Future<std::string> untar(
      const std::string& stage,
      const std::string& store);

  const Flags flags;
  const std::string staging;
  const std::string storage;

  // name -> hash -> image.
  hashmap<std::string, hashmap<std::string, AppcImage>> images;

  Fetcher* fetcher;
};

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC_STORE__
