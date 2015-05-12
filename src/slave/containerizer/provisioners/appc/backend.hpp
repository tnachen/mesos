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

#ifndef __MESOS_APPC_BACKEND__
#define __MESOS_APPC_BACKEND__

#include <string>
#include <vector>

#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include <process/future.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/provisioners/appc.hpp"

namespace mesos {
namespace internal {
namespace slave {

// Provision a root filesystem for a container, built from the
// specified layers.
class Backend
{
public:
  virtual ~Backend() {}

  static Try<process::Owned<Backend>> create(const Flags& flags);

  // Provision a root filesystem for a container using the specified
  // image.
  virtual process::Future<Nothing> provision(
      const std::vector<AppcImage>& images,
      const std::string& directory) = 0;

  // Destroy the root filesystem at the specified path.
  virtual process::Future<Nothing> destroy(const std::string& directory) = 0;
};


// Forward declaration.
class CopyBackendProcess;

class CopyBackend : public Backend
{
public:
  virtual ~CopyBackend();

  static Try<process::Owned<Backend>> create(const Flags& flags);

  virtual process::Future<Nothing> provision(
      const std::vector<AppcImage>& images,
      const std::string& directory);

  virtual process::Future<Nothing> destroy(const std::string& directory);

private:
  explicit CopyBackend(process::Owned<CopyBackendProcess> process);

  CopyBackend(const CopyBackend&); // Not copyable.
  CopyBackend& operator=(const CopyBackend&); // Not assignable.

  process::Owned<CopyBackendProcess> process;
};


class CopyBackendProcess : public process::Process<CopyBackendProcess>
{
public:
  CopyBackendProcess() {}

  process::Future<Nothing> provision(
      const std::vector<AppcImage>& images,
      const std::string& directory);

  process::Future<Nothing> destroy(const std::string& directory);

private:
  process::Future<Nothing> _provision(
    const AppcImage& image,
    const std::string& directory);
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC_BACKEND__
