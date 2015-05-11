
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

#ifndef __MESOS_APPC__
#define __MESOS_APPC__

#include <list>
#include <sstream>
#include <string>
#include <vector>

#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/try.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <mesos/resources.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/provisioner.hpp"

#include "slave/containerizer/provisioners/appc/store.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

// Forward declarations.
class Backend;
class Discovery;
class Store;

Try<Nothing> validate(const AppcImageManifest& manifest);

Try<AppcImageManifest> parse(const std::string& value);

// Canonical name: {name}-{version}-{os}-{arch}
Try<std::string> canonicalize(
    const std::string& name,
    const hashmap<std::string, std::string>& labels);

bool matches(
  const std::string& name,
  const Option<std::string>& id,
  const hashmap<std::string, std::string>& labels,
  const StoredImage& candidate);


// Forward declaration.
class AppcProvisionerProcess;

class AppcProvisioner : public Provisioner
{
public:
  static Try<process::Owned<Provisioner>> create(
      const Flags& flags,
      Fetcher* fetcher);

  ~AppcProvisioner();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<std::string> provision(
      const ContainerID& containerId,
      const ContainerInfo::Image& image);

  virtual process::Future<Nothing> destroy(const ContainerID& containerId);

private:
  AppcProvisioner(process::Owned<AppcProvisionerProcess> process);
  AppcProvisioner(const AppcProvisioner&); // Not copyable.
  AppcProvisioner& operator=(const AppcProvisioner&); // Not assignable.

  process::Owned<AppcProvisionerProcess> process;
};


class AppcProvisionerProcess : public process::Process<AppcProvisionerProcess>
{
public:
  static Try<process::Owned<AppcProvisionerProcess>> create(
      const Flags& flags,
      Fetcher* fetcher);

  process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  process::Future<std::string> provision(
      const ContainerID& containerId,
      const ContainerInfo::Image::AppC& image);

  process::Future<Nothing> destroy(const ContainerID& containerId);

private:
  AppcProvisionerProcess(
      const Flags& flags,
      const process::Owned<Discovery>& discovery,
      const process::Owned<Store>& store,
      const process::Owned<Backend>& backend);

  process::Future<std::vector<StoredImage>> fetchAll(
      const std::string& name,
      const Option<std::string>& hash,
      const hashmap<std::string, std::string>& labels);

  process::Future<StoredImage> fetch(
      const std::string& name,
      const Option<std::string>& hash,
      const hashmap<std::string, std::string>& labels);

  process::Future<std::list<std::vector<StoredImage>>> fetchDependencies(
      const StoredImage& image);

  const Flags flags;

  const process::Owned<Discovery> discovery;
  const process::Owned<Store> store;
  const process::Owned<Backend> backend;

  struct Info
  {
    explicit Info(const std::string& _base) : base(_base) {}

    const std::string base;
  };

  hashmap<ContainerID, process::Owned<Info>> infos;
};

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC__
