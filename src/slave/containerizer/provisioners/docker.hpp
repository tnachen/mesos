
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

#ifndef __MESOS_DOCKER__
#define __MESOS_DOCKER__

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

namespace mesos {
namespace internal {
namespace slave {

// Forward declarations.
class Backend;
class Discovery;
class Store;

struct DockerLayer {
  DockerLayer(
      const std::string& hash,
      const JSON::Object& manifest,
      const std::string& path,
      const std::string& version,
      const Option<DockerLayer>& parent)
    : hash(hash),
      manifest(manifest),
      path(path),
      version(version),
      parent(parent) {}

    Try<DockerLayer> parse(JSON::Object manifest, path, version)

    const std::string hash,
    const std::string manifest,
    const std::string path,
    const std::string version,
    const Option<DockerLayer> parent
};


struct DockerImage 
{
    DockerImage(
      const std::string& name,
      const DockerLayer& layer)
    :name(_name), layer(_layer) {}

  const std::string name;
  const DockerLayer layer;

  

  static Try<std::pair<std::string, std::string>> parseTag(const std::string& name)
  {
    std::size_t found = name.find_last_of(':');
    if (found == std::string::npos) {
      return make_pair(name, "latest");
    }
    return make_pair(name.substr(0, found), name.substr(found + 1)); 
  }

  static Try<DockerImage> parse(const std::string& uri);

};


// Forward declaration.
class DockerProvisionerProcess;

class DockerProvisioner : public Provisioner
{
public:
  static Try<process::Owned<Provisioner>> create(
      const Flags& flags,
      Fetcher* fetcher);

  ~DockerProvisioner();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<std::string> provision(
      const ContainerID& containerId,
      const ContainerInfo::Image& image);

  virtual process::Future<Nothing> destroy(const ContainerID& containerId);

private:
  DockerProvisioner(process::Owned<DockerProvisionerProcess> process);
  DockerProvisioner(const DockerProvisioner&); // Not copyable.
  DockerProvisioner& operator=(const DockerProvisioner&); // Not assignable.

  process::Owned<DockerProvisionerProcess> process;
};


class DockerProvisionerProcess : public process::Process<DockerProvisionerProcess>
{
public:
  static Try<process::Owned<DockerProvisionerProcess>> create(
      const Flags& flags,
      Fetcher* fetcher);

  process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  //TODO(lily): check protobuf on this
  process::Future<std::string> provision(
      const ContainerID& containerId,
      const ContainerInfo::Image::Docker& image);

  process::Future<Nothing> destroy(const ContainerID& containerId);

private:
  DockerProvisionerProcess(
      const Flags& flags,
      const process::Owned<Discovery>& discovery,
      const process::Owned<Store>& store,
      const process::Owned<Backend>& backend);

  process::Future<std::string> _provision(
      const ContainerID& containerId,
      const std::vector<DockerImage>& images);

  //made name optional, make id not optional see fetch 
  process::Future<std::vector<DockerImage>> fetch(
      const std::string& name,
      const Option<std::string>& hash,
      const hashmap<std::string, std::string>& labels);

  process::Future<std::vector<DockerImage>> _fetch(
      const std::string& name,
      const Option<std::string>& hash,
      const std::vector<DockerImage>& candidates);

  process::Future<std::vector<DockerImage>> fetchDependencies(
      const DockerImage& image);

  process::Future<std::vector<DockerImage>> _fetchDependencies(
      const DockerImage& image,
      const std::list<std::vector<DockerImage>>& dependencies);

  const Flags flags;

  process::Owned<Discovery> discovery;
  process::Owned<Store> store;
  process::Owned<Backend> backend;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_DOCKER__
