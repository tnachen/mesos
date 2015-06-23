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

#include <stout/json.hpp>
#include <stout/nothing.hpp>
#include <stout/os.hpp>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/owned.hpp>
#include <process/sequence.hpp>

#include "linux/fs.hpp"

#include "slave/containerizer/provisioners/docker.hpp"

#include "slave/containerizer/provisioners/docker/backend.hpp"
#include "slave/containerizer/provisioners/docker/discovery.hpp"
#include "slave/containerizer/provisioners/docker/store.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

using mesos::slave::ExecutorRunState;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {


Try<Owned<Provisioner>> DockerProvisioner::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  Try<Owned<DockerProvisionerProcess>> create =
    DockerProvisionerProcess::create(flags, fetcher);

  if (create.isError()) {
    return Error(create.error());
  }

  return Owned<Provisioner>(new DockerProvisioner(create.get()));
}


DockerProvisioner::DockerProvisioner(Owned<DockerProvisionerProcess> _process)
  : process(_process)
{
  process::spawn(CHECK_NOTNULL(process.get()));
}


DockerProvisioner::~DockerProvisioner()
{
  process::terminate(process.get());
  process::wait(process.get());
}


Future<Nothing> DockerProvisioner::recover(
    const list<ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  return dispatch(
      process.get(),
      &DockerProvisionerProcess::recover,
      states,
      orphans);
}


Future<string> DockerProvisioner::provision(
    const ContainerID& containerId,
    const ContainerInfo::Image& image)
{
  if (image.type() != ContainerInfo::Image::DOCKER) {
    return Failure("Unsupported container image type");
  }

  if (!image.has_docker()) {
    return Failure("Missing Docker image info");
  }

  return dispatch(
      process.get(),
      &DockerProvisionerProcess::provision,
      containerId,
      image.docker());
}


Future<Nothing> DockerProvisioner::destroy(const ContainerID& containerId)
{
  return dispatch(
      process.get(),
      &DockerProvisionerProcess::destroy,
      containerId);
}


Try<Owned<DockerProvisionerProcess>> DockerProvisionerProcess::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  Try<Nothing> mkdir = os::mkdir(flags.provisioner_rootfs_dir);
  if (mkdir.isError()) {
    return Error("Failed to create provisioner rootfs directory '" +
                 flags.provisioner_rootfs_dir + "': " + mkdir.error());
  }

  Try<Owned<Discovery>> discovery = Discovery::create(flags);
  if (discovery.isError()) {
    return Error("Failed to create discovery: " + discovery.error());
  }

  Try<Owned<Store>> store = Store::create(flags, fetcher);
  if (store.isError()) {
    return Error("Failed to create image store: " + store.error());
  }

  Try<Owned<Backend>> backend = Backend::create(flags);
  if (backend.isError()) {
    return Error("Failed to create image backend: " + backend.error());
  }

  return Owned<DockerProvisionerProcess>(
      new DockerProvisionerProcess(
          flags,
          discovery.get(),
          store.get(),
          backend.get()));
}


DockerProvisionerProcess::DockerProvisionerProcess(
    const Flags& _flags,
    const Owned<Discovery>& _discovery,
    const Owned<Store>& _store,
    const Owned<Backend>& _backend)
  : flags(_flags),
    discovery(_discovery),
    store(_store),
    backend(_backend) {}


Future<Nothing> DockerProvisionerProcess::recover(
    const list<ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  // TODO(idownes): Implement this, if a need arises.

  return Nothing();
}


Future<string> DockerProvisionerProcess::provision(
    const ContainerID& containerId,
    const ContainerInfo::Image::Docker& image)
{
  return fetch(image.name())
    .then(defer(self(),
                &Self::_provision,
                containerId,
                lambda::_1));
}


Future<string> DockerProvisionerProcess::_provision(
    const ContainerID& containerId,
    const DockerImage& image)
{
  // Create root directory.
  string base = path::join(flags.provisioner_rootfs_dir,
                           stringify(containerId));

  string rootfs = path::join(base, "rootfs");

  Try<Nothing> mkdir = os::mkdir(base);
  if (mkdir.isError()) {
    return Failure("Failed to create directory for container filesystem: " +
                    mkdir.error());
  }

  LOG(INFO) << "Provisioning rootfs for container '" << containerId << "'"
            << " to '" << base << "'";

  return backend->provision(image, base)
    .then([=] () -> Future<string> {
      // Bind mount the rootfs to itself so we can pivot_root. We do
      // it now so any subsequent mounts by the containerizer or
      // isolators are correctly handled by pivot_root.
      Try<Nothing> mount =
        fs::mount(rootfs, rootfs, None(), MS_BIND | MS_SHARED, NULL);

      if (mount.isError()) {
        return Failure("Failure to bind mount rootfs: " + mount.error());
      }

      return rootfs;
    });
}


// Fetch an image and all dependencies.
Future<DockerImage> DockerProvisionerProcess::fetch(const string& name)
{
  return store->get(name)
    .then([=](const Option<DockerImage>& image) -> Future<DockerImage> {
      if (image.isSome()) {
        return image.get();
      }

      return discovery->discover(name)
        .then([=](const string& uri) -> Future<DockerImage> {
          return store->put(uri, name);
        });
    });
}


Future<Nothing> DockerProvisionerProcess::destroy(
    const ContainerID& containerId)
{
  string base = path::join(flags.provisioner_rootfs_dir,
                           stringify(containerId));

  string rootfs = path::join(base, "rootfs");

  LOG(INFO) << "Destroying container rootfs for container '"
            << containerId << "'"
            << " at '" << rootfs << "'";

  Try<fs::MountInfoTable> mountTable = fs::MountInfoTable::read();

  if (mountTable.isError()) {
    return Failure("Failed to read mount table: " + mountTable.error());
  }

  foreach (const fs::MountInfoTable::Entry& entry, mountTable.get().entries) {
    if (strings::startsWith(entry.target, base)) {
      fs::unmount(entry.target, MNT_DETACH);
    }
  }

  return backend->destroy(base);
}

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
