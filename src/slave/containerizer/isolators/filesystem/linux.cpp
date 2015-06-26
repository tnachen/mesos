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

#include <iostream>
#include <list>
#include <string>

#include <stout/json.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include "linux/fs.hpp"

#include "slave/paths.hpp"

#include "slave/containerizer/isolators/filesystem/linux.hpp"

using namespace process;

using std::list;
using std::ostringstream;
using std::string;

namespace mesos {
namespace internal {
namespace slave {

using mesos::slave::ExecutorRunState;
using mesos::slave::Isolator;
using mesos::slave::IsolatorProcess;
using mesos::slave::Limitation;

LinuxFilesystemIsolatorProcess::LinuxFilesystemIsolatorProcess(
    const Flags& _flags)
  : flags(_flags) {}


LinuxFilesystemIsolatorProcess::~LinuxFilesystemIsolatorProcess() {}


Try<Isolator*> LinuxFilesystemIsolatorProcess::create(const Flags& flags)
{
  Result<string> user = os::user();
  if (!user.isSome()) {
    return Error("Failed to determine user: " +
                 (user.isError() ? user.error() : "username not found"));
  }

  if (user.get() != "root") {
    return Error("LinuxFilesystemIsolator requires root privileges");
  }

  process::Owned<IsolatorProcess> process(
      new LinuxFilesystemIsolatorProcess(flags));

  return new Isolator(process);
}


Future<Nothing> LinuxFilesystemIsolatorProcess::recover(
    const list<ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  Try<fs::MountInfoTable> table = fs::MountInfoTable::read();
  if (table.isError()) {
    return Failure("Failed to get mount table: " + table.error());
  }

  foreach (const ExecutorRunState& state, states) {
    // Check if the work directory is mounted somewhere, indicating
    // the container has a rootfs.
    auto mount = std::find_if(
        table.get().entries.begin(),
        table.get().entries.end(),
        [=](const fs::MountInfoTable::Entry& entry) {
          return entry.root == state.directory;
        });

    Option<string> rootfs;
    if (mount != table.get().entries.end()) {
      rootfs = strings::remove(
          mount->target, flags.sandbox_directory, strings::Mode::SUFFIX);

      LOG(INFO) << "Recovered container '" << state.id << "'"
                << " with rootfs '" << rootfs.get() << "'";
    }

    infos.put(state.id, Owned<Info>(new Info(state.directory, rootfs)));
  }

  return Nothing();
}


Try<string> prepareHostPath(
    const string& directory,
    const Volume& volume)
{
  // Host path must be provided.
  if (!volume.has_host_path()) {
    return Error("Missing host_path");
  }

  string hostPath;
  if (strings::startsWith(volume.host_path(), "/")) {
    hostPath = volume.host_path();

    // An absolute path must already exist.
    if (!os::exists(hostPath)) {
      return Error("Absolute host path does not exist");
    }
  } else {
    // Path is interpreted as relative to the work directory.
    hostPath = path::join(directory, volume.host_path());

    bool create = !os::exists(hostPath);

    if (create) {
      Try<Nothing> mkdir = os::mkdir(hostPath);
      if (mkdir.isError()) {
        return Error("Could not create relative host_path: " + mkdir.error());
      }

      // TODO(idownes): Consider setting ownership and mode.
    }

    // Check it resolves beneath the work directory.
    Result<string> realpath = os::realpath(hostPath);
    if (realpath.isError()) {
      // Remove hostPath if we created it.
      if (create) { os::rm(hostPath); }

      return Error("Could not check host_path: " + realpath.error());
    }

    if (realpath.isNone()) {
      return Error("Failed to create host_path");
    }

    if (!strings::startsWith(realpath.get(), directory)) {
      // Remove hostPath if we created it.
      if (create) { os::rm(hostPath); }

      return Error("Relative host_path resolves outside work directory");
    }
  }

  return hostPath;
}


Try<string> prepareContainerPath(
    const string& directory,
    const Option<string>& rootfs,
    const Volume& volume)
{
  string containerPath =
    (rootfs.isSome() ? path::join(rootfs.get(), volume.container_path())
                     : volume.container_path());

  // Require container_path to exist if using the host's filesystem.
  if (!rootfs.isSome() && !os::exists(volume.container_path())) {
    return Error("container_path must exist on host filesystem");
  }

  // Create container_path under container rootfs, if required.
  if (rootfs.isSome() && !os::exists(containerPath)) {
    Try<Nothing> mkdir = os::mkdir(containerPath);
    if (mkdir.isError()) {
      return Error("Error creating container_path: " + mkdir.error());
    }

    // Check it resolves beneath rootfs.
    Result<string> realpath = os::realpath(containerPath);
    if (realpath.isError()) {
      os::rm(containerPath);

      return Error("Could not check container_path: " + realpath.error());
    }

    if (realpath.isNone()) {
      return Error("Failed to create host_path");
    }

    if (!strings::startsWith(realpath.get(), rootfs.get())) {
      os::rm(containerPath);

      return Error("Relative container_path resolves outside rootfs");
    }

    containerPath = realpath.get();
  }

  return containerPath;
}


// TODO(idownes): Check we don't mount over the rootfs.
Future<Option<CommandInfo>> LinuxFilesystemIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& rootfs,
    const Option<string>& user)
{
  if (infos.contains(containerId)) {
    return Failure("Container has already been prepared");
  }

  if (executorInfo.has_container() &&
      executorInfo.container().type() != ContainerInfo::MESOS) {
    return Failure("Only MESOS containers supported");
  }

  infos.put(containerId, Owned<Info>(new Info(directory, rootfs)));

  // Mount the work directory into the container's rootfs.
  if (rootfs.isSome()) {
    string containerWorkDirectory =
      path::join(rootfs.get(), flags.sandbox_directory);

    if (!os::exists(containerWorkDirectory)) {
      Try<Nothing> mkdir = os::mkdir(containerWorkDirectory);
      if (mkdir.isError()) {
        return Failure("Failed to create work directory in container rootfs: "
                       + mkdir.error());
      }
    }

    Try<Nothing> mount = fs::mount(
        directory,
        path::join(rootfs.get(), flags.sandbox_directory),
        None(),
        MS_BIND | MS_SHARED,
        NULL);
    if (mount.isError()) {
      return Failure(
          "Failed to mount work directory to container: " + mount.error());
    }
  }

  if (!executorInfo.has_container()) {
    // No container volumes to set up; continue with update().
    return update(containerId, executorInfo.resources())
      .then([]() -> Future<Option<CommandInfo>> { return None(); });
  }

  ostringstream script;
  script << "#!/bin/sh" << std::endl;
  script << "set -x" << std::endl;

  foreach (const Volume& volume, executorInfo.container().volumes()) {
    Try<string> hostPath = prepareHostPath(directory, volume);
    if (hostPath.isError()) {
      return Failure("Failed to prepare host_path for volume '" +
                     stringify(JSON::Protobuf(volume)) + "': " +
                     hostPath.error());
    }

    Try<string> containerPath = prepareContainerPath(directory, rootfs, volume);
    if (containerPath.isError()) {
      return Failure("Failed to prepare container_path for volume '" +
                     stringify(JSON::Protobuf(volume)) + "': " +
                     containerPath.error());
    }

    script << "mount -n --bind "
           << hostPath.get() << " "
           << containerPath.get()
           << std::endl;
  }

  CommandInfo command;
  command.set_value(script.str());

  return update(containerId, executorInfo.resources())
    .then([=]() -> Future<Option<CommandInfo>> { return command; });
}


Future<Nothing> LinuxFilesystemIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  // No-op, isolation happens when unsharing the mount namespace.

  return Nothing();
}


Future<Limitation> LinuxFilesystemIsolatorProcess::watch(
    const ContainerID& containerId)
{
  // No-op, for now.

  return Future<Limitation>();
}


Future<Nothing> LinuxFilesystemIsolatorProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  // Mount persistent volumes. We do this in the host namespace and
  // rely on mount propagation for them to be visible inside the
  // container.
  if (!infos.contains(containerId)) {
    return Failure("Unknown container");
  }

  const Owned<Info>& info = infos[containerId];

  // After recovery 'current' will be empty so we won't remove any
  // persistent volumes. This assumes the first call to update() after
  // recovery has the same persistent volumes as before the slave went
  // down.
  Resources current = info->resources;

  // We first remove unneeded persistent volumes.
  foreach (const Resource& resource, current.persistentVolumes()) {
    // This is enforced by the master.
    CHECK(resource.disk().has_volume());

    if (resources.contains(resource)) {
      continue;
    }

    // Ignore absolute and nested paths.
    const string& containerPath = resource.disk().volume().container_path();
    if (strings::contains(containerPath, "/")) {
      LOG(WARNING) << "Skipping updating mount for persistent volume "
                   << resource << " of container " << containerId
                   << " because the container path '" << containerPath
                   << "' contains slash";
      continue;
    }

    string target = path::join(info->directory, containerPath);

    LOG(INFO) << "Removing mount '" << target << "' for persistent volume "
              << resource << " of container " << containerId;

    Try<Nothing> unmount = fs::unmount(target);
    if (unmount.isError()) {
      return Failure(
          "Failed to unmount unneeded persistent volume at '" + target +
          "': " + unmount.error());
    }
  }

  // We then mount new persistent volumes.
  foreach (const Resource& resource, resources.persistentVolumes()) {
    // This is enforced by the master.
    CHECK(resource.disk().has_volume());

    // Ignore absolute and nested paths.
    const string& containerPath = resource.disk().volume().container_path();
    if (strings::contains(containerPath, "/")) {
      LOG(WARNING) << "Skipping updating mount for persistent volume "
                   << resource << " of container " << containerId
                   << " because the container path '" << containerPath
                   << "' contains slash";
      continue;
    }

    if (current.contains(resource)) {
      continue;
    }

    string target = path::join(info->directory, containerPath);

    string source = paths::getPersistentVolumePath(
        flags.work_dir,
        resource.role(),
        resource.disk().persistence().id());

    LOG(INFO) << "Mounting '" << source << "' to '" << target
              << "' for persistent volume " << resource
              << " of container " << containerId;

    Try<Nothing> mount = fs::mount(source, target, None(), MS_BIND, NULL);
    if (mount.isError()) {
      return Failure(
          "Failed to mount persistent volume from '" +
          source + "' to '" + target + "': " + mount.error());
    }
  }

  // Store the new resources;
  info->resources = resources;

  return Nothing();
}


Future<ResourceStatistics> LinuxFilesystemIsolatorProcess::usage(
    const ContainerID& containerId)
{
  // No-op, no usage gathered.

  return ResourceStatistics();
}


Future<Nothing> LinuxFilesystemIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring cleanup request for unknown container: "
            << containerId;
    return Nothing();
  }

  // Cleanup of container volumes mounts is done automatically by the
  // kernel when the mount namespace is destroyed after the last
  // process terminates.

  const Owned<Info>& info = infos[containerId];

  // Clean up any mounts to the rootfs from the host namespace,
  // notably this includes persistent volume mounts.
  if (info->rootfs.isSome()) {
    Try<fs::MountInfoTable> table = fs::MountInfoTable::read();
    if (table.isError()) {
      return Failure("Failed to get mount table: " + table.error());
    }

    // Process mounts in reverse order, lazily unmounting anything
    // mounted to the rootfs, including mouting to itself.
    for (auto entry = table.get().entries.crbegin();
         entry != table.get().entries.crend();
         ++entry) {
      if (strings::startsWith(entry->target, info->rootfs.get())) {
        Try<Nothing> unmount = fs::unmount(entry->target, MNT_DETACH);
        if (unmount.isError()) {
          return Failure(
              "Failed to unmount '" + entry->target + "': " + unmount.error());
        }
      }
    }
  }

  infos.erase(containerId);

  return Nothing();
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
