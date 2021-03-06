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

#ifndef __POSIX_FILESYSTEM_ISOLATOR_HPP__
#define __POSIX_FILESYSTEM_ISOLATOR_HPP__

#include <mesos/slave/isolator.hpp>

#include "slave/flags.hpp"

namespace mesos {
namespace internal {
namespace slave {

class PosixFilesystemIsolatorProcess : public mesos::slave::IsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(const Flags& flags);

  virtual ~PosixFilesystemIsolatorProcess();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<Option<CommandInfo>> prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& rootfs,
      const Option<std::string>& user);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<mesos::slave::Limitation> watch(
      const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  PosixFilesystemIsolatorProcess(const Flags& flags);

  const Flags flags;

  struct Info
  {
    explicit Info(const std::string& _directory)
      : directory(_directory) {}

    const std::string directory;

    // Track resources so we can unlink unneeded persistent volumes.
    Resources resources;
  };

  hashmap<ContainerID, process::Owned<Info>> infos;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __POSIX_FILESYSTEM_ISOLATOR_HPP__
