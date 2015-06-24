
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

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

// Forward declarations.
class Backend;
class Discovery;
class Store;


struct AppcImage
{
  struct Dependency {
    Dependency(
        const std::string& _name,
        const hashmap<std::string, std::string>& _labels,
        const Option<std::string>& _hash)
      : name(_name), labels(_labels), hash(_hash) {}

    Dependency(const ContainerInfo::Image::AppC& image)
      : name(image.name()),
        labels([=]() -> hashmap<std::string, std::string> {
            hashmap<std::string, std::string> labels_;
            foreach (const Label& label, image.labels().labels()) {
              labels_[label.key()] = label.value();
            }
            return labels_;
          }()),
        hash(image.id()) {}

    std::string name;
    hashmap<std::string, std::string> labels;
    Option<std::string> hash;
  };

  AppcImage(
      const std::string& _name,
      const hashmap<std::string, std::string>& _labels,
      const std::string& _hash,
      const std::vector<Dependency>& _dependencies,
      const std::string& _path,
      const JSON::Object& _manifest)
    : name(_name),
      labels(_labels),
      hash(_hash),
      dependencies(_dependencies),
      path(_path),
      manifest(_manifest) {}

  AppcImage() {}

  static Try<JSON::Object> parse(const std::string& value);

  static Try<AppcImage> parse(
      const std::string& manifest,
      const std::string& hash,
      const std::string& path);

  // Canonical name: {name}-{version}-{os}-{arch}
  static Try<std::string> canonicalize(
      const std::string& name,
      const hashmap<std::string, std::string>& labels)
  {
    std::ostringstream c;
    c << name;

    Try<os::UTSInfo> info = os::uname();
    if (info.isError()) {
      return Error("Failed to determine UTS info: " + info.error());
    }

    // Version, e.g., 0.0.1 or 'latest' if not provided.
    c << "-" << (labels.contains("version") ? labels.get("version").get()
                                            : "latest");

    // OS, e.g., linux (downcased).
    c << "-" << (labels.contains("os") ? strings::lower(labels.get("os").get())
                                      : strings::lower(info.get().sysname));

    // Architecture, e.g., amd64. Convert x86_64 to amd64 (see AppC spec).
    // TODO(idownes): Fix this, conversion to x86_64 is only for
    // linux.
    std::string architecture =
      labels.contains("arch") ? labels.get("arch").get()
                              : info.get().machine;

    c << "-" << (architecture == "x86_64" ? "amd64" : architecture);

    return c.str();
  }

  static bool matches(
    const std::string& name,
    const Option<std::string>& id,
    const hashmap<std::string, std::string>& labels,
    const AppcImage& candidate);

  std::string name;
  hashmap<std::string, std::string> labels;
  std::string hash;
  std::vector<Dependency> dependencies;
  std::string path;
  JSON::Object manifest;
};


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
      const ContainerInfo::Image& image,
      const std::string& directory);

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

  process::Future<std::string> _provision(
      const ContainerID& containerId,
      const std::vector<AppcImage>& images);

  process::Future<std::vector<AppcImage>> fetch(
      const std::string& name,
      const Option<std::string>& hash,
      const hashmap<std::string, std::string>& labels);

  process::Future<std::vector<AppcImage>> _fetch(
      const std::string& name,
      const Option<std::string>& hash,
      const hashmap<std::string, std::string>& labels,
      const std::vector<AppcImage>& candidates);

  process::Future<std::vector<AppcImage>> fetchDependencies(
      const AppcImage& image);

  process::Future<std::vector<AppcImage>> _fetchDependencies(
      const AppcImage& image,
      const std::list<std::vector<AppcImage>>& dependencies);

  const Flags flags;

  process::Owned<Discovery> discovery;
  process::Owned<Store> store;
  process::Owned<Backend> backend;
};

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC__
