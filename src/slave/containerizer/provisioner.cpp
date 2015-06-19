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

#include <stout/hashmap.hpp>
#include <stout/strings.hpp>

#include "slave/containerizer/provisioner.hpp"

#include "slave/containerizer/provisioners/appc.hpp"
#include "slave/containerizer/provisioners/docker.hpp"

using namespace process;

using std::string;

namespace mesos {
namespace internal {
namespace slave {

Try<hashmap<ContainerInfo::Image::Type, Owned<Provisioner>>>
  Provisioner::create(const Flags& flags, Fetcher* fetcher)
{
  if (!flags.provisioners.isSome()) {
    return hashmap<ContainerInfo::Image::Type, Owned<Provisioner>>();
  }

  hashmap<ContainerInfo::Image::Type, Owned<Provisioner>> provisioners;

  foreach (const string& type,
           strings::tokenize(flags.provisioners.get(), ",")) {
    if (type == "appc") {
      Try<Owned<Provisioner>> create = AppcProvisioner::create(flags, fetcher);
      if (create.isError()) {
        return Error("Failed to create AppC provisioner: " + create.error());
      }

      provisioners[ContainerInfo::Image::APPC] = create.get();
    } else if (type == "docker") {
        Try<Owned<Provisioner>> create = DockerProvisioner::create(flags, fetcher);
        if (create.isError()) {
          return Error("Failed to create docker provisioner: " + create.error());
        }

        provisioners[ContainerInfo::Image::DOCKER] = create.get();
    } else {
      return Error("Unknown or unsupported provisioner '" + type + "'");
    }
  }

  return provisioners;
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
