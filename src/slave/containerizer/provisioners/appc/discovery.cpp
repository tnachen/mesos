
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

#include "slave/containerizer/provisioners/appc.hpp"

#include "slave/containerizer/provisioners/appc/discovery.hpp"

using namespace process;

using std::string;

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

Try<Owned<Discovery>> Discovery::create(const Flags& flags)
{
  hashmap<string, Try<Owned<Discovery>>(*)(const Flags&)> creators{
    {"local",  &LocalDiscovery::create},
    {"simple", &SimpleDiscovery::create}
  };

  if (!creators.contains(flags.provisioner_discovery)) {
    return Error("Unknown or unsupported strategy '" +
                 flags.provisioner_discovery + "'");
  }

  return creators[flags.provisioner_discovery](flags);
}


Try<Owned<Discovery>> LocalDiscovery::create(const Flags& flags)
{
  return Owned<Discovery>(new LocalDiscovery(flags));
}


Future<string> LocalDiscovery::discover(
    const std::string& name,
    const hashmap<std::string, std::string>& labels,
    const Option<std::string>& id)
{
  Try<string> canonical = AppcImage::canonicalize(name, labels);

  if (canonical.isError()) {
    return Failure("Failed to canonicalize image name");
  }

  return path::join(
      "file:///",
      flags.provisioner_local_dir,
      canonical.get() + ".aci");
}


Try<Owned<Discovery>> SimpleDiscovery::create(const Flags& flags)
{
  return Owned<Discovery>(new SimpleDiscovery());
}


Future<string> SimpleDiscovery::discover(
    const std::string& name,
    const hashmap<std::string, std::string>& labels,
    const Option<std::string>& id)
{
  Try<string> canonical = AppcImage::canonicalize(name, labels);

  if (canonical.isError()) {
    return Failure("Failed to canonicalize image name");
  }

  return path::join("http://", canonical.get() + ".aci");
}

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
