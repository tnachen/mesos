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

#ifndef __MESOS_DOCKER_DISCOVERY__
#define __MESOS_DOCKER_DISCOVERY__

#include <string>

#include <process/future.hpp>
#include <process/owned.hpp>

#include <stout/hashmap.hpp>

#include "slave/flags.hpp"

namespace mesos {
namespace internal {
namespace slave {

class Discovery
{
public:
  virtual ~Discovery() {}

  static Try<process::Owned<Discovery>> create(const Flags& flags);

  virtual process::Future<std::string> discover(
      const Option<std::string>& repository,
      const Option<std::string>& tag,
      const Option<std::string>& id) = 0;
};


class LocalDiscovery : public Discovery
{
public:
  static Try<process::Owned<Discovery>> create(const Flags& flags);

  virtual process::Future<std::string> discover(const string& name);

private:
  LocalDiscovery(const Flags& _flags)
    : flags(_flags) {}

  const Flags flags;
};


} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_DOCKER_DISCOVERY__
