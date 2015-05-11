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

#ifndef __MESOS_APPC_SHA512_HPP__
#define __MESOS_APPC_SHA512_HPP__

#include <stout/os.hpp>
#include <stout/try.hpp>

#include <process/collect.hpp>
#include <process/future.hpp>
#include <process/io.hpp>
#include <process/subprocess.hpp>

#include <string>
#include <vector>

#include "common/status_utils.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

class SHA512
{
public:
  static Try<std::string> command()
  {
    if (os::exists("/usr/bin/sha512sum")) {
      return "/usr/bin/sha512sum";
    }

    if (os::exists("/usr/bin/shasum")) {
      return "/usr/bin/shasum -a 512";
    }

    if (os::exists("/usr/bin/openssl")) {
      return "/usr/bin/openssl dgst -sha512";
    }

    return Error("No suitable binary for computing hash found");
  }


  static process::Future<std::string> hash(const std::string& path)
  {
    if (!os::exists(path)) {
      return process::Failure("File not found");
    } else if (!os::stat::isfile(path)) {
      return process::Failure("Path is not a file");
    }

    Try<std::string> command_ = command();
    if (command_.isError()) {
      return process::Failure(command_.error());
    }

    Try<process::Subprocess> s = subprocess(
        command_.get() + " " + path,
        process::Subprocess::PATH("/dev/null"),
        process::Subprocess::PIPE(),
        process::Subprocess::PATH("/dev/null"));
    if (s.isError()) {
      return process::Failure("Failed to execute hash command: " + s.error());
    }

    return await(s.get().status(), process::io::read(s.get().out().get()))
      .then(lambda::bind(_hash, lambda::_1));
  }

private:
  static process::Future<std::string> _hash(
      const process::Future<std::tuple<
        process::Future<Option<int>>,
        process::Future<std::string>>>& future)
  {
    CHECK_READY(future);

    process::Future<Option<int>> status = std::get<0>(future.get());
    if (!status.isReady()) {
      return process::Failure("Failed to execute sha512sum: " +
                     (status.isFailed() ? status.failure() : "discarded"));
    } else if (status.get().isNone()) {
      return process::Failure("Failed to reap status of hash command");
    } else if (status.get().get() != 0) {
      return process::Failure("Error from sha512sum: " +
                              WSTRINGIFY(status.get().get()));
    }

    process::Future<std::string> output = std::get<1>(future.get());
    if (!output.isReady()) {
      return process::Failure("Failed to get output from hash command: " +
                     (output.isFailed() ? output.failure() : "discarded"));
    }

    std::vector<std::string> tokens = strings::tokenize(output.get(), " ");
    if (tokens.size() < 2) {
      return process::Failure("Failed to parse output from hash command");
    }

    // Basic sanity check.
    if (tokens[0].size() != 128) {
      return process::Failure("Failed to parse output from hash command");
    }

    return tokens[0];
  }
};

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC_SHA512_HPP__
