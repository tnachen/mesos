
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

#include <list>

#include <stout/path.hpp>

#include <process/collect.hpp>
#include <process/dispatch.hpp>
#include <process/subprocess.hpp>

#include "slave/containerizer/provisioners/docker/backend.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {

Try<Owned<Backend>> Backend::create(const Flags& flags)
{
  hashmap<string, Try<Owned<Backend>>(*)(const Flags&)> creators{
    {"copy", &CopyBackend::create},
  };

  if (!creators.contains(flags.provisioner_backend)) {
    return Error("Unknown or unsupported provisioner backend");
  }

  return creators[flags.provisioner_backend](flags);
}


CopyBackend::CopyBackend(Owned<CopyBackendProcess> _process)
  : process(_process)
{
  process::spawn(CHECK_NOTNULL(process.get()));
}


CopyBackend::~CopyBackend()
{
  process::terminate(process.get());
  process::wait(process.get());
}


Try<Owned<Backend>> CopyBackend::create(const Flags& flags)
{
  Owned<CopyBackendProcess> process = Owned<CopyBackendProcess>(
      new CopyBackendProcess());

  return Owned<Backend>(new CopyBackend(process));
}


Future<Nothing> CopyBackend::provision(
    const DockerImage& image,
    const string& directory)
{
  return dispatch(
      process.get(),
      &CopyBackendProcess::provision,
      image,
      directory);
}


Future<Nothing> CopyBackend::destroy(const string& directory)
{
  return dispatch(
      process.get(),
      &CopyBackendProcess::destroy,
      directory);
}


Future<Nothing> CopyBackendProcess::provision(
    const DockerImage& image,
    const string& directory)
{
  list<Shared<DockerLayer>> layers;
  list<Future<Nothing>> futures{Nothing()};

  Option<Shared<DockerLayer>> layer = image.layer;
  while (layer.isSome()) {
    layers.push_front(layer.get());
    layer = layer.get()->parent;
  }

  foreach (const Shared<DockerLayer>& layer, layers)
  {
    futures.push_back(
        futures.back().then(
          defer(self(), &Self::_provision, image.name, *layer, directory)));
  }

  return collect(futures)
    .then([]() -> Future<Nothing> { return Nothing(); })
    .onFailed(defer(self(), &Self::destroy, directory));
}


Future<Nothing> CopyBackendProcess::_provision(
  const string name,
  const DockerLayer& layer,
  const string& directory)
{
  LOG(INFO) << "Provisioning image '" << name << "' layer '" << layer.hash
            << "' to " << directory;

  vector<string> argv{
    "cp",
    "--archive",
    layer.path,
    directory
  };

  Try<Subprocess> s = subprocess(
      "cp",
      argv,
      Subprocess::PATH("/dev/null"),
      Subprocess::FD(STDOUT_FILENO),
      Subprocess::FD(STDERR_FILENO));

  if (s.isError()) {
    return Failure("Failed to create 'cp' subprocess: " + s.error());
  }

  return s.get().status()
    .then([](const Option<int>& status) -> Future<Nothing> {
        if (status.isNone()) {
          return Failure("Failed to reap subprocess to copy image");
        } else if (status.get() != 0) {
          return Failure("Non-zero exit from subprocess to copy image: " +
                         stringify(status.get()));
        }

        return Nothing();
      });
}


Future<Nothing> CopyBackendProcess::destroy(const string& directory)
{
  vector<string> argv{"rm", "-rf", directory};

  Try<Subprocess> s = subprocess(
      "rm",
      argv,
      Subprocess::PATH("/dev/null"),
      Subprocess::FD(STDOUT_FILENO),
      Subprocess::FD(STDERR_FILENO));

  if (s.isError()) {
    return Failure("Failed to create 'rm' subprocess: " + s.error());
  }

  return s.get().status()
    .then([](const Option<int>& status) -> Future<Nothing> {
        if (status.isNone()) {
          return Failure("Failed to reap subprocess to destroy rootfs");
        } else if (status.get() != 0) {
          return Failure("Non-zero exit from subprocess to destroy rootfs: " +
                         stringify(status.get()));
        }

        return Nothing();
      });
}

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
