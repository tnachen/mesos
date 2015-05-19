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

#ifndef __MESOS_APPC_BIND_BACKEND__
#define __MESOS_APPC_BIND_BACKEND__

#include "slave/containerizer/provisioners/appc/backend.hpp"

#include "linux/fs.hpp"

namespace mesos {
namespace internal {
namespace slave {

// This is a specialized backend that may be useful for deployments
// using large (multi-GB) single-layer images *and* where more recent
// kernel features such as overlayfs are not available. For small
// images (10's to 100's of MB) the Copy backend may be sufficient.
// 1) It supports only a single layer. Multi-layer images will fail to
//    provision and the container will fail to launch!
// 2) The filesystem is read-only because all containers using this
//    image share the source. Select writable areas can be achieved by
//    mounting read-write volumes to places like /tmp, /var/tmp,
//    /home, etc. using the ContainerInfo. These can be relative to
//    the executor work directory.
// 3) It relies on the image persisting in the store.
// 4) It's fast because the bind mount requires (nearly) zero IO.
class BindBackend : public Backend
{
public:
  BindBackend() {}

  virtual ~BindBackend() {}

  static Try<process::Owned<Backend>> create(const Flags& flags)
  {
    return process::Owned<Backend>(new BindBackend());
  }

  virtual process::Future<Nothing> provision(
      const std::vector<AppcImage>& images,
      const std::string& directory)
  {
    if (images.size() > 1) {
      return process::Failure(
          "Multiple layers are not supported by the bind backend");
    }

    if (images.size() == 0) {
      return process::Failure("No image provided");
    }

    Try<Nothing> mkdir = os::mkdir(path::join(directory, "rootfs"));
    if (mkdir.isError()) {
      return process::Failure("Failed to create container rootfs");
    }

    Try<Nothing> mount = fs::mount(
        path::join(images.front().path, "rootfs"),
        path::join(directory, "rootfs"),
        None(),
        MS_BIND | MS_SHARED,
        NULL);

    if (mount.isError()) {
      return process::Failure("Failed to bind mount image rootfs: " +
                              mount.error());
    }

    // And remount it read-only.
    mount = fs::mount(
        path::join(images.front().path, "rootfs"),
        path::join(directory, "rootfs"),
        None(),
        MS_BIND | MS_SHARED | MS_RDONLY | MS_REMOUNT,
        NULL);

    if (mount.isError()) {
      return process::Failure("Failed to remount image rootfs read-only: " +
                              mount.error());
    }
    return Nothing();
  }


  virtual process::Future<Nothing> destroy(const std::string& directory)
  {
    // The provisoner will remove all mounts, including this one.
    // TODO(idownes): Change this logic so a backend removes its own
    // state?
    return Nothing();
  }
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_APPC_BIND_BACKEND__
