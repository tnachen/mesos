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
#include <stout/strings.hpp>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/owned.hpp>
#include <process/sequence.hpp>

#include "linux/fs.hpp"

#include "slave/containerizer/provisioners/appc.hpp"

#include "slave/containerizer/provisioners/appc/backend.hpp"
#include "slave/containerizer/provisioners/appc/discovery.hpp"
#include "slave/containerizer/provisioners/appc/store.hpp"

using namespace process;

using std::list;
using std::string;
using std::vector;

using mesos::slave::ExecutorRunState;

namespace mesos {
namespace internal {
namespace slave {
namespace appc {

Try<Nothing> validate(const AppcImageManifest& manifest)
{
  // TODO(idownes): Validate that required fields are present when
  // this cannot be expressed in the protobuf specification, e.g.,
  // repeated fields with >= 1.
  // TODO(idownes): Validate types.
  return Nothing();
}


Try<AppcImageManifest> parse(const string& value)
{
  Try<JSON::Object> json = JSON::parse<JSON::Object>(value);
  if (json.isError()) {
    return Error("JSON parse failed: " + json.error());
  }

  Try<AppcImageManifest> manifest =
    protobuf::parse<AppcImageManifest>(json.get());

  if (manifest.isError()) {
    return Error("Protobuf parse failed: " + manifest.error());
  }

  Try<Nothing> validation = validate(manifest.get());
  if (validation.isError()) {
    return Error("Schema validation failed: " + validation.error());
  }

  return manifest.get();
}


// Canonical name: {name}-{version}-{os}-{arch}
Try<string> canonicalize(
    const string& name,
    const hashmap<string, string>& labels)
{
  vector<string> fields;

  fields.push_back(name);

  // Version, e.g., 0.0.1 or 'latest' if not provided.
  fields.push_back(labels.contains("version") ? labels.get("version").get()
                                              : "latest");

  Try<os::UTSInfo> info = os::uname();
  if (info.isError()) {
    return Error("Failed to determine UTS info: " + info.error());
  }

  // OS, e.g., linux (downcased).
  fields.push_back(
      labels.contains("os") ? strings::lower(labels.get("os").get())
                            : strings::lower(info.get().sysname));

  // Architecture, e.g., amd64. Convert x86_64 to amd64 (see Appc spec).
  // TODO(idownes): Conversion to x86_64 is only for linux.
  string architecture =
    labels.contains("arch") ? labels.get("arch").get()
                            : info.get().machine;

  fields.push_back(architecture == "x86_64" ? "amd64" : architecture);

  return strings::join("-", fields);
}


bool matches(
    const string& name,
    const Option<string>& id,
    const hashmap<string, string>& labels,
    const StoredImage& candidate)
{
  // The name must match.
  if (candidate.manifest.name() != name) {
    return false;
  }

  // If an id is specified the candidate must match.
  if (id.isSome() && (candidate.id != id.get())) {
    return false;
  }

  // Extract the candidate's labels for easier comparison.
  hashmap<string, string> candidateLabels;

  foreach (const AppcImageManifest::Label& label,
           candidate.manifest.labels()) {
    candidateLabels[label.name()] = label.value();
  }

  // Any label specified must be present and match in the candidate.
  foreachpair (const string& name,
               const string& value,
               labels) {
    if (!candidateLabels.contains(name) ||
        candidateLabels.get(name).get() != value) {
      return false;
    }
  }

  return true;
}


Try<Owned<Provisioner>> AppcProvisioner::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  Try<Owned<AppcProvisionerProcess>> create =
    AppcProvisionerProcess::create(flags, fetcher);

  if (create.isError()) {
    return Error(create.error());
  }

  return Owned<Provisioner>(new AppcProvisioner(create.get()));
}


AppcProvisioner::AppcProvisioner(Owned<AppcProvisionerProcess> _process)
  : process(_process)
{
  process::spawn(CHECK_NOTNULL(process.get()));
}


AppcProvisioner::~AppcProvisioner()
{
  process::terminate(process.get());
  process::wait(process.get());
}


Future<Nothing> AppcProvisioner::recover(
    const list<ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  return dispatch(
      process.get(),
      &AppcProvisionerProcess::recover,
      states,
      orphans);
}


Future<string> AppcProvisioner::provision(
    const ContainerID& containerId,
    const ContainerInfo::Image& image)
{
  if (image.type() != ContainerInfo::Image::APPC) {
    return Failure("Unsupported container image type");
  }

  if (!image.has_appc()) {
    return Failure("Missing AppC image info");
  }

  return dispatch(
      process.get(),
      &AppcProvisionerProcess::provision,
      containerId,
      image.appc());
}


Future<Nothing> AppcProvisioner::destroy(const ContainerID& containerId)
{
  return dispatch(
      process.get(),
      &AppcProvisionerProcess::destroy,
      containerId);
}


Try<Owned<AppcProvisionerProcess>> AppcProvisionerProcess::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  Try<Nothing> mkdir = os::mkdir(flags.appc_rootfs_dir);
  if (mkdir.isError()) {
    return Error("Failed to create provisioner rootfs directory '" +
                 flags.appc_rootfs_dir + "': " + mkdir.error());
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

  return Owned<AppcProvisionerProcess>(
      new AppcProvisionerProcess(
          flags,
          discovery.get(),
          store.get(),
          backend.get()));
}


AppcProvisionerProcess::AppcProvisionerProcess(
    const Flags& _flags,
    const Owned<Discovery>& _discovery,
    const Owned<Store>& _store,
    const Owned<Backend>& _backend)
  : flags(_flags),
    discovery(_discovery),
    store(_store),
    backend(_backend) {}


Future<Nothing> AppcProvisionerProcess::recover(
    const list<ExecutorRunState>& states,
    const hashset<ContainerID>& orphans)
{
  foreach (const ExecutorRunState& state, states) {
    if (state.rootfs.isNone() || !state.executorInfo.has_container()) {
      continue;
    }

    // Only recover images we provisioned, i.e., whose type is APPC.
    const ContainerInfo& containerInfo = state.executorInfo.container();

    if (containerInfo.type() == ContainerInfo::MESOS &&
        containerInfo.mesos().has_image() &&
        containerInfo.mesos().image().type() == ContainerInfo::Image::APPC) {
      const string base =
        strings::remove(state.rootfs.get(), "rootfs", strings::SUFFIX);

      infos.put(state.id, Owned<Info>(new Info(base)));
    }
  }

  LOG(INFO) << "Recovered Appc provisioner";

  return store->recover(states, orphans);
}


Future<string> AppcProvisionerProcess::provision(
    const ContainerID& containerId,
    const ContainerInfo::Image::AppC& image)
{
  // TODO(idownes): Check containerId not already provision{ing,ed}.
  if (infos.contains(containerId)) {
    return Failure("Container filesystem has already been provisioned");
  }

  string base =
    path::join(flags.appc_rootfs_dir, stringify(containerId));

  string rootfs = path::join(base, "rootfs");

  infos.put(containerId, Owned<Info>(new Info(base)));

  hashmap<string, string> labels;
  foreach (const Label& label, image.labels().labels()) {
    labels[label.key()] = label.value();
  }

  return fetchAll(image.name(), image.id(), labels)
    .then(defer(self(),
                [=](const vector<StoredImage>& images) -> Future<string> {
      // Create base directory.
      Try<Nothing> mkdir = os::mkdir(base);
      if (mkdir.isError()) {
        return Failure("Failed to create container root: " + mkdir.error());
      }

      LOG(INFO) << "Provisioning rootfs for container '" << containerId << "'"
                << " to '" << base << "'";

      return backend->provision(images, base)
        .then(defer(self(), [=]() -> Future<string> {
          // Bind mount the rootfs to itself so we can pivot_root. We do
          // it now so any subsequent mounts by the containerizer or
          // isolators are correctly handled by pivot_root.
          Try<Nothing> mount =
            fs::mount(rootfs, rootfs, None(), MS_BIND | MS_SHARED, NULL);

          if (mount.isError()) {
            return Failure("Failure to bind mount rootfs: "
                           + mount.error());
          }

          return rootfs;
        }))
        .onFailed(defer(self(), [=](const string&) {
            os::rm(base);
            infos.erase(containerId);
          }));
    }));
}


Future<vector<StoredImage>> AppcProvisionerProcess::fetchAll(
    const string& name,
    const Option<string>& hash,
    const hashmap<string, string>& labels)
{
  return fetch(name, hash, labels)
    .then(defer(self(), [=](const StoredImage& image) {
      return fetchDependencies(image)
        .then(defer(self(), [=](const list<vector<StoredImage>>& dependencies) {
        vector<StoredImage> images;

        // Concatenate images from each dependency subtree.
        foreach (const vector<StoredImage>& dependency, dependencies) {
          images.insert(images.end(), dependency.begin(), dependency.end());
        }

        // And include this parent image.
        images.push_back(image);

        return images;
      }));
    }));
}


Future<StoredImage> AppcProvisionerProcess::fetch(
    const string& name,
    const Option<string>& hash,
    const hashmap<string, string>& labels)
{
  return store->get(name)
    .then(
        defer(self(),
              [=](const vector<StoredImage>& candidates)-> Future<StoredImage> {
      foreach (const StoredImage& candidate, candidates) {
        if (matches(name, hash, labels, candidate)) {
          LOG(INFO) << "Found match for image '" << name << "' in store";

          return candidate;
        }
      }

      LOG(INFO) << "No match for image '" << name << "', starting discovery";

      return discovery->discover(name, labels, hash)
        .then(defer(self(), [=](const string& uri) {
          LOG(INFO) << "Fetching image from " << uri;
          return store->put(uri);
        }))
        .then(defer(self(),
                    [=](const StoredImage& candidate) -> Future<StoredImage> {
          if (matches(name, hash, labels, candidate)) {
            LOG(INFO) << "Fetched match for image '" << name
                       << "' into store";
            return candidate;
          }

          return Failure("Fetched image for '" + name + "' does not match");
        }));
    }));
}


Future<list<vector<StoredImage>>> AppcProvisionerProcess::fetchDependencies(
    const StoredImage& image)
{
  // Sequentially fetch dependencies.
  // TODO(idownes): Detect recursive dependencies.
  // TODO(idownes): Consider fetching in parallel?
  list<Future<vector<StoredImage>>> futures;
  // Start with a completed future to kickoff the sequence. This is an
  // empty vector so won't contribute in the concatenation of images.
  futures.push_back(vector<StoredImage>());

  foreach (const AppcImageManifest::Dependency& dependency,
           image.manifest.dependencies()) {
    hashmap<string, string> labels;

    foreach (const AppcImageManifest::Label& label, dependency.labels()) {
      labels[label.name()] = label.value();
    }

    LOG(INFO) << "Fetching all images for dependency '"
              << dependency.imagename() << "'";

    futures.push_back(
      futures.back().then(defer(self(),
                                &Self::fetchAll,
                                dependency.imagename(),
                                (dependency.has_imageid() ? dependency.imageid()
                                                          : Option<string>()),
                                labels)));
  }

  return collect(futures);
}


Future<Nothing> AppcProvisionerProcess::destroy(const ContainerID& containerId)
{
  if (!infos.contains(containerId)) {
    VLOG(1) << "Ignoring destroy request for unknown container: "
            << containerId;

    return Nothing();
  }

  const Owned<Info>& info = infos[containerId];

  string rootfs = path::join(info->base, "rootfs");

  LOG(INFO) << "Destroying container rootfs for container '"
            << containerId << "'"
            << " at '" << rootfs << "'";

  Try<fs::MountInfoTable> mountTable = fs::MountInfoTable::read();

  if (mountTable.isError()) {
    return Failure("Failed to read mount table: " + mountTable.error());
  }

  foreach (const fs::MountInfoTable::Entry& entry, mountTable.get().entries) {
    if (strings::startsWith(entry.target, info->base)) {
      fs::unmount(entry.target, MNT_DETACH);
    }
  }

  return backend->destroy(info->base)
    .onAny([=]() {
      os::rmdir(info->base);
      infos.erase(containerId);
    });
}

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
