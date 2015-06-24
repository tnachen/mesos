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


Try<JSON::Object> AppcImage::parse(const std::string& value)
{
  // Convert from string or file to JSON.
  Try<JSON::Object> json = JSON::parse<JSON::Object>(value);
  if (json.isError()) {
    return Error(json.error());
  }

  // XXX Validate the JSON according to the appropriate schema.
  return json;
}


bool AppcImage::matches(
    const string& name,
    const Option<string>& id,
    const hashmap<string, string>& labels,
    const AppcImage& candidate)
{
  // The name must match.
  if (candidate.name != name) {
    return false;
  }

  // If an id is specified the candidate must match.
  if (id.isSome() && candidate.hash != id.get()) {
    return false;
  }

  // Any label specified must be present and match in the candidate.
  foreachpair (const std::string& name,
               const std::string& value,
               labels) {
    if (!candidate.labels.contains(name) ||
        candidate.labels.get(name).get() != value) {
      return false;
    }
  }

  return true;
}


// Parse an array of labels from a JSON object.
static Try<hashmap<string, string>> parseLabels(
    const JSON::Object& json,
    const string& path)
{
  Result<JSON::Array> array = json.find<JSON::Array>(path);
  if (array.isError()) {
    return Error("Could not find path '" + path +
                 "' in json: " + array.error());
  }

  if (array.isNone()) {
    return hashmap<string, string>();
  }

  hashmap<string, string> labels;

  foreach (const JSON::Value& label_, array.get().values) {
    if (!label_.is<JSON::Object>()) {
      return Error("Label is not a json object: " + stringify(label_));
    }
    JSON::Object label = label_.as<JSON::Object>();

    Result<JSON::String> name = label.find<JSON::String>("name");
    if (name.isError() || name.isNone()) {
      return Error("No name for label: " + stringify(label));
    }

    Result<JSON::String> value = label.find<JSON::String>("value");
    if (value.isError() || value.isNone()) {
      return Error("No value for label: " + stringify(label));
    }

    labels[name.get().value] = value.get().value;
  }

  return labels;
}


static Try<vector<AppcImage::Dependency>> parseDependencies(
    const JSON::Object& json)
{
  Result<JSON::Array> array = json.find<JSON::Array>("dependencies");
  if (array.isError()) {
    return Error("Failed to parse dependencies: " + array.error());
  }

  if (array.isNone()) {
    return vector<AppcImage::Dependency>();
  }

  vector<AppcImage::Dependency> dependencies;

  // Index to construct path to the dependency's labels.
  unsigned int index = 0;
  foreach (const JSON::Value& dependency_, array.get().values) {
    if (!dependency_.is<JSON::Object>()) {
      return Error("Failed to parse dependency: " + stringify(dependency_));
    }
    JSON::Object dependency = dependency_.as<JSON::Object>();

    Result<JSON::String> name = dependency.find<JSON::String>("imageName");
    if (name.isError() || name.isNone()) {
      return Error("Failed to find app name for dependency: " +
                  stringify(dependency));
    }

    Result<JSON::String> hash = dependency.find<JSON::String>("imageId");
    if (name.isError()) {
      return Error("Failed to find imageId for dependency: " +
                  stringify(dependency));
    }

    Try<hashmap<string, string>> labels_ =
      parseLabels(json, "dependencies[" + stringify(index) + "].labels");
    if (labels_.isError()) {
      return Error("Failed to parse labels for dependency '" +
                  stringify(dependency) + "': " + labels_.error());
    }

    dependencies.push_back(AppcImage::Dependency(
          name.get().value,
          labels_.get(),
          hash.isSome() ? Option<string>(hash.get().value) : None()));

    index++;
  }

  return dependencies;
}


Try<AppcImage> AppcImage::parse(
    const string& manifest,
    const string& hash,
    const string& path)
{
  Try<JSON::Object> json = JSON::parse<JSON::Object>(manifest);
  if (json.isError()) {
    return Error("Failed to parse manifest json: " + json.error());
  }

  // Parse the name.
  Result<JSON::String> name = json.get().find<JSON::String>("name");
  if (name.isNone()) {
    return Error("Failed to find name in manifest");
  }
  if (name.isError()) {
    return Error("Failed to parse name from manifest: " + name.error());
  }

  // Parse the labels.
  Try<hashmap<string, string>> labels_ = parseLabels(json.get(), "labels");
  if (labels_.isError()) {
    return Error("Failed to parse labels: " + labels_.error());
  }

  // Parse the dependencies.
  Try<vector<Dependency>> dependencies_ = parseDependencies(json.get());
  if (dependencies_.isError()) {
    return Error("Failed to parse dependencies: " + dependencies_.error());
  }

  return AppcImage(
      name.get().value,
      labels_.get(),
      hash,
      dependencies_.get(),
      path,
      json.get());
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
    const ContainerInfo::Image& image,
    const string& directory)
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
  Try<Nothing> mkdir = os::mkdir(flags.provisioner_rootfs_dir);
  if (mkdir.isError()) {
    return Error("Failed to create provisioner rootfs directory '" +
                 flags.provisioner_rootfs_dir + "': " + mkdir.error());
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
  // TODO(idownes): Implement this, if a need arises.

  return Nothing();
}


Future<string> AppcProvisionerProcess::provision(
    const ContainerID& containerId,
    const ContainerInfo::Image::AppC& image)
{
  // TODO(idownes): Check containerId not already provision{ing,ed}.

  hashmap<string, string> labels;
  foreach (const Label& label, image.labels().labels()) {
    labels[label.key()] = label.value();
  }

  return fetch(image.name(), image.id(), labels)
    .then(defer(self(),
                &Self::_provision,
                containerId,
                lambda::_1));
}


Future<string> AppcProvisionerProcess::_provision(
    const ContainerID& containerId,
    const vector<AppcImage>& images)
{
  // Create root directory.
  string base = path::join(flags.provisioner_rootfs_dir,
                           stringify(containerId));

  string rootfs = path::join(base, "rootfs");

  Try<Nothing> mkdir = os::mkdir(base);
  if (mkdir.isError()) {
    return Failure("Failed to create directory for container filesystem: " +
                    mkdir.error());
  }

  LOG(INFO) << "Provisioning rootfs for container '" << containerId << "'"
            << " to '" << base << "'";

  return backend->provision(images, base)
    .then([=] () -> Future<string> {
      // Bind mount the rootfs to itself so we can pivot_root. We do
      // it now so any subsequent mounts by the containerizer or
      // isolators are correctly handled by pivot_root.
      Try<Nothing> mount =
        fs::mount(rootfs, rootfs, None(), MS_BIND | MS_SHARED, NULL);

      if (mount.isError()) {
        return Failure("Failure to bind mount rootfs: " + mount.error());
      }

      return rootfs;
    });
}


// Fetch an image and all dependencies.
Future<vector<AppcImage>> AppcProvisionerProcess::fetch(
    const string& name,
    const Option<string>& hash,
    const hashmap<string, string>& labels)
{
  return store->get(name)
    .then(defer(self(),
                &Self::_fetch,
                name,
                hash,
                labels,
                lambda::_1));
}


Future<vector<AppcImage>> AppcProvisionerProcess::_fetch(
    const string& name,
    const Option<string>& hash,
    const hashmap<string, string>& labels,
    const vector<AppcImage>& candidates)
{
  foreach (const AppcImage& candidate, candidates) {
    if (AppcImage::matches(name, hash, labels, candidate)) {
      LOG(INFO) << "Found matching image in store for image '" << name << "'";

      return fetchDependencies(candidate);
    }
  }

  LOG(INFO) << "No match found for image '" << name << "'"
            << " in image store, starting discovery";

  return discovery->discover(name, labels, hash)
    .then(defer(self(), [=](const string& uri) { return store->put(uri); }))
    .then(defer(self(), [=](const AppcImage& candidate) -> Future<AppcImage> {
            if (AppcImage::matches(name, hash, labels, candidate)) {
              return candidate;
            }

            return Failure("Fetched image (" + candidate.name + ", " +
                           candidate.hash + ")" +
                           "' does not match (" + name + ", " + "'" +
                           (hash.isSome() ? hash.get() : "no id") + ")");
          }))
    .then(defer(self(),
                &Self::fetchDependencies,
                lambda::_1));
}


Future<vector<AppcImage>> AppcProvisionerProcess::fetchDependencies(
    const AppcImage& image)
{
  Result<JSON::Array> dependencies =
    image.manifest.find<JSON::Array>("dependencies");

  if (dependencies.isNone() ||
      dependencies.get().values.size() == 0) {
    // We're at a leaf layer.
    return vector<AppcImage>{image};
  }

  // Sequentially fetch dependencies.
  // TODO(idownes): Detect recursive dependencies.
  // TODO(idownes): Consider fetching in parallel?
  list<Future<vector<AppcImage>>> futures;

  foreach (const JSON::Value& dependency, dependencies.get().values) {
    CHECK(dependency.is<JSON::Object>());

    JSON::Object json = dependency.as<JSON::Object>();

    Result<JSON::String> name = json.find<JSON::String>("imageName");
    if (!name.isSome()) {
      return Failure("Failed to parse dependency name");
    }

    Result<JSON::String> id = json.find<JSON::String>("imageID");
    if (id.isError()) {
      return Failure("Failed to parse dependency id");
    }

    Try<hashmap<string, string>> labels = parseLabels(json, "labels");
    if (labels.isError()) {
      return Failure("Failed to parse dependency labels");
    }

    LOG(INFO) << "Fetching dependency '" << name.get().value << "'";

    if (futures.empty()) {
      Future<vector<AppcImage>> f = vector<AppcImage>();
      futures.push_back(f.then(defer(self(),
                                     &Self::fetch,
                                     name.get().value,
                                     id.isSome() ? id.get().value
                                                 : Option<string>(),
                                     labels.get())));
    } else {
      futures.push_back(
          futures.back().then(defer(self(),
                                    &Self::fetch,
                                    name.get().value,
                                    id.isSome() ? id.get().value
                                                : Option<string>(),
                                    labels.get())));
    }
  }

  return collect(futures)
    .then(defer(self(), &Self::_fetchDependencies, image, lambda::_1));
}


Future<vector<AppcImage>> AppcProvisionerProcess::_fetchDependencies(
    const AppcImage& image,
    const list<vector<AppcImage>>& dependencies)
{
  vector<AppcImage> images;

  CHECK(dependencies.size());
  foreach (const vector<AppcImage>& dependency, dependencies) {
    CHECK(dependency.size());
    images.insert(images.end(), dependency.begin(), dependency.end());
  }

  images.push_back(image);

  return images;
}


Future<Nothing> AppcProvisionerProcess::destroy(const ContainerID& containerId)
{
  string base = path::join(flags.provisioner_rootfs_dir,
                           stringify(containerId));

  string rootfs = path::join(base, "rootfs");

  LOG(INFO) << "Destroying container rootfs for container '"
            << containerId << "'"
            << " at '" << rootfs << "'";

  Try<fs::MountInfoTable> mountTable = fs::MountInfoTable::read();

  if (mountTable.isError()) {
    return Failure("Failed to read mount table: " + mountTable.error());
  }

  foreach (const fs::MountInfoTable::Entry& entry, mountTable.get().entries) {
    if (strings::startsWith(entry.target, base)) {
      fs::unmount(entry.target, MNT_DETACH);
    }
  }

  return backend->destroy(base);
}

} // namespace appc {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
