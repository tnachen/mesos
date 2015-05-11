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

#include <algorithm>
#include <string>
#include <vector>

#include <stout/gtest.hpp>
#include <stout/hashmap.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include <process/gtest.hpp>

#include "slave/containerizer/fetcher.hpp"

#include "slave/containerizer/provisioners/appc.hpp"

#include "slave/containerizer/provisioners/appc/hash.hpp"
#include "slave/containerizer/provisioners/appc/store.hpp"
#include "slave/containerizer/provisioners/appc/discovery.hpp"

#include "tests/flags.hpp"
#include "tests/utils.hpp"

using std::string;
using std::vector;

using namespace process;
/*
using mesos::internal::slave::AppcImage;
using mesos::internal::slave::AppcProvisioner;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::LocalDiscovery;
using mesos::internal::slave::Provisioner;
using mesos::internal::slave::SHA512;
using mesos::internal::slave::Store;
using mesos::internal::slave::StoreProcess;
*/

namespace mesos {
namespace internal {
namespace tests {
/*
#define DEFAULT_LABELS                    \
  ({ hashmap<string, string> labels;      \
     labels["version"] = "0.0.1";         \
     labels["os"] = "linux";              \
     labels["arch"] = "amd64";            \
     labels; })

struct AppcDependency {
  AppcDependency(
      const std::string& _name,
      const hashmap<std::string, std::string>& _labels,
      const Option<std::string>& _hash)
    : name(_name), labels(_labels), hash(_hash) {}

  std::string name;
  hashmap<std::string, std::string> labels;
  Option<std::string> hash;
};

class AppcTest : public TemporaryDirectoryTest
{
public:
  AppcTest() {}

  Try<AppcImage> createImage(
      const string& name,
      const hashmap<string, string>& labels,
      const vector<AppcDependency>& dependencies,
      const hashmap<string, string>& files,
      const slave::Flags& flags)
  {
    Try<string> base = os::mkdtemp(path::join(sandbox, "XXXXXX"));
    EXPECT_SOME(base);

    const string rootfs = path::join(base.get(), "rootfs");
    EXPECT_SOME(os::mkdir(rootfs));

    foreachpair (const string& path, const string& contents, files) {
      EXPECT_SOME(os::write(path::join(rootfs, path), contents));
    }

    JSON::Object manifest_ = manifest(name, labels, dependencies);

    EXPECT_SOME(os::write(
          path::join(base.get(), "manifest"),
          stringify(manifest_)));

    const string tarball = path::join(
        flags.provisioner_local_dir,
        AppcImage::canonicalize(name, labels).get() + ".aci");

    const string command =
      "tar -C " + base.get() + " -f " + tarball + " -c rootfs manifest";

    if (os::system(command) != 0) {
      return Error("Failed to tar image");
    }

    Future<string> hash = SHA512::hash(tarball);
    hash.await(Seconds(1));
    if (!hash.isReady()) {
      return Error("Failed to hash image: " + hash.failure());
    }

    return AppcImage(manifest_, hash.get(), flags.provisioner_local_dir);
  }

  slave::Flags createSlaveFlags()
  {
    slave::Flags flags;
    flags.launcher_dir = path::join(tests::flags.build_dir, "src");

    flags.provisioners = "appc";
    flags.provisioner_discovery  = "local";
    flags.provisioner_backend    = "copy";

    flags.provisioner_store_dir  = path::join(sandbox, "store");
    flags.provisioner_local_dir  = path::join(sandbox, "images");
    flags.provisioner_rootfs_dir = path::join(sandbox, "containers");

    return flags;
  }

protected:
  virtual void SetUp()
  {
    TemporaryDirectoryTest::SetUp();

    sandbox = os::getcwd();

    slave::Flags flags = createSlaveFlags();

    EXPECT_SOME(os::mkdir(flags.provisioner_store_dir));
    EXPECT_SOME(os::mkdir(flags.provisioner_local_dir));
    EXPECT_SOME(os::mkdir(flags.provisioner_rootfs_dir));
  }

private:
  static JSON::Object manifest(
      const string& name,
      const hashmap<string, string>& labels,
      const vector<AppcDependency>& dependencies)
  {
    JSON::Object m;

    m.values["name"] = name;

    JSON::Array labels_;

    foreachpair (const string& name, const string& value, labels) {
      JSON::Object label;
      label.values["name"] = name;
      label.values["value"] = value;

      labels_.values.push_back(label);
    }

    m.values["labels"] = labels_;

    JSON::Array dependencies_;

    foreach (const AppcDependency& dependency, dependencies) {
      JSON::Object dependency_;

      dependency_.values["imageName"] = dependency.name;

      JSON::Array labels;

      foreachpair (const string& name, const string& value, dependency.labels) {
        JSON::Object label;
        label.values["name"] = name;
        label.values["value"] = value;

        labels.values.push_back(label);
      }

      dependency_.values["labels"] = labels;

      if (dependency.hash.isSome()) {
        dependency_.values["imageID"] = "sha512-" + dependency.hash.get();
      }

      dependencies_.values.push_back(dependency_);
    }

    m.values["dependencies"] = dependencies_;

    return m;
  }

  string sandbox;
};

*/

/*
TEST_F(AppcTest, SHA512)
{
  string content = "blah blah";
  string expected = "0196b566757d3a0dcdbb716b024cfeb2"
                    "56be22715af93942b392a126a3ce93b2"
                    "c6c60dd45ee39540b5494dbfdce58678"
                    "c41a69ba9ff155ce691ce093755bfe46";

  string filename = path::join(os::getcwd(), "input");
  ASSERT_SOME(os::write(filename, content));

  Future<string> hash = SHA512::hash(filename);
  AWAIT_READY(hash);
  EXPECT_EQ(hash.get(), expected);
}




TEST_F(AppcTest, manifest)
{
  // TODO(idownes): Write helpers for manifests and images.
  string manifest = R"json(
{"name": "test",
 "labels": [{"name": "version", "value": "0.0.1"},
            {"name": "os", "value": "linux"},
            {"name": "arch", "value": "amd64"}],
 "dependencies": [{"imageName": "test-dependency",
                  "labels": [{"name": "version", "value": "0.0.2"},
                              {"name": "os", "value": "freebsd"},
                              {"name": "arch", "value": "i386"}]}]
}
)json";

  CHECK_SOME(AppcImage::parseManifest(manifest));
}
*/

/*
TEST_F(AppcTest, Parse)
{
  string manifest = R"json(
{
    "acKind": "ImageManifest",
    "acVersion": "0.5.2",
    "name": "example.com/reduce-worker",
    "labels": [
        {
            "name": "version",
            "value": "1.0.0"
        },
        {
            "name": "arch",
            "value": "amd64"
        },
        {
            "name": "os",
            "value": "linux"
        }
    ],
    "app": {
        "exec": [
            "/usr/bin/reduce-worker",
            "--quiet"
        ],
        "user": "100",
        "group": "300",
        "eventHandlers": [
            {
                "exec": [
                    "/usr/bin/data-downloader"
                ],
                "name": "pre-start"
            },
            {
                "exec": [
                    "/usr/bin/deregister-worker",
                    "--verbose"
                ],
                "name": "post-stop"
            }
        ],
        "workingDirectory": "/opt/work",
        "environment": [
            {
                "name": "REDUCE_WORKER_DEBUG",
                "value": "true"
            }
        ],
        "isolators": [
            {
                "name": "resource/cpu",
                "value": {
                    "request": "250",
                    "limit": "500"
                }
            },
            {
                "name": "resource/memory",
                "value": {
                    "request": "1G",
                    "limit": "2G"
                }
            },
            {
                "name": "os/linux/capabilities-retain-set",
                "value": {
                    "set": ["CAP_NET_BIND_SERVICE"]
                }
            }
        ],
        "mountPoints": [
            {
                "name": "work",
                "path": "/var/lib/work",
                "readOnly": false
            }
        ],
        "ports": [
            {
                "name": "health",
                "port": 4000,
                "protocol": "tcp",
                "socketActivated": true
            },
            {
                "name": "ftp-data",
                "port": 20000,
                "count": 1000,
                "protocol": "tcp"
            }
        ]
    },
    "dependencies": [
        {
            "imageName": "example.com/reduce-worker-base",
            "imageID": "sha512-...",
            "labels": [
                {
                    "name": "os",
                    "value": "linux"
                },
                {
                    "name": "env",
                    "value": "canary"
                }
            ],
            "size": 22017258
        }
    ],
    "pathWhitelist": [
        "/etc/ca/example.com/crt",
        "/usr/bin/map-reduce-worker",
        "/opt/libs/reduce-toolkit.so",
        "/etc/reduce-worker.conf",
        "/etc/systemd/system/"
    ],
    "annotations": [
        {
            "name": "authors",
            "value": "Carly Container <carly@example.com>, Nat Network <[nat@example.com](mailto:nat@example.com)>"
        },
        {
            "name": "created",
            "value": "2014-10-27T19:32:27.67021798Z"
        },
        {
            "name": "documentation",
            "value": "https://example.com/docs"
        },
        {
            "name": "homepage",
            "value": "https://example.com"
        }
    ]
}
)json";

  Try<JSON::Object> json = AppcImage::parse(manifest);

  CHECK_SOME(json);
}
*/

/*
TEST_F(AppcTest, store)
{
  slave::Flags flags = createSlaveFlags();

  // Create an image - basic manifest and rootfs tar'red up.
  string rootfs = path::join(os::getcwd(), "rootfs");
  ASSERT_SOME(os::mkdir(rootfs));
  ASSERT_SOME(os::touch(path::join(rootfs, "run")));

  string manifest = R"json(
{"name": "test",
 "labels": [{"name": "version", "value": "0.0.1"},
            {"name": "os", "value": "linux"},
            {"name": "arch", "value": "amd64"}]}
)json";
  ASSERT_SOME(os::write(path::join(os::getcwd(), "manifest"), manifest));

  ASSERT_EQ(os::system("tar cf test-0.0.1-linux-amd64.aci manifest rootfs"), 0);

  Fetcher fetcher;

  // Put image into store. Check return AppcImage matches expected.
  Try<Owned<Store>> store = Store::create(flags, &fetcher);
  ASSERT_SOME(store);

  Future<AppcImage> stored = store.get()->put(
      path::join("file:///", os::getcwd(), "test-0.0.1-linux-amd64.aci"));

  AWAIT_READY(stored);

  EXPECT_EQ(stored.get().name, "test");

  EXPECT_NE(stored.get().path, os::getcwd());

  EXPECT_TRUE(os::exists(path::join(stored.get().path, "manifest")));

  EXPECT_TRUE(os::exists(
        path::join(stored.get().path, "rootfs", "run")));

  // Get image based on name. Check it matches expected.
  Future<vector<AppcImage>> retrieved = store.get()->get("test");
  AWAIT_READY(retrieved);

  EXPECT_EQ(retrieved.get().size(), 1);

  EXPECT_EQ(stored.get().hash, retrieved.get()[0].hash);
}


inline ContainerInfo::Image convert(const AppcImage& image)
{
  ContainerInfo::Image image_;
  image_.set_type(ContainerInfo::Image::APPC);
  image_.mutable_appc()->set_name(image.name);
  image_.mutable_appc()->set_id("sha512-" + image.hash);

  Labels* labels = image_.mutable_appc()->mutable_labels();
  foreachpair (const string& name, const string& value, image.labels) {
    Label* label = labels->add_labels();
    label->set_key(name);
    label->set_value(value);
  }

  return image_;
}


TEST_F(AppcTest, ROOT_Layers)
{
  slave::Flags flags = createSlaveFlags();

  Fetcher fetcher;
  Try<Owned<Provisioner>> provisioner =
    AppcProvisioner::create(flags, &fetcher);
  ASSERT_SOME(provisioner);

  // Layer A.
  Try<AppcImage> layerA = createImage(
      "layerA",
      DEFAULT_LABELS,
      vector<AppcDependency>(),
      hashmap<string, string>{{"/foo", "A"}, {"/bar", "A"}},
      flags);

  ASSERT_SOME(layerA);

  // Layer B.
  Try<AppcImage> layerB = createImage(
      "layerB",
      DEFAULT_LABELS,
      vector<AppcDependency>(),
      hashmap<string, string>{{"/bar", "B"}, {"/baz", "B"}},
      flags);

  ASSERT_SOME(layerB);

  // Stack A then B.
  Try<AppcImage> stackAB = createImage(
      "stackAB",
      DEFAULT_LABELS,
      vector<AppcDependency>{
        AppcDependency(layerA.get().name,
                              layerA.get().labels,
                              layerA.get().hash),
        AppcDependency(layerB.get().name,
                              layerB.get().labels,
                              layerB.get().hash)
        },
      hashmap<string, string>{},
      flags);

  ASSERT_SOME(stackAB);

  // Provision stackAB and check files.
  ContainerID containerAB;
  containerAB.set_value("containerAB");

  Future<string> rootfsAB = provisioner.get()->provision(
    containerAB,
    convert(stackAB.get()));

  AWAIT_READY(rootfsAB);

  EXPECT_SOME_EQ("A", os::read(path::join(rootfsAB.get(), "foo")));
  EXPECT_SOME_EQ("B", os::read(path::join(rootfsAB.get(), "bar")));
  EXPECT_SOME_EQ("B", os::read(path::join(rootfsAB.get(), "baz")));

  AWAIT_READY(provisioner.get()->destroy(containerAB));

  // Stack B then A.
  Try<AppcImage> stackBA = createImage(
      "stackBA",
      DEFAULT_LABELS,
      vector<AppcDependency>{
        AppcDependency(layerB.get().name,
                              layerB.get().labels,
                              layerB.get().hash),
        AppcDependency(layerA.get().name,
                              layerA.get().labels,
                              layerA.get().hash)
        },
      hashmap<string, string>{},
      flags);

  ASSERT_SOME(stackBA);

  // Provision stackBA and check files.
  ContainerID containerBA;
  containerBA.set_value("containerBA");

  Future<string> rootfsBA = provisioner.get()->provision(
    containerBA,
    convert(stackBA.get()));

  AWAIT_READY(rootfsBA);

  EXPECT_SOME_EQ("A", os::read(path::join(rootfsBA.get(), "foo")));
  EXPECT_SOME_EQ("A", os::read(path::join(rootfsBA.get(), "bar")));
  EXPECT_SOME_EQ("B", os::read(path::join(rootfsBA.get(), "baz")));

  AWAIT_READY(provisioner.get()->destroy(containerBA));
}
*/

/*
TEST_F(AppcTest, LocalDiscovery)
{
  string name = "test";

  hashmap<string, string> labels;
  labels["version"] = "0.0.1";
  labels["os"] = "linux";
  labels["arch"] = "amd64";

  EXPECT_SOME_EQ("test-0.0.1-linux-amd64", AppcImage::canonicalize(name, labels));

  string local = "/tmp/mesos/images";

  LocalDiscovery discovery(local);

  Future<vector<string>> discover = discovery.discover(name, labels);
  AWAIT_READY(discover);
  ASSERT_EQ(discover.get().size(), 1);
  EXPECT_SOME_EQ(discover.get()[0],
            path::join("file:///",
                       local,
                       AppcImage::canonicalize(name, labels) + ".aci"));
}
*/


// TODO(idownes): Add more tests.

} // namespace tests {
} // namespace internal {
} // namespace mesos {
