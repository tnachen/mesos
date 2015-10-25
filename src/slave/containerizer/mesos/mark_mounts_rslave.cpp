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

#include "slave/containerizer/mesos/mark_mounts_rslave.hpp"

#include <iostream>
#include <string>

#include <stout/nothing.hpp>
#include <stout/try.hpp>

#include "linux/fs.hpp"

using std::cerr;
using std::endl;
using std::string;

namespace mesos {
namespace internal {
namespace slave {

const string MesosContainerizerMarkMountsRslave::NAME = "mark_mounts_rslave";


MesosContainerizerMarkMountsRslave::Flags::Flags()
{
  add(&rootdir,
      "rootdir",
      "The root directory to start recursively mark mounts as slave.");
}


int MesosContainerizerMarkMountsRslave::execute()
{
  if (flags.rootdir.isNone()) {
    cerr << "Flag --rootdir is not specified" << endl;
    return 1;
  }

  Try<Nothing> mount = mesos::internal::fs::mount(
      None(),
      flags.rootdir.get(),
      None(),
      MS_SLAVE | MS_REC,
      NULL);
  if (mount.isError()) {
    cerr << "Failed to make rslave with rootDir '" << flags.rootdir.get()
         << "': " << mount.error();
    return 1;
  }

  return 0;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
