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

#include <stdio.h>

#include <string>

#include <mesos/mesos.hpp>
#include <mesos/executor.hpp>

#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/subprocess.hpp>
#include <process/reap.hpp>
#include <process/owned.hpp>

#include <stout/flags.hpp>
#include <stout/os.hpp>

#include "common/status_utils.hpp"

#include "docker/docker.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

using std::cerr;
using std::cout;
using std::endl;
using std::string;

namespace mesos {
namespace internal {

using namespace mesos;
using namespace process;


// Executor that is responsible to execute a docker container, and
// redirect log output to configured stdout and stderr files.
// Similar to the CommandExecutor, it is only responsible to launch
// one container and exits afterwards. It also sends a TASK_RUNNING
// update after docker run with the docker info output in the status
// update.
// The executor also assumes it is launched from the
// DockerContainerizer, which already calls setsid before launching
// the executor.
class DockerExecutorProcess : public ProtobufProcess<DockerExecutorProcess>
{
public:
  DockerExecutorProcess(
      const Owned<Docker>& docker,
      const string& container,
      const string& sandboxDirectory,
      const string& mappedDirectory)
    : launched(false),
      killed(false),
      docker(docker),
      container(container),
      sandboxDirectory(sandboxDirectory),
      mappedDirectory(mappedDirectory) {}

  virtual ~DockerExecutorProcess() {}

  void registered(
      ExecutorDriver* _driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    cout << "Registered docker executor on " << slaveInfo.hostname() << endl;
    driver = _driver;
  }

  void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered docker executor on " << slaveInfo.hostname() << endl;
  }

  void disconnected(ExecutorDriver* driver) {}

  void __sendDockerInfo(
      const Owned<Promise<Nothing>>& promise,
      ExecutorDriver* driver,
      const TaskID& taskId,
      bool retry,
      const Future<string>& dockerInfo)
  {
    if (dockerInfo.isReady()) {
      TaskStatus status;
      status.mutable_task_id()->MergeFrom(taskId);
      status.set_state(TASK_RUNNING);
      status.set_data(dockerInfo.get());
      driver->sendStatusUpdate(status);
      runningSent = true;
      promise->set(Nothing());
    } else if (retry) {
      // Keep retrying until we get inspect information, as
      // the container might not be ready yet.
      dispatch(self(), &Self::_sendDockerInfo, promise, driver, taskId, retry);
    } else {
      promise->set(Nothing());
    }
  }

  void _sendDockerInfo(
      const Owned<Promise<Nothing>>& promise,
      ExecutorDriver* driver,
      const TaskID& taskId,
      bool retry)
  {
    if (runningSent) {
      promise.set(Nothing());
      return;
    }

    docker->inspect_string(container)
      .onAny(defer(
          self(),
          &Self::__sendDockerInfo,
	  promise,
          driver,
          taskId,
	  retry,
          lambda::_1));
  }

  Future<Nothing> sendDockerInfo(
      ExecutorDriver* driver,
      const TaskID& taskId,
      bool retry)
  {
    Owned<Promise<Nothing>> promise(new Promise<Nothing>());
    _sendDockerInfo(promise, driver, taskId, retry);
    return promise->future();
  }

  void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    if (launched) {
      TaskStatus status;
      status.mutable_task_id()->MergeFrom(task.task_id());
      status.set_state(TASK_FAILED);
      status.set_message(
          "Attempted to run multiple tasks using a \"docker\" executor");
      driver->sendStatusUpdate(status);
      return;
    }

    cout << "Starting task " << task.task_id().value() << endl;

    // We assume the Docker executor is launched from the
    // DockerContainerizer, which already calls setsid before
    // launching the executor.

    CHECK(task.has_container());
    CHECK(task.has_command());

    ContainerInfo containerInfo = task.container();

    CHECK(containerInfo.type() == ContainerInfo::DOCKER);

    Future<Nothing> run = docker->run(
        containerInfo,
        task.command(),
        container,
        sandboxDirectory,
        mappedDirectory,
        task.resources() + task.executor().resources(),
        None(),
        false);

    run.onAny(defer(
        self(),
        &Self::reaped,
        driver,
        task.task_id(),
        lambda::_1));

    dockerRun = run;

    sendDockerInfo(driver, task.task_id(), true);

    launched = true;
  }

  void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    shutdown(driver);
  }

  void frameworkMessage(ExecutorDriver* driver, const string& data) {}

  void shutdown(ExecutorDriver* driver)
  {
    cout << "Shutting down" << endl;

    if (dockerRun.isSome() && !killed) {
      Future<Nothing> dockerRun_ = dockerRun.get();
      dockerRun_.discard();
      killed = true;
    }
  }

  virtual void error(ExecutorDriver* driver, const string& message) {}

private:
  void _reaped(
      ExecutorDriver* driver,
      const TaskStatus& status)
  {
    driver->sendStatusUpdate(status);

    // A hack for now ... but we need to wait until the status update
    // is sent to the slave before we shut ourselves down.
    os::sleep(Seconds(1));
    driver->stop();
  }

  void reaped(
      ExecutorDriver* driver,
      const TaskID& taskId,
      const Future<Nothing>& run)
  {
    TaskState state;
    string message;
    if (killed) {
      state = TASK_KILLED;
    } else if (!run.isReady()) {
      state = TASK_FAILED;
      message = "Docker container run error: " +
                (run.isFailed() ? run.failure() : "future discarded");
    } else {
      state = TASK_FINISHED;
    }

    TaskStatus taskStatus;
    taskStatus.mutable_task_id()->MergeFrom(taskId);
    taskStatus.set_state(state);
    taskStatus.set_message(message);

    if (!runningSent) {
      // The executor has't sent status TASK_RUNNING with docker info,
      // so we try again as a last effort to do so.
      sendDockerInfo(driver, taskId, false)
	.onAny(defer(self(), &Self::_reaped, driver, taskStatus));
    } else {
      _reaped(driver, taskStatus);
    }
  }

  bool launched;
  bool killed;
  bool runningSent;
  Owned<Docker> docker;
  string container;
  string sandboxDirectory;
  string mappedDirectory;
  Option<Future<Nothing>> dockerRun;
  Option<ExecutorDriver*> driver;
};


class DockerExecutor : public Executor
{
public:
  DockerExecutor(
      const Owned<Docker>& docker,
      const string& container,
      const string& sandboxDirectory,
      const string& mappedDirectory)
  {
    process = new DockerExecutorProcess(
        docker,
        container,
        sandboxDirectory,
        mappedDirectory);

    spawn(process);
  }

  virtual ~DockerExecutor()
  {
    terminate(process);
    wait(process);
    delete process;
  }

  virtual void registered(
      ExecutorDriver* driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process,
             &DockerExecutorProcess::registered,
             driver,
             executorInfo,
             frameworkInfo,
             slaveInfo);
  }

  virtual void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process,
             &DockerExecutorProcess::reregistered,
             driver,
             slaveInfo);
  }

  virtual void disconnected(ExecutorDriver* driver)
  {
    dispatch(process, &DockerExecutorProcess::disconnected, driver);
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    dispatch(process, &DockerExecutorProcess::launchTask, driver, task);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    dispatch(process, &DockerExecutorProcess::killTask, driver, taskId);
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data)
  {
    dispatch(process, &DockerExecutorProcess::frameworkMessage, driver, data);
  }

  virtual void shutdown(ExecutorDriver* driver)
  {
    dispatch(process, &DockerExecutorProcess::shutdown, driver);
  }

  virtual void error(ExecutorDriver* driver, const string& data)
  {
    dispatch(process, &DockerExecutorProcess::error, driver, data);
  }

private:
  DockerExecutorProcess* process;
};

} // namespace internal {
} // namespace mesos {


void usage(const char* argv0, const flags::FlagsBase& flags)
{
  cerr << "Usage: " << os::basename(argv0).get() << " [...]" << endl
       << endl
       << "Supported options:" << endl
       << flags.usage();
}


class Flags : public mesos::internal::logging::Flags
{
public:
  Flags()
  {
    add(&Flags::container,
        "container",
        "The name of the docker container to run.\n");

    add(&Flags::docker,
        "docker",
        "The path to the docker cli executable.\n");

    add(&Flags::sandbox_directory,
        "sandbox_directory",
        "The path to the container sandbox that stores stdout and stderr\n"
        "files that is being redirected with docker container logs.\n");

    add(&Flags::mapped_directory,
        "mapped_directory",
        "The sandbox directory path that is mapped in the docker container.\n");
  }

  Option<string> container;
  Option<string> docker;
  Option<string> sandbox_directory;
  Option<string> mapped_directory;
};


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  Flags flags;

  bool help;
  flags.add(&help,
            "help",
            "Prints this help message",
            false);

  // Load flags from environment and command line.
  Try<Nothing> load = flags.load(None(), &argc, &argv);

  if (load.isError()) {
    cerr << load.error() << endl;
    usage(argv[0], flags);
    return -1;
  }

  mesos::internal::logging::initialize(argv[0], flags, true); // Catch signals.

  if (help) {
    usage(argv[0], flags);
    return -1;
  }

  if (flags.docker.isNone()) {
    LOG(WARNING) << "Expected docker executable path";
    usage(argv[0], flags);
    return 0;
  }

  if (flags.container.isNone()) {
    LOG(WARNING) << "Expected container name";
    usage(argv[0], flags);
    return 0;
  }

  if (flags.sandbox_directory.isNone()) {
    LOG(WARNING) << "Expected sandbox directory path";
    usage(argv[0], flags);
    return 0;
  }

  if (flags.mapped_directory.isNone()) {
    LOG(WARNING) << "Expected mapped sandbox directory path";
    usage(argv[0], flags);
    return 0;
  }

  // We skip validation when creating docker abstraction as we
  // don't want to be checking for cgroups in the executor.
  Try<Docker*> docker = Docker::create(flags.docker.get(), false);
  if (docker.isError()) {
    LOG(WARNING) << "Unable to create docker abstraction: " << docker.error();
    return -1;
  }

  mesos::internal::DockerExecutor executor(
      process::Owned<Docker>(docker.get()),
      flags.container.get(),
      flags.sandbox_directory.get(),
      flags.mapped_directory.get());

  mesos::MesosExecutorDriver driver(&executor);
  return driver.run() == mesos::DRIVER_STOPPED ? 0 : 1;
}
