#!/bin/bash

set -xe

# This is the script used by ASF Jenkins to build and check Mesos for
# a given OS and compiler combination.

# Default compiler is gcc-4.8
COMPILER="${COMPILER:-gcc-4.8}"

# Require the following environment variables to be set.
: ${OS:?"Environment variable 'OS' must be set"}
: ${CONFIGURATION:?"Environment variable 'CONFIGURATION' must be set"}
: ${COMPILER:?"Environment variable 'COMPILER' must be set"}

# Change to the root of Mesos repo for docker build context.
MESOS_DIRECTORY=$( cd "$( dirname "$0" )/.." && pwd )
cd "$MESOS_DIRECTORY"

# Generate a random image tag.
TAG=mesos-$(date +%s)-$RANDOM

# Build the target operating system/compiler image
OS_IMG="mesosbuild/jenkins:$OS-$COMPILER"
docker build -t $OS_IMG support/docker/$OS-$COMPILER

# Set a trap to delete the image on exit.
trap "docker rmi $OS_IMG" EXIT

# Create the docker image containing the source code to build.
echo "
FROM $OS_IMG
WORKDIR mesos
COPY . /mesos/
CMD bash -c -l './bootstrap && ./configure $CONFIGURATION && DISTCHECK_CONFIGURE_FLAGS=\"$CONFIGURATION\" GLOG_v=1 MESOS_VERBOSE=1 make -j8 distcheck'" > Dockerfile

docker build -t $TAG .

# Uncomment below to print kernel log in case of failures.
# trap "dmesg" ERR

# Now run the image.
# NOTE: We run in 'privileged' mode to circumvent permission issues
# with AppArmor. See https://github.com/docker/docker/issues/7276.
docker run --privileged $TAG
