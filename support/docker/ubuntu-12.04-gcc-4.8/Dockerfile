FROM ubuntu:12.04

# Install dependencies.
RUN apt-get update
RUN apt-get -y install build-essential git maven autoconf libtool
RUN apt-get -y install icedtea-7-plugin openjdk-7-jdk python-dev python-boto libcurl4-nss-dev libsasl2-dev libapr1-dev libsvn-dev libevent-dev
RUN update-java-alternatives -s java-1.7.0-openjdk-amd64

# Add an unpriviliged user.
RUN adduser --disabled-password --gecos '' mesos

# Install dependencies.
RUN apt-get install -y python-software-properties
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get -y install gcc-4.8 g++-4.8

# Set environment variables.
ENV CC gcc-4.8
ENV CXX g++-4.8

ENV GTEST_FILTER -SlaveRecoveryTest/0.*:MesosContainerizerSlaveRecoveryTest.*:CpuIsolatorTest/1.*:RevocableCpuIsolatorTest.*:LimitedCpuIsolatorTest.*:MemIsolatorTest/*.*:CgroupsAnyHierarchyTest.*:CgroupsAnyHierarchyWithCpuMemoryTest.*:CgroupsAnyHierarchyWithCpuAcctMemoryTest.*:CgroupsNoHierarchyTest.*:CgroupsAnyHierarchyWithFreezerTest.*:CgroupsAnyHierarchyMemoryPressureTest.*:MemoryPressureMesosTest.*:NamespacesPidIsolatorTest.*:UserCgroupIsolatorTest/*.*:ContainerizerTest.*:FsTest.*:LinuxFilesystemIsolatorTest.*
