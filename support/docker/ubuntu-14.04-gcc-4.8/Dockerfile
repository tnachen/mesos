FROM ubuntu:14.04

# Install dependencies.
RUN apt-get update
RUN apt-get -y install build-essential git maven autoconf libtool
RUN apt-get -y install openjdk-7-jdk python-dev python-boto libcurl4-nss-dev libsasl2-dev libapr1-dev libsvn-dev libevent-dev

# Add an unpriviliged user.
RUN adduser --disabled-password --gecos '' mesos

# Install dependencies.
RUN apt-get update
RUN apt-get -y install gcc-4.8

# Set environment variables.
ENV CC gcc-4.8
ENV CXX g++-4.8

ENV GTEST_FILTER -SlaveRecoveryTest/0.*:MesosContainerizerSlaveRecoveryTest.*:CpuIsolatorTest/1.*:RevocableCpuIsolatorTest.*:LimitedCpuIsolatorTest.*:MemIsolatorTest/*.*:CgroupsAnyHierarchyTest.*:CgroupsAnyHierarchyWithCpuMemoryTest.*:CgroupsAnyHierarchyWithCpuAcctMemoryTest.*:CgroupsNoHierarchyTest.*:CgroupsAnyHierarchyWithFreezerTest.*:CgroupsAnyHierarchyMemoryPressureTest.*:MemoryPressureMesosTest.*:NamespacesPidIsolatorTest.*:UserCgroupIsolatorTest/*.*:ContainerizerTest.*:FsTest.*:LinuxFilesystemIsolatorTest.*