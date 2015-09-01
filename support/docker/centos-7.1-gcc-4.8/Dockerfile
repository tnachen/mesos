FROM centos:7

# Install dependencies.
RUN yum groupinstall -y 'Development Tools'
RUN yum install -y epel-release
RUN yum install -y git maven
RUN yum install -y java-1.7.0-openjdk-devel python-devel zlib-devel libcurl-devel openssl-devel cyrus-sasl-devel cyrus-sasl-md5 apr-devel subversion-devel apr-utils-devel libevent-devel

# Set environment variables.
ENV CC gcc
ENV CXX g++

ENV GTEST_FILTER -SlaveRecoveryTest/0.*:MesosContainerizerSlaveRecoveryTest.*:CpuIsolatorTest/1.UserCpuUsage:CpuIsolatorTest/1.SystemCpuUsage:RevocableCpuIsolatorTest.ROOT_CGROUPS_RevocableCpu:LimitedCpuIsolatorTest.*:MemIsolatorTest/1.MemUsage:ContainerizerTest.ROOT_CGROUPS_BalloonFramework:CgroupsAnyHierarchyTest.*:CgroupsAnyHierarchyWithCpuMemoryTest.*:CgroupsAnyHierarchyWithCpuAcctMemoryTest.*:CgroupsAnyHierarchyWithFreezerTest.*:CgroupsAnyHierarchyMemoryPressureTest.*:MemoryPressureMesosTest.*:MemIsolatorTest/0.MemUsage:MemIsolatorTest/2.MemUsage:NamespacesPidIsolatorTest.ROOT_PidNamespace:UserCgroupIsolatorTest/0.ROOT_CGROUPS_UserCgroup:UserCgroupIsolatorTest/1.ROOT_CGROUPS_UserCgroup:CgroupsNoHierarchyTest.ROOT_CGROUPS_NOHIERARCHY_MountUnmountHierarchy:FsTest.FileSystemTableRead:LaunchChrootTest/0.ROOT_DifferentRoot:NsTest.ROOT_setns:NsTest.ROOT_getns:NsTest.ROOT_destroy:SchedTest.ROOT_PolicySelf:SchedTest.ROOT_PolicyChild:LinuxFilesystemIsolatorTest.*:AuthenticationTest.UnauthenticatedSlave
