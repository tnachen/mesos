FROM mesosbuild/jenkins:ubuntu-14.04-gcc-4.8

RUN apt-get -y install apt-transport-https ca-certificates curl lxc iptables ipcalc aufs-tools

RUN echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
RUN apt-get update -qq && \
            DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -qqy \
                lxc-docker-1.6.2 \
                && \
            apt-get clean

VOLUME /var/lib/docker

ADD wrapdocker /usr/local/bin/wrapdocker

CMD sudo GTEST_FILTER='*DOCKER*' chmod +x /usr/local/bin/wrapdocker
