
FROM ubuntu:16.04

RUN apt-get update --fix-missing && apt-get upgrade -y
RUN apt-get update

RUN apt-get install -y git python3 python3-pip curl sudo
RUN pip3 install pyyaml

RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
RUN apt-get install -y git-lfs

CMD bash -c "python3 /usr/bin/run.py 2>&1"

