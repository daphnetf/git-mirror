
FROM ubuntu:16.04

RUN apt-get update --fix-missing && apt-get upgrade -y
RUN apt-get update

RUN apt-get install -y git python3 python3-pip
RUN pip3 install pyyaml


CMD python3 /usr/bin/run.py

