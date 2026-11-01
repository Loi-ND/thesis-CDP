FROM ubuntu:24.04

RUN mkdir /workspace &&\
    apt update &&\
    apt install -y mysql-client

COPY java /workspace/java
COPY starrocks /workspace/starrocks

RUN mkdir -p /workspace/starrocks/fe/metadata &&\
    mkdir -p /workspace/starrocks/be/data

ENV JAVA_HOME=/workspace/java
ENV STARROCKS_HOME=/workspace/starrocks

ENV PATH=${PATH}:${JAVA_HOME}/bin:${STARROCKS_HOME}/apache_hdfs_broker/bin
ENV PATH=${PATH}:${STARROCKS_HOME}/be/bin:${STARROCKS_HOME}/fe/bin