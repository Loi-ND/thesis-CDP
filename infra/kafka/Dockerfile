FROM python:3.12-slim

RUN mkdir /workspace 
RUN mkdir -p /workspace/kafka/logs
RUN pip install kafka-python flask requests "certifi>=2026.5.20" "msgpack>=1.1.2" "numpy>=2.4.6" "pandas>=3.0.3" "urllib3>=2.7.0" "websockets>=16.0"

WORKDIR /workspace

COPY ./java /workspace/java
COPY ./kafka_2.13-4.1.1 /workspace/kafka


ENV JAVA_HOME=/workspace/java
ENV PATH=$PATH:$JAVA_HOME/bin

ENV KAFKA_HOME=/workspace/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin