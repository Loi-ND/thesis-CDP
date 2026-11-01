FROM python:3.12.3

RUN mkdir /workspace
RUN mkdir -p /workspace/spark-events
WORKDIR /workspace

COPY java /workspace/java
ENV JAVA_HOME=/workspace/java
ENV PATH=$PATH::$JAVA_HOME/bin

COPY spark /workspace/spark
ENV SPARK_HOME=/workspace/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

ENV PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip