FROM java:openjdk-7-jdk

MAINTAINER Xinglang Wang <wangxinglang@gmail.com>

ENV project_version 1.0-SNAPSHOT
ENV project_name metriccalculator

COPY target/${project_name}-${project_version}-bin.tar.gz /opt/app/${project_name}-${project_version}-bin.tar.gz
WORKDIR /opt/app
RUN tar -zxvf ${project_name}-${project_version}-bin.tar.gz
RUN ln -s /opt/app/${project_name}-${project_version} jetstreamapp
WORKDIR /opt/app/jetstreamapp

# App config
ENV JETSTREAM_APP_JAR_NAME ${project_name}.jar
ENV JETSTREAM_APP_NAME ${project_name}
ENV JETSTREAM_CONFIG_VERSION 1.0

# Dependency
ENV JETSTREAM_ZKSERVER_HOST zkserver
ENV JETSTREAM_ZKSERVER_PORT 2181
ENV JETSTREAM_MONGOURL mongo://mongoserver:27017/config
ENV PULSAR_KAFKA_BORKERS kafkaserver:9092
ENV PULSAR_CASSANDRA cassandraserver

EXPOSE 9999 15599 15600 15601
ENTRYPOINT ./start.sh
