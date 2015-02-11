FROM java:openjdk-7-jdk

MAINTAINER Pulsar

COPY target/metricservice-1.0-SNAPSHOT-bin.tar.gz /opt/app/metricservice-1.0-SNAPSHOT-bin.tar.gz
WORKDIR /opt/app
RUN tar -zxvf metricservice-1.0-SNAPSHOT-bin.tar.gz
RUN ln -s /opt/app/metricservice-1.0-SNAPSHOT jetstreamapp
WORKDIR /opt/app/jetstreamapp

# App config
ENV JETSTREAM_APP_JAR_NAME metricservice.jar
ENV JETSTREAM_APP_NAME metricservice
ENV JETSTREAM_CONFIG_VERSION 1.0

# Dependency
ENV JETSTREAM_ZKSERVER_HOST zkserver
ENV JETSTREAM_ZKSERVER_PORT 2181
ENV JETSTREAM_MONGOURL mongo://mongoserver:27017/config
ENV PULSAR_CASSANDRA cassandraserver

EXPOSE 8083 9999
ENTRYPOINT ./start.sh
