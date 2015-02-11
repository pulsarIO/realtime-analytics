FROM java:openjdk-7-jdk

MAINTAINER Pulsar

COPY target/metricUI-1.0-SNAPSHOT-bin.tar.gz /opt/app/metricUI-1.0-SNAPSHOT-bin.tar.gz
WORKDIR /opt/app
RUN tar -zxvf metricUI-1.0-SNAPSHOT-bin.tar.gz
RUN ln -s /opt/app/metricUI-1.0-SNAPSHOT jetstreamapp
WORKDIR /opt/app/jetstreamapp

# App config
ENV JETSTREAM_APP_JAR_NAME metricUI.jar
ENV JETSTREAM_APP_NAME metricUI
ENV JETSTREAM_CONFIG_VERSION 1.0

# Dependency
ENV JETSTREAM_ZKSERVER_HOST zkserver
ENV JETSTREAM_ZKSERVER_PORT 2181
ENV JETSTREAM_MONGOURL mongo://mongoserver:27017/config
ENV METRIC_SERVER_HOST metricservice
ENV METRIC_SERVER_PORT 8083

EXPOSE 9999 15610 8088
ENTRYPOINT ./start.sh
