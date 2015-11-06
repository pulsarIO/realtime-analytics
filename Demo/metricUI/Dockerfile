FROM java:openjdk-7-jdk

ENV project_version 1.0.2
ENV project_name metricUI

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
ENV METRIC_SERVER_HOST metricservice
ENV METRIC_SERVER_PORT 8083
ENV METRIC_CALCULATOR_HOST metriccalculator
ENV METRIC_CALCULATOR_PORT 9999

# One http port, One context port
ENV JETSTREAM_REST_BASEPORT 8088
ENV JETSTREAM_CONTEXT_BASEPORT 15590
ENV JETSTREAM_APP_PORT 9999

EXPOSE 9999 15590 8088
ENTRYPOINT ./start.sh
