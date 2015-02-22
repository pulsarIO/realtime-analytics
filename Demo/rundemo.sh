#!/bin/sh

echo '>>> Start external dependency'
sudo docker run -d --name zkserver -t "jetstream/zookeeper"
sleep 5
sudo docker run -d --name mongoserver -t "mongo"
sleep 5
sudo docker run -d --name kafkaserver -t "jetstream/kafka"
sleep 5
sudo docker run -d --name cassandraserver -t "poklet/cassandra"

echo '>>> Sleep 30 seconds'
sleep 30
mkdir /tmp/pulsarcql
wget https://raw.githubusercontent.com/pulsarIO/realtime-analytics/master/metriccalculator/pulsar.cql -O /tmp/pulsarcql/pulsar.cql
sudo docker run -it --rm --link cassandraserver:cass1 -v /tmp/pulsarcql:/data poklet/cassandra bash -c 'cqlsh $CASS1_PORT_9160_TCP_ADDR -f /data/pulsar.cql'

echo '>>> Sleep 10 seconds'
sleep 10

echo '>>> Start Pulsar pipeline'
sudo docker run -d -p 0.0.0.0:8000:9999 -p 0.0.0.0:8081:8080 --link zkserver:zkserver --link mongoserver:mongoserver -t "jetstream/config"
sleep 5
sudo docker run -d -p 0.0.0.0:8001:9999 --link zkserver:zkserver --link mongoserver:mongoserver --link kafkaserver:kafkaserver -t "pulsar/replay"
sleep 5
sudo docker run -d -p 0.0.0.0:8003:9999 --link zkserver:zkserver --link mongoserver:mongoserver --link kafkaserver:kafkaserver -t "pulsar/sessionizer"
sleep 5
sudo docker run -d -p 0.0.0.0:8004:9999 --link zkserver:zkserver --link mongoserver:mongoserver --link kafkaserver:kafkaserver -t "pulsar/distributor"
sleep 5
sudo docker run -d -p 0.0.0.0:8005:9999 --name metriccalculator --link zkserver:zkserver --link mongoserver:mongoserver --link kafkaserver:kafkaserver  --link cassandraserver:cassandraserver -t "pulsar/metriccalculator"
sleep 5
sudo docker run -d -p 0.0.0.0:8002:9999 -p 0.0.0.0:8080:8080 --link zkserver:zkserver --link mongoserver:mongoserver --link kafkaserver:kafkaserver -t "pulsar/collector"
sleep 5

echo '>>> Start demo'
sudo docker run -d -p 0.0.0.0:8006:9999 -p 0.0.0.0:8083:8083 --name metricservice --link zkserver:zkserver --link mongoserver:mongoserver --link cassandraserver:cassandraserver -t "pulsar/metricservice"
sleep 5
sudo docker run -d -p 0.0.0.0:8007:9999 -p 0.0.0.0:8088:8088 --link zkserver:zkserver --link mongoserver:mongoserver --link metricservice:metricservice --link metriccalculator:metriccalculator -t "pulsar/metricui"

# The twitter sample use the twitter4j api, to run the app, it requires twitter OAUTH token (https://dev.twitter.com/oauth/overview/application-owner-access-tokens)
#sleep 5
#sudo docker run -d -p 0.0.0.0:8008:9999 --link zkserver:zkserver --link mongoserver:mongoserver -e TWITTER4J_OAUTH_CONSUMERKEY= -e TWITTER4J_OAUTH_CONSUMERSECRET=  -e TWITTER4J_OAUTH_ACCESSTOKEN=  -e TWITTER4J_OAUTH_ACCESSTOKENSECRET= -t pulsar/twittersample
#sleep 5
