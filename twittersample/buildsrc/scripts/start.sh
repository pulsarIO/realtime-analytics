#!/bin/sh
# Default NIC use eth0
export JETSTREAM_NETMASK=`ip -f inet addr show eth0 | grep inet | awk "{print \\$2}"`

# JETSTREAM_MONGOURL must be in this form mongo://<mongohost>:<mongoport>/Jetstream
export MONGO_HOME=$JETSTREAM_MONGOURL
export JETSTREAM_HOME=$(pwd)
  
java \
    $JETSTREAM_JAVA_OPTS \
    -Djetstream.runtime.zkserver.host="$JETSTREAM_ZKSERVER_HOST" \
    -Djetstream.runtime.zkserver.port="$JETSTREAM_ZKSERVER_PORT" \
    -Djetstream.runtime.netmask="$JETSTREAM_NETMASK" \
    -Dtwitter4j.oauth.consumerKey="$TWITTER4J_OAUTH_CONSUMERKEY" \
    -Dtwitter4j.oauth.consumerSecret="$TWITTER4J_OAUTH_CONSUMERSECRET" \
    -Dtwitter4j.oauth.accessToken="$TWITTER4J_OAUTH_ACCESSTOKEN" \
    -Dtwitter4j.oauth.accessTokenSecret="$TWITTER4J_OAUTH_ACCESSTOKENSECRET" \
    -jar $JETSTREAM_APP_JAR_NAME \
    -n $JETSTREAM_APP_NAME \
    -cv $JETSTREAM_CONFIG_VERSION \
    -p 9999 
