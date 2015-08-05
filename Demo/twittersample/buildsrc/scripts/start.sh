#!/bin/sh
# Default NIC use eth0
export JETSTREAM_NETMASK=`ip -f inet addr show eth0 | grep inet | awk "{print \\$2}"`

# JETSTREAM_MONGOURL must be in this form mongo://<mongohost>:<mongoport>/Jetstream
if [ ! -z "$JETSTREAM_MONGOURL" ]; then
    export MONGO_HOME=$JETSTREAM_MONGOURL
fi

export JETSTREAM_HOME=$(pwd)

if [ -z "$JETSTREAM_APP_JAR_NAME" ]; then
    list=$(echo *.jar)
    count=0

    for str in $list
    do
        JETSTREAM_APP_JAR_NAME=$str
        if [ -z "$JETSTREAM_APP_NAME" ]; then
            JETSTREAM_APP_NAME=`echo "$str" | cut -d"." -f1`
        fi
        count=`expr $count + 1`
        if [ $count -gt 1 ]
        then
                echo "Aborting! more than 1 app in current folder"
                exit 1
        else
                echo "found app $JETSTREAM_APP_NAME"
        fi
    done
fi


java \
    $JETSTREAM_JAVA_OPTS \
    -Djetstream.context.baseport="${JETSTREAM_CONTEXT_BASEPORT:-15591}" \
    -Djetstream.runtime.zkserver.host="${JETSTREAM_ZKSERVER_HOST:-127.0.0.1}" \
    -Djetstream.runtime.zkserver.port="${JETSTREAM_ZKSERVER_PORT:-2181}" \
    -Djetstream.runtime.netmask="$JETSTREAM_NETMASK" \
    -Dtwitter4j.oauth.consumerKey="$TWITTER4J_OAUTH_CONSUMERKEY" \
    -Dtwitter4j.oauth.consumerSecret="$TWITTER4J_OAUTH_CONSUMERSECRET" \
    -Dtwitter4j.oauth.accessToken="$TWITTER4J_OAUTH_ACCESSTOKEN" \
    -Dtwitter4j.oauth.accessTokenSecret="$TWITTER4J_OAUTH_ACCESSTOKENSECRET" \
    -jar $JETSTREAM_APP_JAR_NAME \
    -n $JETSTREAM_APP_NAME \
    -cv ${JETSTREAM_CONFIG_VERSION:-1.0} \
    -p ${JETSTREAM_APP_PORT:-9999}
