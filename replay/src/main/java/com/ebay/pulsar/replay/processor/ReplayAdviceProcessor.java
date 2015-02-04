/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.replay.processor;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.support.AdviceProcessor;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;

@ManagedResource(objectName = "Event/ReplayAdviceProcessor", description = "Replay Advice processor")
public class ReplayAdviceProcessor extends AdviceProcessor {
    private int checkIntervalInMilliseconds = 5 * 60 * 1000;

    @ManagedAttribute
    public int getCheckIntervalInMilliseconds() {
        return checkIntervalInMilliseconds;
    }

    public void setCheckIntervalInMilliseconds(int checkIntervalInMilliseconds) {
        this.checkIntervalInMilliseconds = checkIntervalInMilliseconds;
    }

    private long lastSentTimeStamp = 0L;

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        //When the replay happens, put it into event backMap map so that it will be serialized into Kafka.
        if (event.getMetaData(Constants.REPLAY_INITIAL_TS) != null) {
            long initTs = (Long) event.getMetaData(Constants.REPLAY_INITIAL_TS);
            event.put(Constants.REPLAY_INITIAL_TS, initTs);
        }
        if (event.getMetaData(Constants.REPLAY_LAST_TS) != null) {
            long lastTs = (Long) event.getMetaData(Constants.REPLAY_LAST_TS);
            event.put(Constants.REPLAY_LAST_TS, lastTs);
        }

        super.sendEvent(event);

        long current = System.currentTimeMillis();
        if (lastSentTimeStamp == 0) {
            lastSentTimeStamp = current;
            this.stopReplay();
            postAlert(
                    "The topic: "
                            + event.get(JetstreamReservedKeys.EventReplayTopic.toString())
                            + " has issue, replay occurs on replay agent",
                            AlertStrength.RED);
            return;
        }

        if (lastSentTimeStamp +  getCheckIntervalInMilliseconds() < current) {
            lastSentTimeStamp = current;
            this.stopReplay();
            postAlert(
                    "The topic: "
                            + event.get(JetstreamReservedKeys.EventReplayTopic.toString())
                            + " has issue, replay occurs on replay agent",
                            AlertStrength.RED);
        }
    }
}
