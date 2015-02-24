/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.distributor.replay;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.support.AdviceProcessor;

@ManagedResource(objectName = "Event/MultipleTopicsOBMAdviceProcessor", description = "Multiple topics outbound messaging channel Advice processor")
public class MultipleTopicsOBCAdviceProcessor extends AdviceProcessor {
    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        String key = JetstreamReservedKeys.EventReplayTopic.toString();

        if (event.containsKey(key)) {
            String retryTopic = (String) event.get(key);
            if (retryTopic != null && retryTopic.length() != 0) {
                retryTopic = "Replay-" + retryTopic.replaceAll("/", "-");
                event.setForwardingTopics(new String[] { retryTopic });
            }
        }

        super.sendEvent(event);
    }
}