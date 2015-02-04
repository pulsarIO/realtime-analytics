/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.support.AbstractEventSource;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.util.BinaryFormatSerializer;
import com.ebay.pulsar.sessionizer.util.Constants;

/**
 * An event producer to send events to sessionizer cluster.
 * 
 * @author xingwang
 *
 */
public class LoopbackEventProducer extends AbstractEventSource implements InitializingBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoopbackEventProducer.class);

    private static final String AFFINITY_KEY = JetstreamReservedKeys.MessageAffinityKey.toString();
    private final AtomicLong totalOwnershipChangedSessionCounter = new AtomicLong();
    private final AtomicLong totalRestoredSessionSent = new AtomicLong();

    @Override
    public void afterPropertiesSet() throws Exception {

    }
    @Override
    public void pause() {

    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {

    }

    @Override
    public void resume() {

    }

    public void forwardSessionEndEvent(String identifier, Session session, String uid) {
        JetstreamEvent jsEvent = createEvent(session, Constants.EVENT_TYPE_SESSION_TRANSFERED_EVENT);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_UNIQUEID, uid);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_TYPE, Integer.valueOf(session.getType()));
        totalOwnershipChangedSessionCounter.incrementAndGet();
        jsEvent.put(AFFINITY_KEY, session.getAffinityKey());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Transfer event: {}", jsEvent);
        }
        super.fireSendEvent(jsEvent);
    }

    public void sendExpirationCheckEvent(JetstreamEvent jsEvent) {
        totalRestoredSessionSent.incrementAndGet();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Transfer event: {}", jsEvent);
        }
        super.fireSendEvent(jsEvent);
    }

    private JetstreamEvent createEvent(Session session, String type) {
        JetstreamEvent jsEvent = new JetstreamEvent();
        jsEvent.setEventType(type);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_PAYLOAD, BinaryFormatSerializer.getInstance().getSessionPayload(session).array());
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_METADATA, BinaryFormatSerializer.getInstance().getSessionMetadata(session).array());
        return jsEvent;
    }

    public long getTotalOwnershipChangedSessionSent () {
        return totalOwnershipChangedSessionCounter.get();
    }
    public long getTotalRestoredSessionSent() {
        return totalRestoredSessionSent.get();
    }
}
