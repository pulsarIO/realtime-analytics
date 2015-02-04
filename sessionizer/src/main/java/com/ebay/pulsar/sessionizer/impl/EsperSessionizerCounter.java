/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventConsumer;

/**
 * A counter manager for troubleshooting purpose.
 * 
 * @author xingwang
 *
 */
@ManagedResource(objectName = "EsperSessionizerCounter", description = "Counter for debugging")
public class EsperSessionizerCounter {
    private final Map<String, LongCounter> counterMap = new ConcurrentHashMap<String, LongCounter>();
    private Date startTime = new Date();

    private final SessionizerProcessor processor;

    public EsperSessionizerCounter(SessionizerProcessor processor) {
        this.processor = processor;
    }


    public void addCount(String name, int count) {
        LongCounter counter = counterMap.get(name);
        if (counter == null) {
            counter = new LongCounter();
            counterMap.put(name, counter);
        }
        counter.addAndGet(count);
    }

    @ManagedAttribute
    public Map<String, LongCounter> getCustomizedCounters() {
        return counterMap;
    }

    @ManagedAttribute
    public Map<Integer, SessionizerCounterManager> getBuiltInCounters() {
        return processor.collectDetailStatistics();
    }


    @ManagedAttribute
    public long getHostd() {
        return EventConsumer.getConsumerId();
    }

    @ManagedAttribute
    public long getServerCurrentTime() {
        return System.currentTimeMillis();
    }

    public String getResetTime() {
        return startTime.toString();
    }

    @ManagedOperation
    public void reset() {
        counterMap.clear();
        startTime = new Date();
    }
}
