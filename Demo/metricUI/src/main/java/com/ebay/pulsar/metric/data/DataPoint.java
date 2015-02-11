/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
 */
package com.ebay.pulsar.metric.data;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ebay.jetstream.event.JetstreamEvent;

public class DataPoint {
    private long timestamp;
    private Queue<JetstreamEvent> events;

    public DataPoint(Long timestamp) {
        this.timestamp = timestamp;
        this.events = new ConcurrentLinkedQueue<JetstreamEvent>();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Queue<JetstreamEvent> getEvents() {
        return events;
    }

    public void addEvent(JetstreamEvent event) {
        this.events.add(event);
    }
}
