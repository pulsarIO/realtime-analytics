/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    protected final String id;
    private final int priority;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id) {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority) {

        this.id = id;
        this.priority = priority;
    }

    public Thread newThread(Runnable runnable) {
        String name = id + ":" + n.getAndIncrement();
        Thread thread = new Thread(runnable, name);
        thread.setPriority(priority);
        thread.setDaemon(true);
        return thread;
    }
}