/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private AtomicInteger threadNumber = new AtomicInteger(0);

    private String poolName = "MetricsThreadPool";

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, poolName + "-" + threadNumber.getAndIncrement());
        t.setDaemon(false);
        t.setUncaughtExceptionHandler(this);

        return t;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
    }
}
