/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.ebay.pulsar.metriccalculator.util.NamedThreadFactory;

public class MCScheduler {

    private static ScheduledExecutorService timer = Executors
            .newScheduledThreadPool(1,
                    new NamedThreadFactory("MCSchedulerTick"));

    public static ScheduledExecutorService getMCScheduler() {
        return timer;
    }
}
