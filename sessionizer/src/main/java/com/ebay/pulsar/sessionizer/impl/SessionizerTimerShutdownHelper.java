/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import com.ebay.jetstream.common.ShutDownable;

/**
 * Help bean for sessionizer to shutdown timer first.
 * 
 * @author xingwang
 *
 */
public class SessionizerTimerShutdownHelper implements ShutDownable {
    private SessionizerProcessor processor;


    @Override
    public int getPendingEvents() {
        return 0;
    }

    public void setProcessor(SessionizerProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void shutDown() {
        processor.shutdownTimer();
    }
}
