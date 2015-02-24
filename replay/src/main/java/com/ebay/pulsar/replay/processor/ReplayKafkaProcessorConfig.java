/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.replay.processor;

import com.ebay.jetstream.event.processor.kafka.SimpleKafkaProcessorConfig;

/**
 * @author weifang
 * 
 */
public class ReplayKafkaProcessorConfig extends SimpleKafkaProcessorConfig {
    public static final String KAFKA_TIMESTAMP_KEY = "js_evt_kafka_produce_ts";
    public static final int DFLT_MAX_READ_RATE = 5000;

    private long delayInMs = 60 * 1000;

    private long retentionInMs = 24 * 3600 * 1000; // default 6 hours.

    private String timestampKey = KAFKA_TIMESTAMP_KEY;

    public void setDelayInMs(long delayInMs) {
        this.delayInMs = delayInMs;
    }

    public void setTimestampKey(String timestampKey) {
        this.timestampKey = timestampKey;
    }

    public long getRetentionInMs() {
        return retentionInMs;
    }

    public void setRetentionInMs(long retentionInMs) {
        this.retentionInMs = retentionInMs;
    }

    public long getDelayInMs() {
        return delayInMs;
    }

    public String getTimestampKey() {
        return timestampKey;
    }
}
