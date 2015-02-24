/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.metric;

public enum MetricFrequency {
    ONE_MINUTE(60000), FIVE_MINUTES(300000), TEN_MINUTES(600000), THIRTY_MINUTES(
            1800000);

    private long period;

    private MetricFrequency(long period) {
        this.period = period;
    }

    public long getValue() {
        return period;
    }

    public static MetricFrequency valueOf(int freq) {
        for (MetricFrequency frequency : values()) {
            if (frequency.getValue() == freq) {
                return frequency;
            }
        }

        return null;
    }
}
