/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

public interface NumericMetric {
    String getMetricName();
    String getGroupId();
    long getValue();
    long getTimestamp();
}
