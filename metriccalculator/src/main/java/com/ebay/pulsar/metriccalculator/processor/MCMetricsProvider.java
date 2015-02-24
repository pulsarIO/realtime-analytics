/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor;

import java.util.Set;

public interface MCMetricsProvider {
    public void collectMetrics(Set<String> counterNames, boolean flushNormal);
}
