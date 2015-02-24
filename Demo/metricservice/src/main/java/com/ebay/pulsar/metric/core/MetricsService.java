/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

import java.util.List;

import com.ebay.pulsar.metric.impl.cassandra.QueryParam;
import com.google.common.util.concurrent.ListenableFuture;

public interface MetricsService {
    ListenableFuture<List<Counter>> findCounters(String metric, String group);
    ListenableFuture<List<RawNumericMetric>> findData(QueryParam queryParam);
}
