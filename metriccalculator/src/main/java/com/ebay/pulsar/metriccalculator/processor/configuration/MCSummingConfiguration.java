/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor.configuration;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.pulsar.metriccalculator.cache.OffHeapCacheConfig;

public class MCSummingConfiguration extends AbstractNamedBean implements
        XSerializable, InitializingBean {

    private boolean enableGroupByCounter = true;
    private Map<String, Integer> metricsThreshold = new HashMap<String, Integer>();
    private Map<String, OffHeapCacheConfig> offheapMetricConf = new HashMap<String, OffHeapCacheConfig>();
    private int groupCounterMax = 1000000;

    public Map<String, Integer> getMetricsThreshold() {
        return metricsThreshold;
    }

    public void setMetricsThreshold(Map<String, Integer> metricsThreshold) {
        this.metricsThreshold = metricsThreshold;
    }

    public Map<String, OffHeapCacheConfig> getOffheapMetricConf() {
        return offheapMetricConf;
    }

    public void setOffheapMetricConf(
            Map<String, OffHeapCacheConfig> offheapMetricConf) {
        this.offheapMetricConf = offheapMetricConf;
    }

    public boolean isEnableGroupByCounter() {
        return enableGroupByCounter;
    }

    public void setEnableGroupByCounter(boolean enableGroupByCounter) {
        this.enableGroupByCounter = enableGroupByCounter;
    }

    public int getGroupCounterMax() {
        return groupCounterMax;
    }

    public void setGroupCounterMax(int groupCounterMax) {
        this.groupCounterMax = groupCounterMax;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    }
}
