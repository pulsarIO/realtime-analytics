/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.util;

public class MetricDef {
    private String metricName;
    private String metricGroup;
    private String metricTable;
    private long metricTime;
    private long frequency;

    private Object affinityKey;
    private String[] topics;

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public String getMetricTable() {
        return metricTable;
    }

    public void setMetricTable(String metricTable) {
        this.metricTable = metricTable;
    }

    public Object getAffinityKey() {
        return affinityKey;
    }

    public void setAffinityKey(Object affinityKey) {
        this.affinityKey = affinityKey;
    }

    public long getMetricTime() {
        return metricTime;
    }

    public void setMetricTime(long metricTime) {
        this.metricTime = metricTime;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricGroup() {
        return metricGroup;
    }

    public void setMetricGroup(String metricGroup) {
        this.metricGroup = metricGroup;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (frequency ^ (frequency >>> 32));
        result = prime * result
                + ((metricGroup == null) ? 0 : metricGroup.hashCode());
        result = prime * result
                + ((metricName == null) ? 0 : metricName.hashCode());
        result = prime * result + (int) (metricTime ^ (metricTime >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricDef other = (MetricDef) obj;
        if (frequency != other.frequency)
            return false;
        if (metricGroup == null) {
            if (other.metricGroup != null)
                return false;
        } else if (!metricGroup.equals(other.metricGroup))
            return false;
        if (metricName == null) {
            if (other.metricName != null)
                return false;
        } else if (!metricName.equals(other.metricName))
            return false;
        if (metricTime != other.metricTime)
            return false;
        return true;
    }
}
