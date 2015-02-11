/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

import java.util.Map;

public class RawNumericMetric implements NumericMetric {

    public RawNumericMetric(String metricName, String groupId, long timestamp, long value) {
        super();
        this.metricName = metricName;
        this.groupId = groupId;
        this.timestamp = timestamp;
        this.value = value;
    }

    private String metricName;
    
    private String groupId;

    private long value;

    private long timestamp;
    
    private Map<String, String> tagMap;

    public RawNumericMetric() {
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getTagMap() {
        return tagMap;
    }

    public void setTagMap(Map<String, String> tagMap) {
        this.tagMap = tagMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
        result = prime * result
                + ((metricName == null) ? 0 : metricName.hashCode());
        result = prime * result + ((tagMap == null) ? 0 : tagMap.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        result = prime * result + (int) (value ^ (value >>> 32));
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
        RawNumericMetric other = (RawNumericMetric) obj;
        if (groupId == null) {
            if (other.groupId != null)
                return false;
        } else if (!groupId.equals(other.groupId))
            return false;
        if (metricName == null) {
            if (other.metricName != null)
                return false;
        } else if (!metricName.equals(other.metricName))
            return false;
        if (tagMap == null) {
            if (other.tagMap != null)
                return false;
        } else if (!tagMap.equals(other.tagMap))
            return false;
        if (timestamp != other.timestamp)
            return false;
        if (value != other.value)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RawNumericMetric [metricName=" + metricName + ", groupId="
                + groupId + ", value=" + value + ", timestamp=" + timestamp
                + ", tagMap=" + tagMap + "]";
    }
    
}
