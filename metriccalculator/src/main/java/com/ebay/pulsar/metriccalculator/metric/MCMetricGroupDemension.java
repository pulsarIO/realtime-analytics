/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.metric;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MCMetricGroupDemension implements Externalizable {
    private String metricName;
    private String groupId;

    private Map<String, String> dimensions;
    private String internalMetricName;

    public MCMetricGroupDemension() {
    }

    public MCMetricGroupDemension(String metricName, String groupId) {
        if (groupId == null || groupId.trim().isEmpty())
            throw new IllegalArgumentException(
                    "Metric groupId cann't be null or empty.");
        this.metricName = metricName;
        this.groupId = groupId;

        StringBuilder nameBuilder = new StringBuilder().append(".")
                .append(this.metricName).append(".").append(this.groupId);
        internalMetricName = nameBuilder.toString();
    }

    public MCMetricGroupDemension(String metricName, String groupId,
            Map<String, String> dimensions) {
        if (groupId == null || groupId.trim().isEmpty())
            throw new IllegalArgumentException(
                    "Metric groupId cann't be null or empty.");
        this.metricName = metricName;
        this.groupId = groupId;

        this.dimensions = new HashMap<String, String>(6);
        if (dimensions != null) {
            for (Entry<String, String> entry : dimensions.entrySet()) {
                if (entry.getValue() == null)
                    throw new IllegalArgumentException("Dimension ["
                            + entry.getKey() + "] value cann't be null.");
                this.dimensions.put(entry.getKey(), entry.getValue());
            }
        }

        StringBuilder nameBuilder = new StringBuilder().append(".")
                .append(this.metricName).append(".").append(this.groupId)
                .append(".").append(this.dimensions);

        internalMetricName = nameBuilder.toString();
    }

    public String getMetricName() {
        return metricName;
    }

    public String getGroupId() {
        return groupId;
    }

    public Map<String, String> getDimensions() {
        if (dimensions != null) {
            return Collections.unmodifiableMap(dimensions);
        } else
            return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MCMetricGroupDemension that = (MCMetricGroupDemension) o;
        return internalMetricName.equals(that.internalMetricName);
    }

    @Override
    public int hashCode() {
        return internalMetricName.hashCode();
    }

    @Override
    public String toString() {
        return internalMetricName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(metricName);
        out.writeObject(groupId);
        out.writeObject(internalMetricName);
        if (dimensions != null) {
            out.writeInt(dimensions.size());
        } else {
            out.writeInt(-1);
        }
        if (dimensions != null) {
            Iterator<Entry<String, String>> itr = dimensions.entrySet()
                    .iterator();
            while (itr.hasNext()) {
                Map.Entry<String, String> entry1 = itr.next();
                out.writeObject(entry1.getKey());
                out.writeObject(entry1.getValue());
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        metricName = (String) in.readObject();
        groupId = (String) in.readObject();
        internalMetricName = (String) in.readObject();

        int mapSize = in.readInt();
        if (mapSize > 0) {
            dimensions = new HashMap<String, String>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                dimensions.put((String) in.readObject(),
                        (String) in.readObject());
            }
        }
    }
}
