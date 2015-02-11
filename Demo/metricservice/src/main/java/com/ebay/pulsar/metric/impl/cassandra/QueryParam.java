/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.impl.cassandra;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryParam {

    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String METRIC_TIME = "metricTime";
    public static final String METRIC_NAME = "metricName";
    public static final String GROUP_ID = "groupId";
    public static final String TOPN = "topn";

    public static final String COLUMN_FAMILYNAME = "columnFamilyName";

    private Map<String, Object> backedMap = new LinkedHashMap<String, Object>();
    private String columnFamilyName;

    private QueryParam() {
    }

    public QueryParam columnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
        return this;
    }

    public QueryParam groupId(String groupId) {
        backedMap.put(GROUP_ID, groupId);
        return this;
    }

    public QueryParam metricName(String metricName) {
        backedMap.put(METRIC_NAME, metricName);
        return this;
    }

    public QueryParam startTime(long startTime) {
        long _metricTime = (startTime / 60000) * 60000;
        backedMap.put(START_TIME, new Date(_metricTime));
        return this;
    }

    public QueryParam endTime(long endTime) {
        long _metricTime = (endTime / 60000) * 60000;
        backedMap.put(END_TIME, new Date(_metricTime));
        return this;
    }

    public QueryParam metricTime(long metricTime) {
        long _metricTime = metricTime;
        backedMap.put(METRIC_TIME, new Date(_metricTime));
        return this;
    }
    
    public QueryParam topN(Integer topN) {
        backedMap.put(TOPN, topN);
        return this;
    }

    public QueryParam addParam(String name, String value) {
        backedMap.put(name, value);
        return this;
    }
    


    public String getQueryColumnFamilyName() {
        return this.columnFamilyName;
    }

    public Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(backedMap);
    }

    public static QueryParam build() {
        return new QueryParam();
    }
    
    @Override
    public String toString() {
        return "QueryParam [backedMap=" + backedMap + ", columnFamilyName="
                + columnFamilyName + "]";
    }
}
