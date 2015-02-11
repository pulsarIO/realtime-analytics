/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.impl.cassandra;

import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class DataAccess {
    private PreparedStatement findMCGroupData;
    private PreparedStatement findMCGroupDataByMetricName;
    private PreparedStatement findMCGroupDataByMetricNameAndTime1;
    private PreparedStatement findMCGroupDataByMetricNameAndTime2;
    private PreparedStatement findMCGroupDataByMetricNameAndTimeAndGroup;

    private PreparedStatement findMCCountryGroupData;
    private PreparedStatement findMCCountryGroupDataByGroup;
    private PreparedStatement findMCCountryGroupDataByGroupAndTime1;
    private PreparedStatement findMCCountryGroupDataByGroupAndTime2;
    
    private PreparedStatement findTopGroupDataByMetricName;
    private PreparedStatement findTopGroupDataByMetricNameAndTime1;
    private PreparedStatement findTopGroupDataByMetricNameAndTime2;
    
    private PreparedStatement findCountersByMetricGroup;

    private Session session;

    public DataAccess(Session session) {
        this.session = session;
        initPreparedStatements();
    }

    private void initPreparedStatements() {
        findMCGroupData = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_groupmetric");
        findMCGroupDataByMetricName = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_groupmetric WHERE metricname = ?");
        findMCGroupDataByMetricNameAndTime1 = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_groupmetric WHERE metricname = ? AND metrictime >= ? AND metrictime < ?");
        findMCGroupDataByMetricNameAndTime2 = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_groupmetric WHERE metricname = ? AND metrictime = ?");
        findMCGroupDataByMetricNameAndTimeAndGroup = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_groupmetric WHERE metricname = ? AND metrictime = ? AND groupid = ?");
        
        findMCCountryGroupData = session
                .prepare("SELECT metricname, groupid, metrictime, value, tag_value FROM mc_countrygroupmetric");
        findMCCountryGroupDataByGroup = session
                .prepare("SELECT metricname, groupid, metrictime, value, tag_value FROM mc_countrygroupmetric WHERE metricname = ? AND groupid = ?");
        findMCCountryGroupDataByGroupAndTime1 = session
                .prepare("SELECT metricname, groupid, metrictime, value, tag_value FROM mc_countrygroupmetric WHERE metricname = ? AND groupid = ? AND metrictime >= ? AND metrictime < ?");
        findMCCountryGroupDataByGroupAndTime2 = session
                .prepare("SELECT metricname, groupid, metrictime, value, tag_value FROM mc_countrygroupmetric WHERE metricname = ? AND groupid = ? AND metrictime = ?");

        findTopGroupDataByMetricName = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_topgroupmetric WHERE metricname = ? order by metrictime ASC, value DESC limit ? ");
        findTopGroupDataByMetricNameAndTime1 = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_topgroupmetric WHERE metricname = ? AND metrictime >= ? AND metrictime < ? order by metrictime ASC, value DESC limit ?");
        findTopGroupDataByMetricNameAndTime2 = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_topgroupmetric WHERE metricname = ? AND metrictime = ? order by metrictime ASC, value DESC limit ?");
   
        findCountersByMetricGroup = session
                .prepare("SELECT metricname, groupid, metrictime, value FROM mc_mctimegroupmetric WHERE metricname = ? and groupid = ?");
    }

    public ResultSetFuture findData(QueryParam queryParam) {
        if (queryParam.getQueryColumnFamilyName() == null)
            throw new IllegalArgumentException("Query table can not be null!");
        
        Map<String, Object> bindParams = queryParam.getParameters();
        BoundStatement statement  = null;
        
        if (queryParam.getQueryColumnFamilyName().toLowerCase()
                .equals("mc_groupmetric")){
            if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.METRIC_TIME) != null
                    && bindParams.get(QueryParam.GROUP_ID) != null) {
                statement = findMCGroupDataByMetricNameAndTimeAndGroup.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.METRIC_TIME),
                        bindParams.get(QueryParam.GROUP_ID));
            } else if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.START_TIME) != null
                    && bindParams.get(QueryParam.END_TIME) != null) {
                statement = findMCGroupDataByMetricNameAndTime1.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.START_TIME),
                        bindParams.get(QueryParam.END_TIME));
            } else if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.METRIC_TIME) != null) {
                statement = findMCGroupDataByMetricNameAndTime2.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.METRIC_TIME));
            } else if (bindParams.get(QueryParam.METRIC_NAME) != null){
                statement = findMCGroupDataByMetricName.bind(bindParams.get(QueryParam.METRIC_NAME));
            } else {
                statement = findMCGroupData.bind();
            }
        }else if(queryParam.getQueryColumnFamilyName().toLowerCase()
                .equals("mc_countrygroupmetric")){
            if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.GROUP_ID) != null
                    && bindParams.get(QueryParam.START_TIME) != null
                    && bindParams.get(QueryParam.END_TIME) != null) {
                statement = findMCCountryGroupDataByGroupAndTime1.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.GROUP_ID),
                        bindParams.get(QueryParam.START_TIME),
                        bindParams.get(QueryParam.END_TIME));
            } else if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.GROUP_ID) != null
                    && bindParams.get(QueryParam.METRIC_TIME) != null) {
                statement = findMCCountryGroupDataByGroupAndTime2.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.GROUP_ID),
                        bindParams.get(QueryParam.METRIC_TIME));
            } else if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.GROUP_ID) != null) {
                statement = findMCCountryGroupDataByGroup.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.GROUP_ID));
            } else {
                statement = findMCCountryGroupData.bind();
            }
        }else if(queryParam.getQueryColumnFamilyName().toLowerCase()
                .equals("mc_topgroupmetric")){
            if(bindParams.get(QueryParam.TOPN) == null){
                //Default to top 10 query
                queryParam.topN(10);
                bindParams = queryParam.getParameters();
            }
            
            if (bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.START_TIME) != null
                    && bindParams.get(QueryParam.END_TIME) != null){
                statement = findTopGroupDataByMetricNameAndTime1.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.START_TIME),
                        bindParams.get(QueryParam.END_TIME),
                        bindParams.get(QueryParam.TOPN));
            }else if(bindParams.get(QueryParam.METRIC_NAME) != null
                    && bindParams.get(QueryParam.METRIC_TIME) != null){
                statement = findTopGroupDataByMetricNameAndTime2.bind(
                    bindParams.get(QueryParam.METRIC_NAME),
                    bindParams.get(QueryParam.METRIC_TIME),
                    bindParams.get(QueryParam.TOPN));
            }else if(bindParams.get(QueryParam.METRIC_NAME) != null){
                statement = findTopGroupDataByMetricName.bind(
                        bindParams.get(QueryParam.METRIC_NAME),
                        bindParams.get(QueryParam.TOPN));
            }
        }
        if(statement != null)
            return session.executeAsync(statement);
        else
            throw new IllegalArgumentException("Unsupported table : "
                    + queryParam.getQueryColumnFamilyName()
                    + ", please extend the DataAccess class!");
    }

    public ResultSetFuture findCounters(String metricName, String groupId) {
        if(metricName == null || groupId == null)
            throw new IllegalArgumentException("Counter table metricname and groupid can not be null!");
        BoundStatement statement = findCountersByMetricGroup
                .bind(metricName, groupId);
        return session.executeAsync(statement);
    }
}
