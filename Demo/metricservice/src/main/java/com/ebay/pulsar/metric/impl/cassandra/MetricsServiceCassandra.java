/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.impl.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.InitializingBean;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ebay.jetstream.common.ShutDownable;
import com.ebay.pulsar.metric.core.Counter;
import com.ebay.pulsar.metric.core.MetricsService;
import com.ebay.pulsar.metric.core.MetricsThreadFactory;
import com.ebay.pulsar.metric.core.RawMetricMapper;
import com.ebay.pulsar.metric.core.RawNumericMetric;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class MetricsServiceCassandra implements MetricsService, InitializingBean, ShutDownable {
	
    private static final String DEFAULT_KEYSPACE = "pulsar";
	private static final String KEY_SPACE = "keySpace";
	private static final String LOCALHOST = "127.0.0.1";
	private static final String CONTACT_POINTS = "contactPoints";
	
	private Map<String, String> configuration;
    private Optional<Session> session;
    private DataAccess dataAccess;
    
    private MapQueryResultSet mapQueryResultSet = new MapQueryResultSet();

    private Function<ResultSet, List<Counter>> mapCounters = new Function<ResultSet, List<Counter>>() {
        @Override
        public List<Counter> apply(ResultSet resultSet) {
            List<Counter> counters = new ArrayList<Counter>();
            for (Row row : resultSet) {
                try{
                    counters.add(new Counter(row.getString(0), row.getString(1), row.getDate(2).getTime(), row.getLong(3)));
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            }
            return counters;
        }
    };

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    private void startUp() {
        int port = 9042;
        String[] seeds;
        if (configuration.containsKey(CONTACT_POINTS)) {
            seeds = configuration.get(CONTACT_POINTS).split(",");
        } else {
            seeds = new String[] {LOCALHOST};
        }

        Cluster cluster = new Cluster.Builder()
            .addContactPoints(seeds)
            .withPort(port)
            .build();

        String keySpace = configuration.get(KEY_SPACE);
        if (keySpace == null || keySpace.isEmpty()) {
            keySpace=DEFAULT_KEYSPACE;
        }
        session = Optional.of(cluster.connect(keySpace));
        dataAccess = new DataAccess(session.get());
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String metric, String group) {
        ResultSetFuture future = dataAccess.findCounters(metric, group);
        return Futures.transform(future, mapCounters, metricsTasks);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(QueryParam queryParam) {
        ResultSetFuture future = dataAccess.findData(queryParam);
        return Futures.transform(future, mapQueryResultSet, metricsTasks);
    }

    private static class MapQueryResultSet implements Function<ResultSet, List<RawNumericMetric>> {
        RawMetricMapper mapper = new RawMetricMapper();
        @Override
        public List<RawNumericMetric> apply(ResultSet resultSet) {
            return mapper.map(resultSet);
        }
    }
    
    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        startUp();        
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void shutDown() {
        if(session.isPresent()) {
            Session s = session.get();
            s.close();
            s.getCluster().close();
        }
    }
}
