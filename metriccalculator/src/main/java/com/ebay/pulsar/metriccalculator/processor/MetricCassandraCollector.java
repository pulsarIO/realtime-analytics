/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.pulsar.metriccalculator.cassandra.CassandraConfig;
import com.ebay.pulsar.metriccalculator.metric.MCMetricGroupDemension;
import com.ebay.pulsar.metriccalculator.util.MCConstant;
import com.ebay.pulsar.metriccalculator.util.MCCounterHelper;
import com.ebay.pulsar.metriccalculator.util.NamedThreadFactory;

@ManagedResource(objectName = "Event/Processor", description = "MC Collector for reporting to Cassandra")
public class MetricCassandraCollector extends AbstractEventProcessor implements
        XSerializable, BeanChangeAware {

    private static final String COUNTER_TABLE = MCCounterHelper.COUNTERGROUPBYMETICTIMEEVENT;
    private static final Logger LOGGER = LoggerFactory
            .getLogger("com.ebay.pulsar.metriccalculator.processor.MetricCassandraCollector");

    private static final Long ONE_MINUTE = 60 * 1000L;
    private static final int TTL = 86400;

    private static final String LASTEVENTINBATCH = "LastEventInBatch";

    private CassandraConfig config;
    private Advice m_adviceListener = null;

    private Session cassandraSession;
    private Metrics cassandraMetrics;

    private Map<String, String> metricColumnFamily = new ConcurrentHashMap<String, String>();
    private Map<String, PreparedStatement> stmtMap = new ConcurrentHashMap<String, PreparedStatement>();
    private Map<String, PreparedStatement> updatestmtMap = new ConcurrentHashMap<String, PreparedStatement>();

    private JetstreamEvent m_lastEvent;

    private List<MetricCounter> meticCounters = new LinkedList<MetricCounter>();

    protected final AtomicLong pendingRequestCounter = new AtomicLong(0);

    private ExecutorService pool;
    private ExecutorService worker;
    private ScheduledExecutorService timer;
    private BlockingQueue<Runnable> workQueue;

    private final LongCounter totalCassandraBatchInsert = new LongCounter();
    private final LongEWMACounter cassandraBatchInsertPerSec = new LongEWMACounter(
            60, MessageServiceTimer.sInstance().getTimer());

    private final LongCounter totalCassandraBatchUpdate = new LongCounter();
    private final LongEWMACounter cassandraBatchUpdatePerSec = new LongEWMACounter(
            60, MessageServiceTimer.sInstance().getTimer());

    private final LongCounter ePLBatchCount = new LongCounter();
    private final LongCounter cassandraErrorCount = new LongCounter();
    private final LongCounter eventSentToAdviceListener = new LongCounter();
    private final LongCounter totalCassandraInsertRequest = new LongCounter();

    private final LongCounter totalCassandraUpdateRequest = new LongCounter();

    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);

    private int counterFactor = 1;
    private volatile String keySpace;
    private volatile List<String> contactPoints = new ArrayList<String>();

    private AtomicBoolean connected = new AtomicBoolean(false);

    private int m_workingQueueSize = 100000;
    // Make it fix to 1
    private int m_workerThreadSize = 1;

    public MetricCassandraCollector(CassandraConfig config) {
        this.config = config;
    }

    @Hidden
    public CassandraConfig getConfig() {
        return config;
    }

    public void setConfig(CassandraConfig config) {
        this.config = config;
    }

    @Hidden
    public Advice getAdviceListener() {
        return m_adviceListener;
    }

    public void setAdviceListener(Advice adviceListener) {
        this.m_adviceListener = adviceListener;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
        if (config.getEnableCassandra()) {
            connect();
        }
        init();
    }

    @ManagedOperation
    public void connect() {
        if (!connected.get()) {
            pool = Executors.newSingleThreadExecutor(new NamedThreadFactory(
                    "CassandraCallBack"));
            keySpace = config.getKeySpace();
            contactPoints = new ArrayList<String>();
            contactPoints.addAll(config.getContactPoints());
            connectInternal();
        }
    }

    private void connectInternal() {
        try {
            Cluster cluster = config.createBuilder().build();
            cassandraSession = cluster.connect(keySpace);
            cassandraMetrics = cluster.getMetrics();
            connected.set(true);
        } catch (Exception e) {
            LOGGER.error("Error connection to Cassandra" + e.getMessage());
            if (pool != null) {
                pool.shutdownNow();
                pool = null;
            }
            if (cassandraSession != null) {
                cassandraSession.close();
                if (cassandraSession.getCluster() != null)
                    cassandraSession.getCluster().close();
            }
            connected.set(false);
        }
    }

    private void init() {
        workQueue = new LinkedBlockingQueue<Runnable>(m_workingQueueSize);
        worker = new ThreadPoolExecutor(m_workerThreadSize, m_workerThreadSize,
                30, TimeUnit.SECONDS, workQueue, new NamedThreadFactory(
                        "CassandraRequestWorker"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        timer = MCScheduler.getMCScheduler();
        timer.scheduleWithFixedDelay(new CassandraChecker(), ONE_MINUTE,
                ONE_MINUTE, TimeUnit.MILLISECONDS);
    }

    private void prepareStatements(String columnFamilyName,
            Map<String, String> demensions) {
        StringBuilder stmtStr = new StringBuilder();
        stmtStr.append("INSERT INTO ");
        stmtStr.append(columnFamilyName);
        stmtStr.append(" (metricname, groupid, metrictime");

        int demensionSize = 0;
        if (demensions != null) {
            for (Map.Entry<String, String> entry : demensions.entrySet()) {
                stmtStr.append(",");
                stmtStr.append(entry.getKey());
                demensionSize++;
            }
        }
        stmtStr.append(", value) VALUES (?, ?, ?");
        for (int i = 0; i < demensionSize; i++) {
            stmtStr.append(", ?");
        }

        stmtStr.append(", ?) USING TTL ");
        stmtStr.append(TTL);
        
        PreparedStatement stmt = cassandraSession.prepare(stmtStr.toString());
        stmtMap.put(columnFamilyName, stmt);
    }

    private void prepareStatementsForUpdate(String columnFamilyName,
            Map<String, String> demensions) {
        StringBuilder stmtStr = new StringBuilder();
        stmtStr.append("update ");
        stmtStr.append(columnFamilyName);
        stmtStr.append(" set value = value + ? ");
        stmtStr.append(" where metricname = ? and groupid = ? and metrictime = ? ");

        if (demensions != null) {
            for (String demensionName : demensions.keySet()) {
                stmtStr.append(" and ");
                stmtStr.append(demensionName.toLowerCase());
                stmtStr.append(" = ? ");
            }
        }
        PreparedStatement stmt = cassandraSession.prepare(stmtStr.toString());
        updatestmtMap.put(columnFamilyName, stmt);
    }

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        if (isPaused() || shutdownFlag.get()) {
            super.incrementEventDroppedCounter();
            return;
        }
        if (MCCounterHelper.isGroupByCounterEvent(event)) {
            if (!connected.get()) {
                if (config.getEnableCassandra()) {
                    if (event.get(JetstreamReservedKeys.MessageAffinityKey
                            .toString()) == null) {
                        event.put(JetstreamReservedKeys.MessageAffinityKey
                                .toString(), (String) event
                                .get(MCConstant.METRIC_NAME));
                    }
                    getAdviceListener().retry(event, RetryEventCode.MSG_RETRY,
                            "Cassandra Down");
                    eventSentToAdviceListener.increment();
                } else {
                    super.incrementEventDroppedCounter();
                }
                return;
            }
            incrementEventRecievedCounter();
            m_lastEvent = event;
            publishAsync(event);
        }
    }

    private void publishAsync(final JetstreamEvent event) {
        if (config.getEnableCassandra()) {
            worker.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String metricName = (String) event
                                .get(MCConstant.METRIC_NAME);
                        String columnFamilyName = event.getEventType()
                                .toLowerCase();

                        String groupId = null;
                        Map<String, String> tags = null;
                        if (event.get(MCConstant.METRIC_DEMENSION) != null) {
                            MCMetricGroupDemension groupDemension = (MCMetricGroupDemension) event
                                    .get(MCConstant.METRIC_DEMENSION);
                            groupId = groupDemension.getGroupId();

                            if (event.get(MCConstant.TAG_TIME_IGNORE) != null) {
                                tags = new HashMap<String, String>(
                                        groupDemension.getDimensions());
                                tags.put(
                                        MCConstant.TAG_TIME_IGNORE,
                                        (String) event
                                                .get(MCConstant.TAG_TIME_IGNORE));
                            } else {
                                tags = groupDemension.getDimensions();
                            }
                        }

                        if (groupId == null) {
                            if (event.get(MCConstant.GROUP_ID) != null) {
                                groupId = (String) event
                                        .get(MCConstant.GROUP_ID);
                            } else {
                                groupId = metricName;
                            }
                        }
                        long count = 0;
                        if (event.get(MCConstant.AGGREGATED_COUNT) != null) {
                            count = (Long) event
                                    .get(MCConstant.AGGREGATED_COUNT);
                        } else {
                            count = (Long) event.get(MCConstant.METRIC_COUNT);
                        }
                        long metricTime = (Long) event
                                .get(MCConstant.METRIC_TIME);

                        if (event.get(MCConstant.SHUTDOWN_FLUSH) == null
                                && (event.get(MCConstant.FIRST_FLUSH) == null)) {
                            metricTime = (metricTime / 60000) * 60000;
                        }

                        boolean counterTable = columnFamilyName
                                .contains(COUNTER_TABLE);
                        if (counterTable) {
                            // To let the old table work
                            if (tags == null) {
                                tags = new HashMap<String, String>();
                                tags.put(MCCounterHelper.TAG_METRICTIME, String
                                        .valueOf(event
                                                .get(MCConstant.METRIC_TIME)));
                            }
                            Calendar c = Calendar.getInstance();
                            c.setTimeInMillis(metricTime);
                        }
                        // Register the metricColumnFamily and also the prepared
                        // statement.
                        if (metricColumnFamily.get(metricName) == null
                                || !metricColumnFamily.get(metricName).equals(
                                        columnFamilyName)) {
                            if (counterTable) {
                                if (updatestmtMap.get(columnFamilyName) == null) {
                                    if (tags != null && tags.size() > 0) {
                                        prepareStatementsForUpdate(
                                                columnFamilyName, tags);
                                    } else {
                                        prepareStatementsForUpdate(
                                                columnFamilyName, null);
                                    }
                                }
                            } else {
                                if (stmtMap.get(columnFamilyName) == null) {
                                    if (tags != null && tags.size() > 0) {
                                        prepareStatements(columnFamilyName,
                                                tags);
                                    } else {
                                        prepareStatements(columnFamilyName,
                                                null);
                                    }
                                }
                            }
                            metricColumnFamily
                                    .put(metricName, columnFamilyName);
                        }

                        // Enable batch
                        if (config.getBatchSize() > 1) {
                            MetricCounter metric = new MetricCounter(
                                    metricName, groupId, tags, count,
                                    metricTime);
                            meticCounters.add(metric);
                            int counterSize = meticCounters.size();
                            if (counterSize >= config.getBatchSize()) {
                                publishToCassandraInBatch();
                            } else if (event.get(LASTEVENTINBATCH) != null) {
                                ePLBatchCount.increment();
                                publishToCassandraInBatch();
                            }
                        }
                        // Disable batch
                        else {
                            if (event.get(LASTEVENTINBATCH) != null) {
                                ePLBatchCount.increment();
                            }

                            int parameterSize = 4;
                            if (tags != null) {
                                parameterSize = 4 + tags.size();
                            }
                            Object[] paramterValues = new Object[parameterSize];
                            if (counterTable) {
                                paramterValues[0] = count;
                                paramterValues[1] = metricName;
                                paramterValues[2] = groupId;
                                paramterValues[3] = new Date(metricTime);
                                int i = 1;
                                if (tags != null) {
                                    for (Map.Entry<String, String> entry : tags
                                            .entrySet()) {
                                        paramterValues[3 + i] = entry
                                                .getValue();
                                        i++;
                                    }
                                }
                            } else {
                                paramterValues[0] = metricName;
                                paramterValues[1] = groupId;
                                paramterValues[2] = new Date(metricTime);
                                int i = 1;
                                if (tags != null) {
                                    for (Map.Entry<String, String> entry : tags
                                            .entrySet()) {
                                        paramterValues[2 + i] = entry
                                                .getValue();
                                        i++;
                                    }
                                }
                                paramterValues[parameterSize - 1] = Long
                                        .valueOf(count).intValue();
                            }
                            publishToCassandra(paramterValues, event);
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Error publising metrics in MetricCassandraCollector:" + ex.getMessage());
                        cassandraErrorCount.increment();
                        if (event.get(JetstreamReservedKeys.MessageAffinityKey
                                .toString()) == null) {
                            event.put(JetstreamReservedKeys.MessageAffinityKey
                                    .toString(), (String) event
                                    .get(MCConstant.METRIC_NAME));
                        }
                        getAdviceListener().retry(event,
                                RetryEventCode.MSG_RETRY, ex.getMessage());
                        eventSentToAdviceListener.increment();
                        registerError(ex);
                    }
                }
            });
        }
    }

    private void publishToCassandraInBatch() {
        // String timeSlice = dateFormater.format(timestamp);
        int round = counterFactor;
        while (round > 0) {
            List<Insert> insertRequest = new ArrayList<Insert>();
            List<Update> updateRequest = new ArrayList<Update>();
            for (MetricCounter entry : meticCounters) {
                String metricName = entry.getMetricName();
                String columnFamilyName = metricColumnFamily.get(metricName);
                if (round != 1) {
                    metricName = metricName + round;
                }
                if (columnFamilyName.contains(COUNTER_TABLE)) {
                    Update update = QueryBuilder.update(columnFamilyName);
                    update.with(QueryBuilder.incr("value", entry.getCount()));
                    update.where(QueryBuilder.eq("metricname", metricName))
                            .and(QueryBuilder.eq("groupid", entry.getGroupId()))
                            .and(QueryBuilder.eq("metrictime",
                                    entry.getMetricTime()));

                    Map<String, String> tags = entry.getDemensions();
                    for (Map.Entry<String, String> entryInTag : tags.entrySet()) {
                        update.where(QueryBuilder.eq(entryInTag.getKey(),
                                entryInTag.getValue()));
                    }
                    updateRequest.add(update);
                } else {
                    Insert insert = QueryBuilder.insertInto(columnFamilyName)
                            .value("metricname", metricName)
                            .value("groupid", entry.getGroupId())
                            .value("metrictime", entry.getMetricTime());
                    Map<String, String> tags = entry.getDemensions();
                    for (Map.Entry<String, String> entryInTag : tags.entrySet()) {
                        insert.value(entryInTag.getKey(), entryInTag.getValue());
                    }
                    insert.value("value", entry.getCount());
                    insert.using(QueryBuilder.ttl(TTL));
                    insertRequest.add(insert);
                }
            }
            if (!insertRequest.isEmpty()) {
                runBatchInsert(insertRequest);
            }
            if (!updateRequest.isEmpty()) {
                runBatchUpdate(updateRequest);
            }
            round--;
        }
        meticCounters.clear();
    }

    private void publishToCassandra(Object[] paramterValues,
            JetstreamEvent event) {
        int round = counterFactor;
        String metricName = null;
        if (paramterValues[0] instanceof String) {
            metricName = (String) paramterValues[0];
        } else {
            metricName = (String) paramterValues[1];
        }

        String columnFamilyName = metricColumnFamily.get(metricName);
        PreparedStatement stmt = null;

        boolean counterTable = columnFamilyName.contains(COUNTER_TABLE);
        if (counterTable) {
            stmt = updatestmtMap.get(columnFamilyName);
        } else {
            stmt = stmtMap.get(columnFamilyName);
        }
        if (stmt == null) {
            // Unregistered statement
            return;
        }
        while (round > 0) {
            if (round != 1) {
                if (counterTable) {
                    paramterValues[1] = metricName + round;
                } else {
                    paramterValues[0] = metricName + round;
                }
            } else {
                if (counterTable) {
                    paramterValues[1] = metricName;
                } else {
                    paramterValues[0] = metricName;
                }
            }

            BoundStatement q = new BoundStatement(stmt).bind(paramterValues);
            execute(q, event);
            if (counterTable) {
                totalCassandraUpdateRequest.increment();
            } else {
                totalCassandraInsertRequest.increment();
            }
            // Throttling the Cassandra insertion/Updating
            if (config.getSleepTime() > 0) {
                if ((totalCassandraUpdateRequest.get() + totalCassandraInsertRequest
                        .get()) % config.getThrottlingCount() == 0) {
                    try {
                        Thread.sleep(config.getSleepTime());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                }
            }
            round--;
        }
    }

    private void execute(BoundStatement q, JetstreamEvent event) {
        try {
            ResultSetFuture future = cassandraSession.executeAsync(q);
            CallBackListener listener = new CallBackListener(future, event);
            future.addListener(listener, pool);
            pendingRequestCounter.incrementAndGet();
        } catch (Throwable ex) {
            LOGGER.error(
                    "Error publising metrics in MetricCassandraCollector:"
                            + ex.getMessage());
            cassandraErrorCount.increment();
            if (event.get(JetstreamReservedKeys.MessageAffinityKey.toString()) == null) {
                event.put(JetstreamReservedKeys.MessageAffinityKey.toString(),
                        (String) event.get(MCConstant.METRIC_NAME));
            }
            getAdviceListener().retry(event, RetryEventCode.MSG_RETRY,
                    ex.getMessage());
            eventSentToAdviceListener.increment();
            registerError(ex);
        }
    }

    private void runBatchInsert(List<Insert> insertRequest) {
        try {
            Batch batch;
            if (config.getLoggedBatch()) {
                batch = QueryBuilder.batch(insertRequest
                        .toArray(new RegularStatement[insertRequest.size()]));
            } else {
                batch = QueryBuilder.unloggedBatch(insertRequest
                        .toArray(new RegularStatement[insertRequest.size()]));
            }
            totalCassandraInsertRequest.addAndGet(insertRequest.size());
            ResultSetFuture future = cassandraSession.executeAsync(batch);
            CallBackListener listener = new CallBackListener(future, null);
            future.addListener(listener, pool);
            incrementBatchInsertCounter();
            pendingRequestCounter.incrementAndGet();
        } catch (Throwable ex) {
            LOGGER.error("Error publising metrics in MetricCassandraCollector:" + ex.getMessage());
            cassandraErrorCount.increment();
            registerError(ex);
        } finally {
            insertRequest.clear();
        }
    }

    private void runBatchUpdate(List<Update> updateRequest) {
        try {
            Batch batch;

            if (config.getLoggedBatch()) {
                batch = QueryBuilder.batch(updateRequest
                        .toArray(new RegularStatement[updateRequest.size()]));
            } else {
                batch = QueryBuilder.unloggedBatch(updateRequest
                        .toArray(new RegularStatement[updateRequest.size()]));
            }
            totalCassandraUpdateRequest.addAndGet(updateRequest.size());
            ResultSetFuture future = cassandraSession.executeAsync(batch);
            CallBackListener listener = new CallBackListener(future, null);
            future.addListener(listener, pool);
            incrementBatchUpdateCounter();
            pendingRequestCounter.incrementAndGet();
        } catch (Throwable ex) {
            LOGGER.error("Error publising metrics in MetricCassandraCollector:" + ex.getMessage());
            cassandraErrorCount.increment();
            registerError(ex);
        } finally {
            updateRequest.clear();
        }
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void shutDown() {
        if (shutdownFlag.compareAndSet(false, true)) {
            LOGGER.warn("Start gracefully stop MetricCassandraCollector!");
            if (worker != null) {
                worker.shutdown();
                try {
                    worker.awaitTermination(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // Ignore
                }
                worker = null;
            }
            disconnect();
            Management.removeBeanOrFolder(getBeanName(), this);
            LOGGER.warn("Gracefully stop MetricCassandraCollector!");
        }
    }

    @ManagedOperation
    public void disconnect() {
        if (connected.get()) {
            pool.shutdown();
            try {
                pool.awaitTermination(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            pool = null;
            cassandraSession.close();
            cassandraSession.getCluster().close();
            metricColumnFamily.clear();
            stmtMap.clear();
            updatestmtMap.clear();
            connected.set(false);
        }
    }

    @ManagedOperation
    @Override
    public void pause() {
        if (isPaused()) {
            LOGGER.warn(getBeanName() + " could not be paused. It is already in paused state");
            return;
        }
        changeState(ProcessorOperationState.PAUSE);
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {
            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
            // Apply changes
            if (bcInfo.getBeanName().equals(getConfig().getBeanName())) {
                LOGGER.info("Received change bean:  - "
                        + event.getClass().getName());
                LOGGER.info("Received new configuration for  - "
                        + bcInfo.getBeanName());
                try {
                    setConfig(((CassandraConfig) bcInfo.getChangedBean()));
                } catch (Exception e) {
                    LOGGER.warn("Error while applying config to CassandraConfig - "
                                    + e.getMessage());
                }
                if ((!getConfig().getKeySpace().equals(this.keySpace))
                        || (getConfig().getContactPoints() != this.contactPoints)) {
                    if (config.getEnableCassandra()) {
                        disconnect();
                        connect();
                    }
                }
            }
        }
    }

    @ManagedOperation
    @Override
    public void resume() {
        if (!isPaused()) {
            LOGGER.warn(getBeanName() + " could not be resumed. It is already in resume state");
            return;
        }
        changeState(ProcessorOperationState.RESUME);
    }

    public double getFifteenMinuteRate() {
        if (cassandraMetrics == null) {
            return 0;
        }
        return cassandraMetrics.getRequestsTimer().getFifteenMinuteRate();
    }

    public double getFiveMinuteRate() {
        if (cassandraMetrics == null) {
            return 0;
        }
        return cassandraMetrics.getRequestsTimer().getFiveMinuteRate();
    }

    public JetstreamEvent getLastEvent() {
        return m_lastEvent;
    }

    public int getCounterFactor() {
        return counterFactor;
    }

    @ManagedAttribute
    public void setCounterFactor(int counterFactor) {
        this.counterFactor = counterFactor;
    }

    protected void incrementBatchInsertCounter() {
        totalCassandraBatchInsert.increment();
        cassandraBatchInsertPerSec.increment();
    }

    protected void incrementBatchUpdateCounter() {
        totalCassandraBatchUpdate.increment();
        cassandraBatchUpdatePerSec.increment();
    }

    public long getPendingRequestCounter() {
        return pendingRequestCounter.get();
    }

    public long getTotalCassandraBatchInsert() {
        return totalCassandraBatchInsert.get();
    }

    public long getCassandraBatchInsertPerSec() {
        return cassandraBatchInsertPerSec.get();
    }

    public long getTotalCassandraInsertRequest() {
        return totalCassandraInsertRequest.get();
    }

    public long getTotalCassandraBatchUpdate() {
        return totalCassandraBatchUpdate.get();
    }

    public long getCassandraBatchUpdatePerSec() {
        return cassandraBatchUpdatePerSec.get();
    }

    public long getTotalCassandraUpdateRequest() {
        return totalCassandraUpdateRequest.get();
    }

    public long getePLBatchCount() {
        return ePLBatchCount.get();
    }

    public long getCassandraErrorCount() {
        return cassandraErrorCount.get();
    }

    public long getEventSentToAdviceListener() {
        return eventSentToAdviceListener.get();
    }

    public long getCassandraInsertionAvgRate() {
        if (ePLBatchCount.get() == 0)
            return 0;
        else
            return totalCassandraInsertRequest.get() / (ePLBatchCount.get());
    }

    public String getMetricColumnFamily() {
        return metricColumnFamily.toString();
    }

    public String getConnectStr() {
        return contactPoints.toString();
    }

    public String getKeySpace() {
        return keySpace;
    }

    @ManagedOperation
    public void disableCassandra() {
        this.config.setEnableCassandra(false);
        disconnect();
    }

    @ManagedOperation
    public void enableCassandra() {
        this.config.setEnableCassandra(true);
        connect();
    }

    public int getWorkingQueueSize() {
        return m_workingQueueSize;
    }

    public void setWorkingQueueSize(int m_workingQueueSize) {
        this.m_workingQueueSize = m_workingQueueSize;
    }

    public int getWokerThreadSize() {
        return m_workerThreadSize;
    }

    public int getCassandraQueueSize() {
        return workQueue.size();
    }

    private final class CallBackListener implements Runnable {
        private final ResultSetFuture future;
        private final JetstreamEvent event;

        public CallBackListener(ResultSetFuture future, JetstreamEvent event) {
            this.future = future;
            this.event = event;
        }

        @Override
        public void run() {
            pendingRequestCounter.decrementAndGet();
            try {
                future.getUninterruptibly();
            } catch (DriverException e) {
                cassandraErrorCount.increment();
                if (event != null) {
                    if (event.get(JetstreamReservedKeys.MessageAffinityKey
                            .toString()) == null) {
                        event.put(JetstreamReservedKeys.MessageAffinityKey
                                .toString(), (String) event
                                .get(MCConstant.METRIC_NAME));
                    }
                    getAdviceListener().retry(event, RetryEventCode.MSG_RETRY,
                            e.getMessage());
                    eventSentToAdviceListener.increment();
                }
                registerError(e);
            }
        }
    }

    private static class MetricCounter {
        private String metricName;
        private String groupId;
        private SortedMap<String, String> demensions = new TreeMap<String, String>();
        private long count;
        private long metricTime;

        public MetricCounter(String metricName, String groupId,
                Map<String, String> demensions, long count, long metricTime) {
            super();
            this.metricName = metricName;
            this.groupId = groupId;
            if (demensions != null) {
                this.demensions.putAll(demensions);
            }
            this.count = count;
            this.metricTime = metricTime;
        }

        public String getMetricName() {
            return metricName;
        }

        public String getGroupId() {
            return groupId;
        }

        public Map<String, String> getDemensions() {
            return demensions;
        }

        public long getCount() {
            return count;
        }

        public long getMetricTime() {
            return metricTime;
        }
    }

    private class CassandraChecker implements Runnable {
        @Override
        public void run() {
            if (config.getEnableCassandra()) {
                connect();
            }
        }
    }
}
