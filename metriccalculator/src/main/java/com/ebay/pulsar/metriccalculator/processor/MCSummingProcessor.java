/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.pulsar.metriccalculator.cache.CacheManager;
import com.ebay.pulsar.metriccalculator.cache.OffHeapCacheConfig;
import com.ebay.pulsar.metriccalculator.cache.OffHeapMemoryManagerRegistry;
import com.ebay.pulsar.metriccalculator.metric.MCMetricGroupDemension;
import com.ebay.pulsar.metriccalculator.metric.MetricFrequency;
import com.ebay.pulsar.metriccalculator.processor.configuration.MCSummingConfiguration;
import com.ebay.pulsar.metriccalculator.statistics.basic.AvgCounter;
import com.ebay.pulsar.metriccalculator.statistics.basic.Counter;
import com.ebay.pulsar.metriccalculator.util.MCConstant;
import com.ebay.pulsar.metriccalculator.util.MCCounterHelper;

@ManagedResource(objectName = "Event/Processor", description = "MC Counter  for reporting to Cassandra and EVPS")
public class MCSummingProcessor extends AbstractEventProcessor implements
        XSerializable, MCMetricsProvider, BeanChangeAware {
    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.pulsar.metriccalculator.processor.MCSummingProcessor");

    private static final String CASSANDRA_SEND_COUNT = "cassandraMetricCount";
    private static final String EVPS_SEND_COUNT = "evpsMetricCount";
    private static final String MC_SUMMING_PROCESSOR_TIMER = "MCSummingProcessorTimer";

    private final Map<String, Counter> normalMetrics = new HashMap<String, Counter>();
    private final Map<String, Map<MCMetricGroupDemension, Counter>> groupbyWithTagsMetricMap = new HashMap<String, Map<MCMetricGroupDemension, Counter>>();
    private MCSummingConfiguration configuration;

    private long maxMetricCollectionTime;
    private long lastMetricCollectionTime;

    private Timer timer = new Timer(MC_SUMMING_PROCESSOR_TIMER);
    private TimerTask summingTimingTask;
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);

    private final LongCounter mapClearCount = new LongCounter();
    private final LongCounter normalMetricCount = new LongCounter();
    private LongCounter groupByMetricCount = new LongCounter();

    private LongCounter rawEventCount = new LongCounter();
    private LongCounter totalMetricCollectionCount = new LongCounter();

    private long timerIterationCounter = 0;
    private long timerLoopSize = 0;

    private long maxDimensionSize = 0;
    private long latestDimensionSize = 0;

    private JetstreamEvent lastRawEvent;

    private Map<Long, Set<String>> frequencyMetrics = new ConcurrentHashMap<Long, Set<String>>();
    private Map<String, Long> metricFrequencies = new ConcurrentHashMap<String, Long>();
    private Map<String, String> metricTables = new ConcurrentHashMap<String, String>();
    private Map<String, LongCounter> metricCollectionCounts = new ConcurrentHashMap<String, LongCounter>();

    private static final MetricFrequency DEFAULT_FREQUENCY = MetricFrequency.ONE_MINUTE;
    private static final long TIMER_FREQUENCY;

    static {
        long timerFrequency = DEFAULT_FREQUENCY.getValue();
        for (MetricFrequency freq : MetricFrequency.values()) {
            timerFrequency = gcf(timerFrequency, freq.getValue());
        }
        TIMER_FREQUENCY = timerFrequency;
    }

    public MCSummingProcessor() {
        normalMetrics.put(EVPS_SEND_COUNT, new Counter());
        normalMetrics.put(CASSANDRA_SEND_COUNT, new Counter());
    }

    public MCSummingConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(MCSummingConfiguration configuration) {
        this.configuration = configuration;
    }

    public void setMetricFrequency(String metricName, MetricFrequency frequency) {
        if (metricName != null && frequency != null) {
            setMetricFrequency(metricName, frequency.getValue());
        }
    }

    public void setMetricFrequencyInMin(String metricName, int min) {
        if (metricName != null && min > 0) {
            long frequency = min * MetricFrequency.ONE_MINUTE.getValue();
            setMetricFrequency(metricName, frequency);
        }
    }

    public void setMetricFrequencyMap(
            Map<String, MetricFrequency> metricFreqencyMap) {
        if (metricFreqencyMap != null && !metricFreqencyMap.isEmpty()) {
            for (Map.Entry<String, MetricFrequency> entry : metricFreqencyMap
                    .entrySet()) {
                setMetricFrequency(entry.getKey(), entry.getValue());
            }
        }
    }

    public void setMetricFrequencyInMin(Map<String, Integer> metricFreqencyMap) {
        if (metricFreqencyMap != null && !metricFreqencyMap.isEmpty()) {
            for (Map.Entry<String, Integer> entry : metricFreqencyMap
                    .entrySet()) {
                setMetricFrequencyInMin(entry.getKey(), entry.getValue());
            }
        }
    }

    private void setMetricFrequency(String metricName, long seconds) {
        metricFrequencies.put(metricName, seconds);
        Set<String> metricNameSet = frequencyMetrics.get(seconds);
        if (metricNameSet == null) {
            metricNameSet = new HashSet<String>();
            frequencyMetrics.put(seconds, metricNameSet);
        }
        metricNameSet.add(metricName);
    }

    @Override
    public void processApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {
            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
            // Apply changes
            if (bcInfo.getBeanName().equals(getConfiguration().getBeanName())) {
                LOGGER.info("Received change bean:  - " + event.getClass().getName());
                LOGGER.info("Received new configuration for  - "+ bcInfo.getBeanName());
                try {
                    setConfiguration(((MCSummingConfiguration) bcInfo
                            .getChangedBean()));
                } catch (Exception e) {
                    LOGGER.error("Error while applying config to MCSummingProcessor - " + e.getMessage());
                }
            }
        }
    }

    private void evaluateRawEvent(JetstreamEvent event) {
        rawEventCount.increment();
        lastRawEvent = event;
    }

    @Override
    public void collectMetrics(Set<String> avaiableCounterName,
            boolean flushNormal) {
        try {
            totalMetricCollectionCount.increment();
            long start = System.currentTimeMillis();
            // Collect normal counters
            if (flushNormal) {
                List<JetstreamEvent> events = createJetStreamCountEvent();
                for (JetstreamEvent event : events) {
                    super.fireSendEvent(event);
                    incrementEventSentCounter();
                    Object metricNameInEvent = event
                            .get(MCConstant.METRIC_NAME);
                    if ((!EVPS_SEND_COUNT.equals(metricNameInEvent))
                            && (!CASSANDRA_SEND_COUNT.equals(metricNameInEvent))) {
                        normalMetricCount.increment();
                        normalMetrics.get(EVPS_SEND_COUNT).inc();
                    }
                }
            }
            // Collect group by counters
            if (getConfiguration().isEnableGroupByCounter()) {
                for (String metricName : avaiableCounterName) {
                    List<JetstreamEvent> groupByEvents = createJetStreamGroupbyCountEventsWithTags(metricName);
                    for (JetstreamEvent event : groupByEvents) {
                        super.fireSendEvent(event);
                        incrementEventSentCounter();
                        groupByMetricCount.increment();
                        normalMetrics.get(CASSANDRA_SEND_COUNT).inc();
                    }
                }

                long end = System.currentTimeMillis();
                lastMetricCollectionTime = end - start;
                if (lastMetricCollectionTime > maxMetricCollectionTime) {
                    maxMetricCollectionTime = lastMetricCollectionTime;
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("Error collection metrics in MCSummingProcessor:"
                    + ex.getMessage());
            registerError(ex);
        }
    }

    private Counter getCounterByMetricName(String metricName, boolean isAvg) {
        Counter counter = normalMetrics.get(metricName);
        if (counter == null) {
            if (isAvg) {
                counter = new AvgCounter();
            } else {
                counter = new Counter();
            }
            normalMetrics.put(metricName, counter);
        }
        return counter;
    }

    private Counter getCounterByMetricDemensionAndInc(String metricName,
            String groupId, Map<String, String> tags, boolean isAvg,
            Long count, Long total) {
        Map<MCMetricGroupDemension, Counter> counters = groupbyWithTagsMetricMap
                .get(metricName);
        OffHeapCacheConfig conf = null;
        if (getConfiguration().getOffheapMetricConf() != null) {
            conf = getConfiguration().getOffheapMetricConf().get(metricName);
        }
        if (counters == null) {
            synchronized (this) {
                if (counters == null) {
                    if (conf != null) {
                        counters = CacheManager.getCounterOffHeapCache(
                                metricName, conf);
                    } else {
                        counters = CacheManager.getCounterCache();
                    }
                    groupbyWithTagsMetricMap.put(metricName, counters);
                }
            }
        }

        MCMetricGroupDemension groupDemension = null;
        String tag_time = null;
        if (tags == null || tags.isEmpty()) {
            groupDemension = new MCMetricGroupDemension(metricName, groupId);
        } else {
            if (tags.containsKey(MCConstant.TAG_TIME_IGNORE)) {
                tag_time = tags.remove(MCConstant.TAG_TIME_IGNORE);
            }
            groupDemension = new MCMetricGroupDemension(metricName, groupId,
                    tags);
        }

        Counter counter = counters.get(groupDemension);
        if (counter == null) {
            if (isAvg) {
                counter = new AvgCounter();
            } else {
                counter = new Counter();
            }
            if (conf == null) {
                // Store new counter to Map for heap map
                counters.put(groupDemension, counter);
            }
        }
        if (tag_time != null) {
            counter.setLastCounterTime(tag_time);
        }

        if (isAvg) {
            ((AvgCounter) counter).inc(count, total);
        } else {
            if (count != null) {
                counter.inc(count);
            } else {
                counter.inc();
            }
        }
        if (conf != null) {
            // Store counter back to off heap map
            counters.put(groupDemension, counter);
        }

        return counter;
    }

    private long getFrequencyByMetricName(String metricName) {
        Long frequency = metricFrequencies.get(metricName);
        if (frequency == null) {
            return -1;
        }
        return frequency.longValue();
    }

    private List<JetstreamEvent> createJetStreamCountEvent() {
        List<JetstreamEvent> result = new ArrayList<JetstreamEvent>(
                normalMetrics.size());
        for (Map.Entry<String, Counter> entry : normalMetrics.entrySet()) {
            String metricName = entry.getKey();
            Counter counter = entry.getValue();
            counter.mark();
            Map<String, Object> internalMap = new HashMap<String, Object>();
            if (counter instanceof AvgCounter) {
                internalMap.put(MCConstant.AGGREGATED_COUNT,
                        ((AvgCounter) counter).getLatestAvgValue());
            } else {
                internalMap.put(MCConstant.AGGREGATED_COUNT,
                        counter.getLastDeltaValue());
            }
            internalMap.put(MCConstant.METRIC_NAME, metricName);
            internalMap.put(MCConstant.METRIC_FREQUENCY,
                    getFrequencyByMetricName(metricName));
            if (shutdownFlag.get()) {
                internalMap.put(MCConstant.SHUTDOWN_FLUSH,
                        MCConstant.SHUTDOWN_FLUSH);
            }
            JetstreamEvent event = new JetstreamEvent(
                    MCCounterHelper.COUNTEREVENT, null, internalMap);
            result.add(event);
        }
        return result;
    }

    private List<JetstreamEvent> createJetStreamGroupbyCountEventsWithTags(
            String metricName) {
        Map<MCMetricGroupDemension, Counter> counterMap = groupbyWithTagsMetricMap
                .get(metricName);
        if (counterMap == null)
            return Collections.emptyList();

        OffHeapCacheConfig conf = null;
        if (getConfiguration().getOffheapMetricConf() != null) {
            conf = getConfiguration().getOffheapMetricConf().get(metricName);
        }

        Integer threshold = getConfiguration().getMetricsThreshold().get(
                metricName);
        int _threshold = 0;
        if (threshold != null) {
            _threshold = threshold.intValue();
        }

        boolean mapClear = false;
        // Create new off heap map for each round of metric collection
        if (conf != null) {
            synchronized (this) {
                Map<MCMetricGroupDemension, Counter> newCounterMap = CacheManager
                        .getCounterOffHeapCache(metricName, conf);
                groupbyWithTagsMetricMap.put(metricName, newCounterMap);
            }
            mapClear = true;
        } else if (conf == null
                && counterMap.size() > getConfiguration().getGroupCounterMax()) {
            synchronized (this) {
                Map<MCMetricGroupDemension, Counter> newCounterMap = CacheManager
                        .getCounterCache();
                groupbyWithTagsMetricMap.put(metricName, newCounterMap);
            }
            mapClear = true;
        }

        List<JetstreamEvent> result = new ArrayList<JetstreamEvent>(
                counterMap.size());
        long now = System.currentTimeMillis();
        for (Map.Entry<MCMetricGroupDemension, Counter> entry : counterMap
                .entrySet()) {
            MCMetricGroupDemension groupDemension = entry.getKey();
            Counter counter = entry.getValue();
            counter.mark();

            boolean timeBasedMetric = false;
            if (groupDemension.getDimensions() != null
                    && groupDemension.getDimensions().get(
                            MCCounterHelper.TAG_METRICTIME) != null) {
                timeBasedMetric = true;
            }

            // Only publish events with deltaValue > _threshold
            if (counter.getLastDeltaValue() > _threshold) {
                Map<String, Object> internalMap = new LinkedHashMap<String, Object>();
                if (timeBasedMetric) {
                    internalMap.put(
                            MCConstant.METRIC_TIME,
                            Long.valueOf(groupDemension.getDimensions().get(
                                    MCCounterHelper.TAG_METRICTIME)));
                } else {
                    internalMap.put(MCConstant.METRIC_TIME, now);
                }

                if (counter instanceof AvgCounter) {
                    internalMap.put(MCConstant.AGGREGATED_COUNT,
                            ((AvgCounter) counter).getLatestAvgValue());
                } else {
                    internalMap.put(MCConstant.AGGREGATED_COUNT,
                            counter.getLastDeltaValue());
                }
                internalMap.put(MCConstant.METRIC_NAME, metricName);
                internalMap.put(MCConstant.METRIC_FREQUENCY,
                        getFrequencyByMetricName(metricName));

                internalMap.put(MCConstant.METRIC_DEMENSION, groupDemension);

                if (counter.getLastCounterTime() != null) {
                    internalMap.put(MCConstant.TAG_TIME_IGNORE,
                            counter.getLastCounterTime());
                }

                if (shutdownFlag.get()) {
                    internalMap.put(MCConstant.SHUTDOWN_FLUSH,
                            MCConstant.SHUTDOWN_FLUSH);
                }
                if ((metricCollectionCounts.get(metricName) != null)
                        && metricCollectionCounts.get(metricName).get() == 1) {
                    internalMap.put(MCConstant.FIRST_FLUSH,
                            MCConstant.FIRST_FLUSH);
                }

                JetstreamEvent event = new JetstreamEvent(
                        metricTables.get(metricName), null, internalMap);
                result.add(event);
            }
            if (timeBasedMetric && counter.getLastDeltaValue() == 0) {
                // remove the time based metric in memory
                counterMap.remove(groupDemension);
            }
        }
        if (result.size() > 0) {
            JetstreamEvent lastEvent = result.get(result.size() - 1);
            lastEvent.put("LastEventInBatch", "true");
        }
        if (mapClear) {
            counterMap.clear();
            counterMap = null;
            mapClearCount.increment();
        }
        return result;
    }

    @Override
    // Received event from Esper processor
    public void sendEvent(JetstreamEvent event) throws EventException {
        if (isPaused() || shutdownFlag.get()) {
            super.incrementEventDroppedCounter();
            return;
        }

        incrementEventRecievedCounter();

        if (MCCounterHelper.isMCCounterEvent(event)) {
            String metricName = (String) event.get(MCConstant.METRIC_NAME);
            Long total = null;
            if (MCCounterHelper.isAvgEvent(metricName)) {
                for (Map.Entry<String, Object> entry : event.entrySet()) {
                    if (entry.getKey().toLowerCase().contains("total")) {
                        total = (Long) entry.getValue();
                        break;
                    }
                }
            }

            if (event.get(MCConstant.METRIC_COUNT) != null) {
                Long count = (Long) event.get(MCConstant.METRIC_COUNT);
                if (total != null) {
                    ((AvgCounter) getCounterByMetricName(metricName, true))
                            .inc(count, total);
                } else
                    getCounterByMetricName(metricName, false).inc(count);
            } else {
                getCounterByMetricName(metricName, false).inc();
            }
        } else if (MCCounterHelper.isMCMultiCounterEvent(event)) {
            // Does not support avg calculator

            for (Map.Entry<String, Object> entry : event.entrySet()) {
                if (entry.getKey().toLowerCase().contains("count")) {
                    String metricName = entry.getKey();
                    Long count = (Long) entry.getValue();
                    if (count != null) {
                        getCounterByMetricName(metricName, false).inc(count);
                    } else {
                        LOGGER.warn("Null Count returned by EPL, CountName:"
                                + metricName);
                    }
                }
            }
        } else if (MCCounterHelper.isGroupByCounterEvent(event)) {
            if (getConfiguration().isEnableGroupByCounter()) {
                String metricName = (String) event.get(MCConstant.METRIC_NAME);
                String groupId = (String) event.get(MCConstant.GROUP_ID);
                if (groupId != null && groupId.trim().length() != 0) {
                    long registerdFreq = getFrequencyByMetricName(metricName);

                    // Unregistered, using default to register on the fly
                    if (event.get(MCConstant.FREQUENCY_IN_MIN) == null
                            && registerdFreq <= 0) {
                        setMetricFrequency(metricName,
                                MetricFrequency.ONE_MINUTE.getValue());
                    }
                    // Update registered frequency on the fly by EPL
                    else if (event.get(MCConstant.FREQUENCY_IN_MIN) != null) {
                        Integer frequencyInMin = (Integer) event
                                .get(MCConstant.FREQUENCY_IN_MIN);
                        long frequency = frequencyInMin
                                * MetricFrequency.ONE_MINUTE.getValue();
                        if (frequency > 0 && frequency != registerdFreq) {
                            setMetricFrequency(metricName, frequency);
                        }
                    }
                    metricTables.put(metricName, event.getEventType());

                    Map<String, String> tags = new HashMap<String, String>(5);
                    MCCounterHelper.isGroupByCounterEventWithTag(event, tags);

                    Long total = null;
                    if (MCCounterHelper.isAvgEvent(metricName)) {
                        for (Map.Entry<String, Object> entry : event.entrySet()) {
                            if (entry.getKey().toLowerCase().contains("total")) {
                                total = (Long) entry.getValue();
                                break;
                            }
                        }
                    }

                    if (event.get(MCConstant.METRIC_COUNT) != null) {
                        Long count = (Long) event.get(MCConstant.METRIC_COUNT);
                        if (total != null) {
                            getCounterByMetricDemensionAndInc(metricName,
                                    groupId, tags, true, count, total);
                        } else {
                            getCounterByMetricDemensionAndInc(metricName,
                                    groupId, tags, false, count, null);
                        }
                    } else {
                        getCounterByMetricDemensionAndInc(metricName, groupId,
                                tags, false, null, null);
                    }
                } else {
                    LOGGER.warn("Null or empty groupId returned by EPL, metricName:"
                            + metricName);
                }
            }
        } else {
            evaluateRawEvent(event);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        summingTimingTask = new MetricsSummingTimingTask(this);
        timer.scheduleAtFixedRate(summingTimingTask, 0, TIMER_FREQUENCY);
        Management.addBean(getBeanName(), this);
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void shutDown() {
        if (shutdownFlag.compareAndSet(false, true)) {
            LOGGER.warn("Start gracefully stop MCSummingProcessor!");
            try {
                collectMetrics(metricFrequencies.keySet(), true);
            } catch (Exception ex) {
                LOGGER.error("Error collecting metrics in MCSummingProcessor before shutdown:"
                                + ex.getMessage());
                registerError(ex);
            }
            timer.cancel();
            Management.removeBeanOrFolder(getBeanName(), this);
            LOGGER.warn("Gracefully stopped MCSummingProcessor!");
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

    @ManagedOperation
    @Override
    public void resume() {
        if (!isPaused()) {
            LOGGER.warn(getBeanName() + " could not be resumed. It is already in resume state");
            return;
        }
        changeState(ProcessorOperationState.RESUME);
    }

    public long getMapClearCount() {
        return mapClearCount.get();
    }

    public long getGroupByMetricCount() {
        return groupByMetricCount.get();
    }

    public long getNormalMetricCount() {
        return normalMetricCount.get();
    }

    public long getReceivedRawEventCount() {
        return rawEventCount.get();
    }

    public long getMaxMetricCollectionTime() {
        return maxMetricCollectionTime;
    }

    public long getLastMetricCollectionTime() {
        return lastMetricCollectionTime;
    }

    public long getTotalMetricCollectionCount() {
        return totalMetricCollectionCount.get();
    }

    public String getMetricCollectionCount() {
        return metricCollectionCounts.toString();
    }

    public long getLatestDimensionSize() {
        return latestDimensionSize;
    }

    public long getMaxDimensionSize() {
        return maxDimensionSize;
    }

    public String getInternalMapSize() {
        StringBuilder builder = new StringBuilder();
        builder.append("NormalMetricsMapSize:");
        builder.append(normalMetrics.size());
        builder.append(";");
        builder.append("GroupbyMetricsMapSize:");
        for (Map.Entry<String, Map<MCMetricGroupDemension, Counter>> entry : groupbyWithTagsMetricMap
                .entrySet()) {
            String metricName = entry.getKey();
            builder.append("Metric:");
            builder.append(metricName);
            builder.append("-Size:");
            Map<MCMetricGroupDemension, Counter> counterMap = entry.getValue();
            if (counterMap != null) {
                builder.append(counterMap.size());
            } else {
                builder.append("NULL");
            }
            builder.append(";");
        }
        return builder.toString();
    }

    public String getInternalOffHeapManagerInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("InternalOffHeapManagerInfo:");
        Map<String, OffHeapMemoryManager> memoryManagers = OffHeapMemoryManagerRegistry
                .getInstance().getMemoryManagers();
        for (Map.Entry<String, OffHeapMemoryManager> entry : memoryManagers
                .entrySet()) {
            builder.append("MemoryManager-");
            builder.append(entry.getKey());
            builder.append(":");
            OffHeapMemoryManager manager = entry.getValue();
            if (manager != null) {
                builder.append("[");
                builder.append("FreeMemory:");
                builder.append(manager.getFreeMemory());
                builder.append(";");
                builder.append("MaxMemory:");
                builder.append(manager.getMaxMemory());
                builder.append(";");
                builder.append("ReservedMemory:");
                builder.append(manager.getReservedMemory());
                builder.append(";");
                builder.append("UsedMemory:");
                builder.append(manager.getUsedMemory());
                builder.append(";");
                builder.append("OutOfMeomoryErrorCount:");
                builder.append(manager.getOOMErrorCount());
                builder.append("]");
            }
        }
        return builder.toString();
    }

    public String getInternalTimerInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("TimeFrequency:");
        builder.append(TIMER_FREQUENCY);
        builder.append(";");
        builder.append("IterationCounter:");
        builder.append(timerIterationCounter);
        builder.append(";");
        builder.append("TimerLoopSize:");
        builder.append(timerLoopSize);
        builder.append(";");
        return builder.toString();
    }

    public String getMetricFrequencies() {
        return metricFrequencies.toString();
    }

    public String getMetricTableMapping() {
        return metricTables.toString();
    }

    public JetstreamEvent getLastRawEvent() {
        return lastRawEvent;
    }

    // least common multiple in frequencies
    private long findFreqLCM(long minFrequency) {
        Set<Long> values = frequencyMetrics.keySet();
        if (values == null || values.isEmpty()) {
            return 1;
        }
        Long valueArray[] = values.toArray(new Long[0]);
        long result = valueArray[0];
        for (int i = 1; i < valueArray.length; i++) {
            result = lcm(result, valueArray[i]);
        }
        return (result / minFrequency);
    }

    private long lcm(long a, long b) {
        return a * (b / gcf(a, b));
    }

    // greatest common factor
    private static final long gcf(long a, long b) {
        if (b == 0) {
            return a;
        }
        return gcf(b, a % b);
    }

    private void incrementMetricCollectionCounts(Set<String> value) {
        for (String metricName : value) {
            LongCounter metricCollectionCount = metricCollectionCounts
                    .get(metricName);
            if (metricCollectionCount == null) {
                metricCollectionCount = new LongCounter();
                metricCollectionCounts.put(metricName, metricCollectionCount);
            }
            metricCollectionCount.increment();
        }
    }

    private class MetricsSummingTimingTask extends TimerTask {
        MCMetricsProvider metricProvider;

        public MetricsSummingTimingTask(MCMetricsProvider metricProvider) {
            this.metricProvider = metricProvider;
        }

        public void run() {
            try {
                timerIterationCounter++;

                evaluateDimensionSize();

                // find out a loop size for the timer
                long timer_loop_size = findFreqLCM(TIMER_FREQUENCY);
                timerLoopSize = timer_loop_size;

                Set<String> avaiableCounterName = new HashSet<String>();
                for (Entry<Long, Set<String>> entry : frequencyMetrics
                        .entrySet()) {
                    Long freq = entry.getKey();
                    if ((timerIterationCounter * TIMER_FREQUENCY)
                            % freq.longValue() > 0) {
                        continue;
                    }
                    avaiableCounterName.addAll(frequencyMetrics.get(freq));
                    incrementMetricCollectionCounts(entry.getValue());
                }

                metricProvider.collectMetrics(avaiableCounterName, true);
                long executionTime = System.currentTimeMillis()
                        - scheduledExecutionTime();
                if (executionTime >= TIMER_FREQUENCY) {
                    LOGGER.warn("Metrics publishing took too long ("
                            + executionTime + "ms).");
                }
                if (timerIterationCounter >= timer_loop_size) {
                    timerIterationCounter = 0;
                }
            } catch (Exception ex) {
                LOGGER.error("Error scheduing MCSummingProcessorTimer in MCSummingProcessor:"
                        + ex.getMessage());
                registerError(ex);
            }
        }

        private void evaluateDimensionSize() {
            int size = normalMetrics.size();
            for (Map.Entry<String, Map<MCMetricGroupDemension, Counter>> entry : groupbyWithTagsMetricMap
                    .entrySet()) {
                Map<MCMetricGroupDemension, Counter> counterMap = entry
                        .getValue();
                if (counterMap != null) {
                    size = size + counterMap.size();
                }
            }
            latestDimensionSize = size;
            if (latestDimensionSize > maxDimensionSize) {
                maxDimensionSize = latestDimensionSize;
            }
        }
    }
}
