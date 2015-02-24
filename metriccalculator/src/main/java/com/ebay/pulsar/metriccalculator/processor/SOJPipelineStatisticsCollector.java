/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.processor;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.pulsar.metriccalculator.statistics.basic.Counter;
import com.ebay.pulsar.metriccalculator.util.MCConstant;
import com.ebay.pulsar.metriccalculator.util.MetricDef;
import com.ebay.pulsar.metriccalculator.util.ThreadSafeDateParser;

@ManagedResource(objectName = "Event/Processor", description = "SOJPipelineStatistics Collector")
public class SOJPipelineStatisticsCollector extends AbstractEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.pulsar.metriccalculator.processor.SOJPipelineStatisticsCollector");

    private static final String VALIDATION_TYPE = "metricName";

    private static final Long ONE_MINUTE = 60 * 1000L;
    private static final Long ONE_HOUR = 60 * 60 * 1000L;

    private static final String DATEPATTERN = "yyyy-MM-dd";
    private static final TimeZone TIMEZONE = TimeZone.getTimeZone("MST");
    private static final String affinityKey = JetstreamReservedKeys.MessageAffinityKey
            .toString();

    private Map<MetricDef, Counter> metrics = new ConcurrentHashMap<MetricDef, Counter>();
    private ScheduledExecutorService timer;

    private volatile long currentMaxTime;

    public SOJPipelineStatisticsCollector() {
        timer = MCScheduler.getMCScheduler();
        timer.scheduleWithFixedDelay(new MetricChecker(), ONE_MINUTE,
                ONE_MINUTE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        incrementEventRecievedCounter();
        if (isValidationEvent(event)) {
            postMetricFromEvent(event);
        }

    }

    private void postMetricFromEvent(JetstreamEvent event) {
        MetricDef metricDef = new MetricDef();
        metricDef.setMetricName((String) event.get(VALIDATION_TYPE));

        Long time = (Long) event.get("timestamp");
        if (currentMaxTime < time) {
            currentMaxTime = time;
        }
        long metricTime = (time / ONE_HOUR) * ONE_HOUR;

        try {
            String groupName = ThreadSafeDateParser.format(new Date(time),
                    DATEPATTERN, TIMEZONE);
            metricDef.setMetricGroup(groupName);
        } catch (ParseException e) {
            LOGGER.warn("failed to format date from time:" + time);
        }

        metricDef.setMetricTime(metricTime);
        metricDef.setFrequency(ONE_MINUTE);
        metricDef.setMetricTable(event.getEventType());
        metricDef.setAffinityKey(event.get(affinityKey));
        metricDef.setTopics(event.getForwardingTopics());

        updateMetric(metricDef);
    }

    private void updateMetric(MetricDef metricDef) {
        Counter counter = metrics.get(metricDef);
        if (counter == null) {
            counter = new Counter();
            metrics.put(metricDef, counter);
        }
        counter.inc();
    }

    private boolean isValidationEvent(JetstreamEvent event) {
        if (event.containsKey(VALIDATION_TYPE)) {
            return true;
        }
        return false;
    }

    private void flushMetrics(MetricDef metricDef) {
        Counter count = metrics.get(metricDef);
        count.mark();

        if (count.getLastDeltaValue() > 0) {
            Map<String, Object> internalMap = new HashMap<String, Object>();
            internalMap.put(MCConstant.METRIC_COUNT, count.getLastDeltaValue());
            internalMap.put(MCConstant.METRIC_NAME, metricDef.getMetricName());
            internalMap.put(MCConstant.GROUP_ID, metricDef.getMetricGroup());
            internalMap.put(MCConstant.METRIC_TIME, metricDef.getMetricTime());
            internalMap.put(MCConstant.METRIC_FREQUENCY,
                    metricDef.getFrequency());
            internalMap.put(affinityKey, metricDef.getAffinityKey());

            JetstreamEvent event = new JetstreamEvent(
                    metricDef.getMetricTable(), null, internalMap);
            event.setForwardingTopics(metricDef.getTopics());
            fireSendEvent(event);
            incrementEventSentCounter();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Management.removeBeanOrFolder(getBeanName(), this);
        Management.addBean(getBeanName(), this);
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void shutDown() {
        for (MetricDef metricDef : metrics.keySet()) {
            // Flush metrics to MC pool
            flushMetrics(metricDef);
            metrics.remove(metricDef);
        }
    }

    @Override
    public void pause() {
    }

    public long getMetricSize() {
        return metrics.size();
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
    }

    @Override
    public void resume() {
    }

    private class MetricChecker implements Runnable {
        @Override
        public void run() {
            long latestTime = (System.currentTimeMillis() > currentMaxTime ? System
                    .currentTimeMillis() : currentMaxTime);
            for (MetricDef metricDef : metrics.keySet()) {
                // Flush metrics to MC pool
                flushMetrics(metricDef);
                if (latestTime - metricDef.getMetricTime() >= 24 * ONE_HOUR) {
                    metrics.remove(metricDef);
                }
            }
        }
    }
}
