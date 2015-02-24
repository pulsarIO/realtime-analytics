/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.replay.processor;

import java.util.Collection;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.processor.kafka.SimpleKafkaProcessor;
import com.ebay.jetstream.event.processor.kafka.SimpleKafkaProcessorConfig;

@ManagedResource(objectName = "Event/ReplayInboundKafkaProcessor", description = "Replay inbound kafka processor")
public class ReplayInboundKafkaProcessor extends SimpleKafkaProcessor implements
InitializingBean {
    private static final String REPLAY_TAG = JetstreamReservedKeys.EventReplay
            .toString();

    private final LongCounter m_totalEventsDelayed = new LongCounter();

    @Override
    public BatchResponse onNextBatch(BatchSource source,
            Collection<JetstreamEvent> events) throws EventException {

        // check if it's time to send the batch, check the delay time of the
        // lastEvent, if it's no time to send the batch, tell the IKC to wait
        // and resend this batch
        long start = System.currentTimeMillis();
        if (getReplayConfig().getTimestampKey() != null
                && getReplayConfig().getDelayInMs() > 0) {
            JetstreamEvent[] eventArr = events
                    .toArray(new JetstreamEvent[events.size()]);
            JetstreamEvent lastEvent = eventArr[eventArr.length - 1];
            Long ts = (Long) lastEvent.get(getReplayConfig().getTimestampKey());
            if (ts != null) {
                long v = ts + getReplayConfig().getDelayInMs() - start;
                if (v > 0) {
                    incrementEventRecievedCounter(events.size());
                    incrementEventDelayedCounter(events.size());
                    return BatchResponse.getNextBatch()
                            .setOffset(source.getHeadOffset())
                            .setWaitTimeInMs(v);
                }
            }
        }

        return super.onNextBatch(source, events);

    }

    public void incrementEventDelayedCounter(long lNumber) {
        m_totalEventsDelayed.addAndGet(lNumber);
    }

    public long getTotalEventsDelayed() {
        return m_totalEventsDelayed.get();
    }

    @Override
    public void simplySendEvent(JetstreamEvent event) throws EventException {
        // check the expiration first.
        long current = System.currentTimeMillis();
        if (!event.containsKey(Constants.REPLAY_INITIAL_TS)) {
            event.addMetaData(Constants.REPLAY_INITIAL_TS, current);
            event.addMetaData(Constants.REPLAY_LAST_TS, current);
        } else if (event.containsKey(Constants.REPLAY_LAST_TS)) {
            long initTs = (Long) event.get(Constants.REPLAY_INITIAL_TS);

            if (initTs + getReplayConfig().getRetentionInMs() < current) {
                this.incrementEventDroppedCounter();
                return;
            }
            event.addMetaData(Constants.REPLAY_INITIAL_TS, initTs);
            event.addMetaData(Constants.REPLAY_LAST_TS, current);

            // remove these replay_only attributes to reduce the payload.
            event.remove(Constants.REPLAY_INITIAL_TS);
            event.remove(Constants.REPLAY_LAST_TS);
        }

        if (event.containsKey(JetstreamReservedKeys.RetryCount.toString())) {
            event.put(JetstreamReservedKeys.RetryCount.toString(), 0);
        }
        if (event.containsKey(getReplayConfig().getTimestampKey())) {
            event.remove(getReplayConfig().getTimestampKey());
        }

        String replayTopicKey = JetstreamReservedKeys.EventReplayTopic
                .toString();

        if (replayTopicKey != null && event.containsKey(replayTopicKey)) {
            event.remove(replayTopicKey);
        }
        event.put(REPLAY_TAG, Boolean.TRUE);
        event.setForwardingTopics(null);
        super.fireSendEvent(event);

        this.incrementEventSentCounter();
    }

    @Override
    public void init() throws Exception {
        super.init();
    }

    public ReplayKafkaProcessorConfig getReplayConfig() {
        SimpleKafkaProcessorConfig config = getConfig();
        if (config != null) {
            return (ReplayKafkaProcessorConfig) config;
        } else {
            return null;
        }
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {

            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

            if (bcInfo.isChangedBean(getConfig())) {
                setConfig((ReplayKafkaProcessorConfig) bcInfo.getChangedBean());
            }
        }
    }
}
