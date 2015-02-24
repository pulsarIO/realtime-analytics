/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.event.channel.messaging.MessagingChannelAddress;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventConsumer;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.ConsistentHashingRingUpdateListener;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.ConsistentHashing;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.pulsar.sessionizer.impl.SessionizerProcessor;

/**
 * Listener to the jetstream scheduler to maintain the cluster info.
 * 
 * @author xingwang
 *
 */
public class SessionizerLoopbackRingListener
implements ConsistentHashingRingUpdateListener, ClusterManager, XSerializable, ShutDownable {
    private class ConsistentHashingState {
        private final ConsistentHashing<EventConsumerInfo> chState;
        private final long effectiveTime;
        private ConsistentHashingState previous;
        private final List<Long> consumerIds;
        private final Date expiredDate;

        public ConsistentHashingState(ConsistentHashing<EventConsumerInfo> state, List<Long> consumerIds, Date expiredDate) {
            this.chState = state;
            this.effectiveTime = System.nanoTime();
            this.consumerIds = consumerIds;
            this.expiredDate = expiredDate;
        }

        private <T> boolean check(T affinityKey, EventConsumerInfo currentInfo, long currentNanoTime) {
            if (chState == null) {
                return true;
            }
            EventConsumerInfo oldInfo = chState.get(affinityKey);

            if (oldInfo.getAdvertisement().getConsumerId() == currentInfo.getAdvertisement().getConsumerId()) {
                if ((currentNanoTime - this.effectiveTime) < TimeUnit.NANOSECONDS.convert(getMaxIdleTime() + GRACE_PERIOD, TimeUnit.MILLISECONDS)) {
                    ConsistentHashingState previousState = previous;
                    if (previousState != null) {
                        return previousState.check(affinityKey, currentInfo, currentNanoTime);
                    } else {
                        return true;
                    }
                } else {
                    if (previous != null) {
                        previous = null;
                    }
                    return false;
                }
            } else {
                return true;
            }
        }

        public List<Long> getConsumerIds() {
            return consumerIds;
        }

        public Date getExpiredDate() {
            return expiredDate;
        }
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionizerLoopbackRingListener.class);
    
    private JetstreamTopic loopbackTopic;

    public static final long GRACE_PERIOD = 5 * 60 * 1000;

    private volatile ConsistentHashingState head;

    private volatile boolean shutdownFlag = false;
    // Use this to determine the graceful shutdown case.
    // When graceful shutdown, the inbound channel will be closed and this
    // listener may think the cluster have ring changes. use this flag to
    // avoid send out inter cluster event when this instance is trying to leave the cluster.
    private volatile boolean leavingCluster = true;

    private volatile Map<JetstreamTopic, ConsistentHashing<EventConsumerInfo>> currentState = new ConcurrentHashMap<JetstreamTopic, ConsistentHashing<EventConsumerInfo>>();

    private Date lastModifiedTime;
    private SessionizerProcessor sessionizer;
    private final List<Long> activeConsumers = new ArrayList<Long>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    @Override
    public <T> boolean isOwnershipChangedRecently(T affinityKey) {
        if (shutdownFlag || leavingCluster) {
            return false;
        }
        long currentNanoTime = System.nanoTime();
        if (head == null || (currentNanoTime - head.effectiveTime) > TimeUnit.NANOSECONDS.convert(getMaxIdleTime() + GRACE_PERIOD, TimeUnit.MILLISECONDS)) {
            return false;
        } else {
            EventConsumerInfo currentInfo = null;
            try {
                if (currentState.containsKey(loopbackTopic)) {
                    ConsistentHashing<EventConsumerInfo> che = currentState.get(loopbackTopic);
                    Object obj = affinityKey;
                    if ((che != null) && (obj != null)) {
                        currentInfo = che.get(obj);
                    }
                }
            } catch (Throwable t) {}

            if (currentInfo == null) {
                // Invalid state,
                return false;
            }
            return head.check(affinityKey, currentInfo, currentNanoTime);
        }
    }

    @Override
    public long getHostFailureTime(long clientId) {
        lock.readLock().lock();
        try {
            ConsistentHashingState x = head;
            while (x != null) {
                if (x.getConsumerIds().contains(clientId)) {
                    return x.getExpiredDate().getTime();
                }
                x = x.previous;
            }
            return -1;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    @Override
    public long getMaxIdleTime() {
        if (sessionizer == null) {
            return 0L;
        }
        return sessionizer.getMaxIdleTime();
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public boolean isLeader() {
        if (shutdownFlag || leavingCluster) {
            return false;
        }
        lock.readLock().lock();
        try {
            ArrayList<Long> copy = new ArrayList<Long>(activeConsumers);
            if (copy.isEmpty()) {
                return false;
            }
            Long maxConsumerId = Collections.max(copy);
            return maxConsumerId == getHostId();
        } finally {
            lock.readLock().unlock();
        }
    }


    @Override
    public boolean isHostLive(long clientId) {
        lock.readLock().lock();
        try {
            return activeConsumers.contains(clientId);
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public <T> boolean hasOwnership(T affinityKey) {
        if (shutdownFlag || leavingCluster) {
            return true;
        }
        long currentNanoTime = System.nanoTime();
        if (head == null || (currentNanoTime - head.effectiveTime) > TimeUnit.NANOSECONDS.convert(getMaxIdleTime() + GRACE_PERIOD, TimeUnit.MILLISECONDS)) {
            return true;
        }
        EventConsumerInfo currentInfo = null;
        try {
            if (currentState.containsKey(loopbackTopic)) {
                ConsistentHashing<EventConsumerInfo> che = currentState.get(loopbackTopic);
                Object obj = affinityKey;
                if ((che != null) && (obj != null)) {
                    currentInfo = che.get(obj);
                }
            }
        } catch (Throwable t) {}

        return currentInfo == null || currentInfo.getAdvertisement().getConsumerId() == getHostId();
    }

    public void setLoopbackChannelAddress(MessagingChannelAddress loopbackChannelAddress) {
        if (loopbackChannelAddress != null && loopbackChannelAddress.getChannelJetstreamTopics().size() == 1) {
            loopbackTopic = loopbackChannelAddress.getChannelJetstreamTopics().get(0);
        } else {
            throw new IllegalArgumentException("Loopback address can only have one topic");
        }
    }

    @Override
    public void setSessionizer(SessionizerProcessor sessionizer) {
        this.sessionizer = sessionizer;
    }

    @Override
    public void shutDown() {
        shutdownFlag = true;
    }

    @Override
    public void update(Map<JetstreamTopic, ConsistentHashing<EventConsumerInfo>> map,  Map<Long, EventConsumerInfo> allConsumers) {
        lock.writeLock().lock();
        try {
            Date changedDate = new Date();
            List<Long> existedConsumers = new ArrayList<Long>(activeConsumers);
            currentState = map;
            boolean hasNewConsumer = false;
            List<Long> newConsumers = new ArrayList<Long>();

            for (EventConsumerInfo info : allConsumers.values()) {
                if (info.getAdvertisement().getInterestedTopics() != null && info.getAdvertisement().getInterestedTopics().contains(loopbackTopic)) {
                    if (!activeConsumers.contains(info.getAdvertisement().getConsumerId())) {
                        hasNewConsumer = true;
                    }
                    newConsumers.add(info.getAdvertisement().getConsumerId());
                }
            }

            if (activeConsumers.size()  == newConsumers.size() && !hasNewConsumer) {
                // no change, just return
                return;
            }
            activeConsumers.clear();
            activeConsumers.addAll(newConsumers);
            LOGGER.info("Consistent hashing changed: {}", activeConsumers);

            Collections.sort(activeConsumers);
            ConsistentHashing<EventConsumerInfo> hashing = map.get(loopbackTopic);
            ConsistentHashing<EventConsumerInfo> chState = null;
            if (hashing != null) {
                chState = new ConsistentHashing<EventConsumerInfo>(
                        hashing.getHashFunction(), hashing.getNumHashesPerEntry(),
                        hashing.getSpreadFactor());
                for (int i = 0, t = activeConsumers.size(); i < t; i++) {
                    EventConsumerInfo point = allConsumers.get(activeConsumers.get(i));
                    chState.add(point);
                }
            }

            lastModifiedTime = changedDate;
            ConsistentHashingState newHead = new ConsistentHashingState(chState, existedConsumers, lastModifiedTime);
            newHead.previous = head;
            ConsistentHashingState oldHead = head;
            int count = 0;
            leavingCluster = !activeConsumers.contains(getHostId());
            if (oldHead != null) {
                ConsistentHashingState c = oldHead;
                long currentNanoTime = System.nanoTime();
                while (c.previous != null) {
                    if (count > 200) {
                        LOGGER.warn("Too much consistent hashing history (exceed 100 in 30 minutes), ignore oldest one");
                        c.previous = null;
                        break;
                    }
                    if ((currentNanoTime - c.effectiveTime) > TimeUnit.NANOSECONDS.convert(getMaxIdleTime() + GRACE_PERIOD, TimeUnit.MILLISECONDS)) {
                        // Leave 10 minutes buffer;
                        c.previous = null;
                        break;
                    } else {
                        c = c.previous;
                        // the c.previous maybe change to null by another thread.
                        count ++;
                    }
                }
            }
            head = newHead;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long getHostId() {
        return EventConsumer.getConsumerId();
    }
}