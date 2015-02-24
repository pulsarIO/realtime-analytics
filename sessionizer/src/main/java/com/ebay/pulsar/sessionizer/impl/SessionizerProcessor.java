/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.processor.esper.EPL;
import com.ebay.jetstream.event.processor.esper.EsperDeclaredEvents;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.jetstream.util.disruptor.SingleConsumerDisruptorQueue;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.pulsar.sessionizer.cache.MemoryCache;
import com.ebay.pulsar.sessionizer.cache.impl.SessionMemoryCache;
import com.ebay.pulsar.sessionizer.cluster.ClusterManager;
import com.ebay.pulsar.sessionizer.config.SessionProfile;
import com.ebay.pulsar.sessionizer.config.SessionizerConfig;
import com.ebay.pulsar.sessionizer.config.SessionizerConfigValidator;
import com.ebay.pulsar.sessionizer.esper.impl.EsperController;
import com.ebay.pulsar.sessionizer.esper.impl.SessionizerEsperExceptionHandlerFactory;
import com.ebay.pulsar.sessionizer.impl.SessionizerErrorManager.ErrorType;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;
import com.ebay.pulsar.sessionizer.spi.RemoteStoreCallback;
import com.ebay.pulsar.sessionizer.spi.RemoteStoreProvider;
import com.ebay.pulsar.sessionizer.spi.SessionizerExtension;
import com.ebay.pulsar.sessionizer.util.BinaryFormatSerializer;
import com.ebay.pulsar.sessionizer.util.Constants;
/**
 * Sessionizer processor.
 * 
 * Entrypoint for sessionization.
 * 
 * @author xingwang
 *
 */
@ManagedResource(objectName = "Event/Processor", description = "Sessionizer")
public class SessionizerProcessor extends AbstractEventProcessor implements RemoteStoreCallback {

    
    
    private class LeakedRemoteSessionChecker implements Runnable {
        @Override
        public void run() {
            if (!timerFlag.get()) {
                return;
            }
            try {
                RemoteStoreProvider l = provider;
                if (l != null) {
                    l.checkExpiredSession();
                }
            } catch (Throwable e) {
                errorManager.registerError(e, ErrorType.Unexpected);
            }
        }
    }
    private class LocalExpirationChecker implements Runnable {
        @Override
        public void run() {
            if (!timerFlag.get()) {
                return;
            }
            try {
                for (BlockingQueue<JetstreamEvent> q : requestQueues) {
                    try {
                        q.put(TIMER_EVENT);
                    } catch (InterruptedException e) {
                        errorManager.registerError(e, ErrorType.Unexpected);
                    }
                }
            } catch (Throwable e) {
                errorManager.registerError(e, ErrorType.Unexpected);
            }
        }
    }

    private static class PendingEventHolder {
        private final Queue<JetstreamEvent> queue = new LinkedList<JetstreamEvent>();
        private final long timestamp = System.nanoTime();

        public long getTimestamp() {
            return timestamp;
        }

        public Queue<JetstreamEvent> getQueue() {
            return queue;
        }

        public void offer(JetstreamEvent ex) {
            queue.offer(ex);
        }
    }

    private class CompiledConfig {
        private Map<Integer, Sessionizer> newSessionTypeToSessionerMap;
        private Map<String, Sessionizer> newSessionizerMap;
        private EsperController newSelector;
        private final List<Sessionizer> createdSessionizers = new ArrayList<Sessionizer>();
        public CompiledConfig(SessionizerConfig config, SessionizerRunnable task) {
            boolean success = false;
            try {
                newSessionTypeToSessionerMap = new HashMap<Integer, Sessionizer>();
                newSessionizerMap = new HashMap<String, Sessionizer>();

                for (SessionProfile profile : config.getMainSessionProfiles()) {
                    Sessionizer sessionizer = new Sessionizer(task.taskId, profile, task, config.getMaxIdleTime(), esperCounter, extensions);
                    createdSessionizers.add(sessionizer);
                    newSessionTypeToSessionerMap.put(profile.getSessionType(), sessionizer);
                    newSessionizerMap.put(profile.getName(), sessionizer);
                    if (!task.counters.containsKey(sessionizer.getType())) {
                        task.counters.put(sessionizer.getType(), new SessionizerCounterManager());
                    }
                }

                newSelector = new EsperController(config.getEpl(), config.getRawEventDefinition(),
                        "SessionizerController" + System.currentTimeMillis(), config.getImports(),
                        esperCounter, newSessionizerMap.keySet());
                success = true;
            } finally {
                if (!success) {
                    for (Sessionizer s : createdSessionizers) {
                        s.destroy();
                    }
                    if (newSelector != null) {
                        newSelector.destroy();
                    }
                }
            }
        }
        public void destroy() {
            for (Sessionizer s : createdSessionizers) {
                s.destroy();
            }
            if (newSelector != null) {
                newSelector.destroy();
            }
        }
    }

    private List<SessionizerExtension> extensions;

    public void setExtensions(List<SessionizerExtension> extensions) {
        this.extensions = extensions;
    }

    private class SessionizerRunnable implements Runnable, SsnzEventSender {
        private final Map<Integer, SessionizerCounterManager> counters = new HashMap<Integer, SessionizerCounterManager>();
        private final SessionizerCounterManager mainCounter = new SessionizerCounterManager();
        private CompiledConfig newCompiledConfig;
        private long maxSessionDuration;
        private int maxEventCount;
        private long maxEventInterval;
        private long maxSessionLagTime;
        private long maxSessionExpirationDelay;

        private long sessionEvitCounter = 0;
        private long subSessionEvitCounter = 0;
        private final long invalidRawEventCounter = 0;
        private long invalidInternalEventCounter = 0;
        private long bypassEventCounter= 0;

        private long pendingReadsCounter = 0;
        private long asyncReadFailure = 0;

        private long onDemandExpiredSessionCounter = 0;
        private long readTimeoutCounter = 0;
        private long readHitsCounter = 0;
        private long readResponseIgnoredCounter = 0;
        private long missedReadResponseCounter = 0;
        private long eventSentInLastSecond;
        private long eventSentInCurrentSecond;
        private long maxEventsSentPerSecond;

        private final Map<String, PendingEventHolder> pendingReadEvents = new LinkedHashMap<String, PendingEventHolder>();
        private Map<Integer, Sessionizer> sessionTypeToSessionerMap = new HashMap<Integer, Sessionizer>();
        private Map<String, Sessionizer> sessionizerMap = new HashMap<String, Sessionizer>();
        private final BlockingQueue<JetstreamEvent> requestQueue;
        private final Queue<JetstreamEvent> responseQueue;
        private final Queue<JetstreamEvent> localQueue;
        private int lastPendingSize = 0;
        private boolean hasPendingExpiration = false;
        private volatile boolean running = true;
        private final int taskId;
        private long queryTimeOutInNano;

        private boolean enableReadOptimization;
        private final MemoryCache localSessionCache;
        private volatile boolean intialized = false;
        private volatile boolean intializeFailed = false;

        public SessionizerRunnable(BlockingQueue<JetstreamEvent> requestQueue, Queue<JetstreamEvent> responseQueue, int taskId) {
            this.requestQueue = requestQueue;
            this.responseQueue = responseQueue;
            this.taskId = taskId;
            localSessionCache = new SessionMemoryCache(config);
            localQueue = new LinkedList<JetstreamEvent>();
        }

        private boolean asyncLoadFromRemoteStore(JetstreamEvent event, String uid, RemoteStoreProvider remoteDAO, String affinityKey) {
            if (remoteDAO.asyncLoad(uid, affinityKey)) {
                PendingEventHolder holder = new PendingEventHolder();
                holder.offer(event);
                pendingReadEvents.put(uid, holder);
                pendingReadsCounter++;
                return true;
            } else {
                asyncReadFailure++;
                return false;
            }
        }

        private void checkTimeOutPendingRequests() {
            long currentNanoTime = System.nanoTime();
            Iterator<Entry<String, PendingEventHolder>> iterator = pendingReadEvents.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, PendingEventHolder> entry = iterator.next();
                if (currentNanoTime - entry.getValue().getTimestamp() < queryTimeOutInNano) {
                    break;
                }
                readTimeoutCounter++;
                String uid = entry.getKey();
                String identifier = identifierValue(uid);
                Sessionizer sessionizer = getSessionizer(uid);

                PendingEventHolder holder = entry.getValue();
                iterator.remove();
                pendingReadsCounter--;

                Queue<JetstreamEvent> q = holder.getQueue();
                JetstreamEvent firstEvent = q.poll();
                if (Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT.equals(firstEvent.getEventType())) {
                    // Just ignore it if it is expired session event.
                    firstEvent = q.poll();
                    if (firstEvent == null) {
                        continue;
                    }
                    SessionizationInfo info = (SessionizationInfo) firstEvent.remove(CURRENT_SESSIOIZERINFO);
                    handleRawEvent(uid, identifier, null, firstEvent, sessionizer, info);
                } else if (Constants.EVENT_TYPE_SESSION_TRANSFERED_EVENT.equals(firstEvent.getEventType())) {
                    Session session = reconstructSession(firstEvent, uid);
                    if (session != null) {
                        updateRemoteSession(uid, identifier, session, sessionizer);
                    }
                    firstEvent = q.poll();
                    if (firstEvent == null) {
                        continue;
                    }
                    SessionizationInfo info = (SessionizationInfo) firstEvent.remove(CURRENT_SESSIOIZERINFO);
                    handleRawEvent(uid, identifier, localSessionCache.get(uid), firstEvent, sessionizer, info);
                } else {
                    SessionizationInfo info = (SessionizationInfo) firstEvent.remove(CURRENT_SESSIOIZERINFO);
                    handleRawEvent(uid, identifier, null, firstEvent, sessionizer, info);
                }

                JetstreamEvent nextEvent = q.poll();
                if (nextEvent != null) {
                    Session session = localSessionCache.get(uid);
                    while (nextEvent != null) {
                        SessionizationInfo info = (SessionizationInfo) nextEvent.remove(CURRENT_SESSIOIZERINFO);
                        updateSessionOnly(session, nextEvent, identifier, sessionizer, info);
                        nextEvent = q.poll();
                    }
                    updateSessionOnStore(uid, session);
                }
            }
        }

        private void expiredTimeoutSessions(JetstreamEvent event) {
            if (TIMER_EVENT == event) {
                int pending = this.requestQueue.size();
    
                if (pending >= warnThresHold && pending < errorThresHold && lastPendingSize < warnThresHold) {
                    postAlert("High pending event on sessionizer", AlertListener.AlertStrength.YELLOW);
                } else if (pending >= errorThresHold && lastPendingSize < errorThresHold) {
                    postAlert("Sessionizer is stuck", AlertListener.AlertStrength.RED);
                }
    
                lastPendingSize = pending;
    
                eventSentInLastSecond = eventSentInCurrentSecond;
                if (eventSentInLastSecond > maxEventsSentPerSecond) {
                    maxEventsSentPerSecond = eventSentInLastSecond;
                }
                eventSentInCurrentSecond = 0;
                checkTimeOutPendingRequests();
            } else {
                hasPendingExpiration = false;
            }
            long timestamp = System.currentTimeMillis();
            if (event == CONTINUE_EXPIRATION_EVENT || !hasPendingExpiration) {
                int count = 0;
                Session session = localSessionCache.removeExpired(timestamp);
                while (session != null) {
                    String identifier = session.getIdentifier();
                    Sessionizer sessionizer = getSessionizer(session.getType());
                    String uid = sessionizer.getUniqueSessionIdentifier(identifier);
                    long ts = session.getFirstExpirationTime();
                    if (clusterManager.hasOwnership(session.getAffinityKey())) {
                        sessionizer.checkSubSessions(identifier, session, ts);
                        if (session.getExpirationTime() <= ts) {
                            sessionizer.sessionEnd(identifier, session);
                            RemoteStoreProvider remoteDAO = provider;
                            if (remoteDAO != null) {
                                remoteDAO.delete(session, uid);
                            }
                        } else {
                            updateSessionOnStore(uid, session);
                        }
                    } else {
                        loopbackEventProducer.forwardSessionEndEvent(identifier, session, uid);
                    }
                    if (++ count > 100) { // Check when send 100 expiration events.
                        // Avoid run session expiration for long time when there are long GC pause.
                        // Avoid request queue build up.
                        if ((requestQueue.size() + responseQueue.size()) > 200) {
                            if (!hasPendingExpiration) {
                                hasPendingExpiration = this.requestQueue.offer(CONTINUE_EXPIRATION_EVENT);
                            }
                            break;
                        } else {
                            count = 0;
                        }
                    }
                    session = localSessionCache.removeExpired(timestamp);
                }
            }

            expirationInfo[taskId] = timestamp;
            if (taskId == 0 && TIMER_EVENT == event) {
                RemoteStoreProvider l = provider;
                if (l != null) {
                    l.updateHeartbeat(timestamp);
                }
                // Only need one thread send it.
            }
        }


        private void fireSessionEndMarkerEvent(Session session, Sessionizer sessionizer) {
            String mainIdentifier = session.getIdentifier();
            List<SubSession> subSessions = session.getSubSessions();
            if (subSessions != null) {
                for (int i = subSessions.size() - 1; i >= 0; i--) {
                    SubSession subSession = subSessions.get(i);
                    sessionizer.subSessionEnd(mainIdentifier, session, subSession);
                }
            }

            sessionizer.sessionEnd(mainIdentifier, session);
        }

        private void forceTerminateSession(Session session) {
            String mainIdentifier = session.getIdentifier();
            Sessionizer sessionizer = getSessionizer(session.getType());
            String uid = sessionizer.getUniqueSessionIdentifier(mainIdentifier);
            List<SubSession> subSessions = session.getSubSessions();
            if (subSessions != null) {
                for (int i = subSessions.size() - 1; i >= 0; i--) {
                    SubSession subSession = subSessions.get(i);
                    sessionizer.subSessionEnd(mainIdentifier, session, subSession);
                }
            }

            RemoteStoreProvider remoteDAO = provider;
            if (remoteDAO != null) {
                remoteDAO.delete(session, uid);
            }
            sessionizer.sessionEnd(mainIdentifier, session);
        }

        private void forceUpdateLocalStore(String uid, Session session) {
            maxSessionDuration = Math.max(maxSessionDuration, session.getLastModifiedTime() - session.getCreationTime());
            maxEventCount = Math.max(maxEventCount, session.getEventCount());
            if (localSessionCache.put(uid, session)) {
                return;
            }
            do {
                Session evictedSession = localSessionCache.removeFirst();
                sessionEvitCounter++;
                if (evictedSession.getSubSessions() != null) {
                    subSessionEvitCounter += evictedSession.getSubSessions().size();
                }
                forceTerminateSession(evictedSession);
            } while (!localSessionCache.put(uid, session));
        }

        private Sessionizer getSessionizer(int type) {
            return sessionTypeToSessionerMap.get(Integer.valueOf(type));
        }

        private Sessionizer getSessionizer(String uid) {
            return sessionTypeToSessionerMap.get(Integer.valueOf(identifierType(uid)));
        }


        private void handleExpiredSession(String identifier, String uid, JetstreamEvent event, Sessionizer sessionizer) {
            String ak = (String) event.get(AFFINITY_KEY);
            Session localSession = localSessionCache.get(uid);

            // There are two cases: the event itself carry the session, the event did not carry the session.
            if (event.containsKey(Constants.EVENT_PAYLOAD_SESSION_PAYLOAD) || event.containsKey(Constants.EVENT_PAYLOAD_SESSION_METADATA)) {
                Session session = reconstructSession(event, uid);

                if (session == null) {
                    return;
                }
                if (localSession == null && !pendingReadEvents.containsKey(uid)) {
                    updateRemoteSession(uid, identifier, session, sessionizer);
                } else if (localSession != null) {
                    if (session.getFirstEventTimestamp() != localSession.getFirstEventTimestamp()) {
                        session.setIdentifier(identifier);
                        session.setType(sessionizer.getType());
                        fireSessionEndMarkerEvent(session, sessionizer);
                    }
                } else if (pendingReadEvents.containsKey(uid)) {
                    session.setIdentifier(identifier);
                    session.setType(sessionizer.getType());
                    fireSessionEndMarkerEvent(session, sessionizer);
                }
            } else {

                if (localSession == null && !pendingReadEvents.containsKey(uid)) {
                    // Need reload from remote store and send session End event.

                    RemoteStoreProvider remoteDAO = provider;
                    if (remoteDAO != null && remoteDAO.asyncLoadSupport()) {
                        asyncLoadFromRemoteStore(event, uid, remoteDAO, ak);
                    } else if (remoteDAO != null) {
                        Session session = remoteDAO.load(uid);
                        if (session != null) {
                            updateRemoteSession(uid, identifier, session, sessionizer);
                        }
                    }
                }
            }
        }

        private void handleInternalEvent(JetstreamEvent event, String eventType) {
            Integer sessionType = (Integer) event.get(Constants.EVENT_PAYLOAD_SESSION_TYPE);
            if (sessionType == null) {
                errorManager.registerError("Session type Missed", ErrorType.MissedData, "Session type not found in event");
                return;
            }
            Sessionizer sessionizer = sessionTypeToSessionerMap.get(sessionType);
            if (sessionizer == null) {
                errorManager.registerError("Session profile not configured", ErrorType.Configuration,
                        "Session profile not configured for " + event.getEventType());
                return;
            }
            String uid = (String) event.get(Constants.EVENT_PAYLOAD_SESSION_UNIQUEID);
            if (uid == null) {
                errorManager.registerError("Can not find unique session id", ErrorType.Unexpected,
                        "Can not find unique session id, event type is " + event.getEventType());
                return;
            }
            String identifier = identifierValue(uid);
            if (Constants.EVENT_TYPE_SESSION_LOAD_EVENT.equals(eventType)) {
                sessionLoadedFromRemoteStore(identifier, uid, event, sessionizer);
            } else if (Constants.EVENT_TYPE_SESSION_TRANSFERED_EVENT.equals(eventType)) {
                totalOwnershipChangedSessionReceived.incrementAndGet();
                handleTransferedSession(identifier, uid, event, sessionizer);
            } else if (Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT.equals(eventType)) {
                totalRestoredSessionReceived.incrementAndGet();
                handleExpiredSession(identifier, uid, event, sessionizer);
            } else {
                errorManager.registerError("Invalid event type", ErrorType.MissedData,
                        eventType + " not supported.");
                invalidInternalEventCounter ++;
            }
        }


        private void handleRawEvent(String uid, String identifier, Session existedSession, JetstreamEvent event, Sessionizer sessionizer, SessionizationInfo info) {
            long currentTimestamp = System.currentTimeMillis();
            Session session = existedSession;
            boolean newSession = session == null;
            if (newSession) {
                session = sessionizer.createNewSession(identifier, event, currentTimestamp, info);
            } else {
                SessionizerCounterManager subCounter = counters.get(Integer.valueOf(sessionizer.getType()));
                sessionizer.checkSubSessions(identifier, session, currentTimestamp);
                if (session.getExpirationTime() <= currentTimestamp
                        || (sessionizer.getMaxActiveTime() > 0 && (currentTimestamp - session.getCreationTime()) > sessionizer.getMaxActiveTime())) {
                    if ((currentTimestamp - session.getCreationTime()) > sessionizer.getMaxActiveTime()) {
                        mainCounter.incLongSessionCounter();
                        subCounter.incLongSessionCounter();
                    } else {
                        maxSessionExpirationDelay = Math.max((currentTimestamp - session.getExpirationTime()), maxSessionExpirationDelay);
                        onDemandExpiredSessionCounter ++;
                    }
                    session = sessionizer.renewExpiredSession(identifier, session, event, currentTimestamp, info);
                } else {
                    maxEventInterval = Math.max(maxEventInterval, currentTimestamp - session.getLastModifiedTime());
                    session.setLastModifiedTime(currentTimestamp);
                    session.setExpirationTime(currentTimestamp + session.getTtl());
                    updateDynamicAttributes(session, event, sessionizer, info);
                }
            }

            subSessionize(event, session, identifier, sessionizer, info);

            updateExpirationTime(session);
            RemoteStoreProvider remoteDAO = provider;
            if (remoteDAO != null) {
                if (newSession) {
                    remoteDAO.insert(session, uid);
                } else {
                    remoteDAO.update(session, uid);
                }
            } else {
                session.setRemoteServerInfo(null);
            }
            forceUpdateLocalStore(uid, session);
        }

        private void handleTransferedSession(String identifier, String uid, JetstreamEvent event, Sessionizer sessionizer) {
            String ak = (String) event.get(AFFINITY_KEY);
            Session localSession = localSessionCache.get(uid);
            if (localSession == null && !pendingReadEvents.containsKey(uid)) {
                RemoteStoreProvider remoteDAO = provider;
                if (remoteDAO != null && remoteDAO.asyncLoadSupport()) {
                    asyncLoadFromRemoteStore(event, uid, remoteDAO, ak);
                } else if (remoteDAO != null) {
                    Session session = remoteDAO.load(uid);
                    if (session == null) {
                        session = reconstructSession(event, uid);
                        if (session != null) {
                            updateRemoteSession(uid, identifier, session, sessionizer);
                        }
                    } else {
                        Session transferInSession = reconstructSession(event, uid);
                        if (session.getFirstEventTimestamp() != transferInSession.getFirstEventTimestamp()) {
                            // The transfered session will be some session created during the consistent hash ring change.
                            // The session key should not exist on remote since it can not be found.
                            transferInSession.setIdentifier(identifier);
                            transferInSession.setType(sessionizer.getType());
                            fireSessionEndMarkerEvent(transferInSession, sessionizer);
                        }
                    }
                } else {
                    Session session = reconstructSession(event, uid);
                    if (session != null) {
                        updateRemoteSession(uid, identifier, session, sessionizer);
                    }
                }
            } else if (localSession != null) {
                Session session = reconstructSession(event, uid);
                if (session != null && session.getFirstEventTimestamp() != localSession.getFirstEventTimestamp()) {
                    // The transfered session should be overwrite by the local session
                    // And the remote session will be cleaned by this local session if it create concurrently.
                    session.setIdentifier(identifier);
                    session.setType(sessionizer.getType());
                    fireSessionEndMarkerEvent(session, sessionizer);
                }
            }
        }

        private void processNormalEvent(String identifier, JetstreamEvent event, Sessionizer sessionizer, String affinityKey, SessionizationInfo info) {
            String uid = sessionizer.getUniqueSessionIdentifier(identifier);
            PendingEventHolder holder = pendingReadEvents.get(uid);
            if (holder != null) {
                holder.offer(event);
                return;
            }
            Session session = localSessionCache.get(uid);
            RemoteStoreProvider remoteDAO = provider;
            if (session == null && remoteDAO != null
                    && (!enableReadOptimization || clusterManager.isOwnershipChangedRecently(
                            affinityKey))) {
                if (remoteDAO.asyncLoadSupport()) {
                    if (asyncLoadFromRemoteStore(event, uid, remoteDAO, affinityKey)) {
                        return;
                    }
                } else {
                    session = remoteDAO.load(uid);
                }
            }
            event.remove(CURRENT_SESSIOIZERINFO);
            handleRawEvent(uid, identifier, session, event, sessionizer, info);
        }

        public void resetHighWaterMark() {
            maxSessionDuration = 0;
            maxEventCount = 0;
            maxEventInterval = 0;
            maxSessionLagTime = 0;
            maxSessionExpirationDelay = 0;
            maxEventsSentPerSecond = 0;
            localSessionCache.resetMaxItemSize();
        }

        @Override
        public void run() {

            while (running) {
                JetstreamEvent event;
                while ((event = localQueue.poll()) != null) {
                    @SuppressWarnings("unchecked")
                    LinkedList<SessionizationInfo> pendingSessionizers = (LinkedList<SessionizationInfo>) event.get(SESSIONIZER_LIST);
                    SessionizationInfo next = pendingSessionizers.removeFirst();
                    if (pendingSessionizers.isEmpty()) {
                        event.remove(SESSIONIZER_LIST);
                    }
                    processSessionizableEvent(event, sessionizerMap.get(next.getName()), next);
                }
                try {
                    event = responseQueue.poll();
                    if (event == null) {
                        event = requestQueue.take();
                    }
                } catch (InterruptedException e) {
                    continue;
                }
                if (REFRESH_EVENT == event) {
                    refreshCounter.incrementAndGet();
                    continue;
                } else if (RESET_EVENT == event) {
                    resetHighWaterMark();
                    continue;
                } else if (CONFIG_REFRESH_EVENT == event) {
                    try {
                        updateConfig(config);
                    } catch (Throwable ex) {
                        exceptionCounter.incrementAndGet();
                        errorManager.registerError(ex, ErrorType.Unexpected);
                    }
                    continue;
                }
                try {
                    if (TIMER_EVENT == event || event == CONTINUE_EXPIRATION_EVENT) {
                        expiredTimeoutSessions(event);
                    } else {

                        String eventType = (String) event.get(JS_EVENT_TYPE);
                        if (!interEventTypes.contains(eventType)) {
                            eventCounters[taskId]++;

                            Map<String, SessionizationInfo> m = selector.process(event);
                            if (m == null || m.isEmpty()) {
                                bypassEventCounter ++;
                                // bypass non configured event;
                                sendRawEvent(event);
                            } else if (m.size() == 1) {
                                Entry<String, SessionizationInfo> entry = m.entrySet().iterator().next();
                                event.put(CURRENT_SESSIOIZERINFO, entry.getValue());
                                processSessionizableEvent(event, sessionizerMap.get(entry.getKey()), entry.getValue());
                            } else {
                                LinkedList<SessionizationInfo> slist = new LinkedList<SessionizationInfo>(m.values());
                                event.put(SESSIONIZER_LIST, slist);
                                SessionizationInfo sessionizationInfo = slist.removeFirst();
                                event.put(CURRENT_SESSIOIZERINFO, sessionizationInfo);
                                processSessionizableEvent(event, sessionizerMap.get(sessionizationInfo.getName()), sessionizationInfo);
                            }
                        } else {
                            handleInternalEvent(event, eventType);
                        }
                    }
                } catch (Throwable ex) {
                    exceptionCounter.incrementAndGet();
                    errorManager.registerError(ex, event, ErrorType.Unexpected);
                }
            }
        }


        private void processSessionizableEvent(JetstreamEvent event, Sessionizer sessionizer, SessionizationInfo info) {
            counters.get(Integer.valueOf(sessionizer.getType())).incRawEvents();
            String identifier = info.getIdentifier();
            String ak = (String) event.get(AFFINITY_KEY);
            if (ak == null) {
                ak = identifier;
            }
            processNormalEvent(identifier, event, sessionizer, ak, info);

        }

        private void sendRawEvent(JetstreamEvent event) {
            if (event.containsKey(SESSIONIZER_LIST)) {
                localQueue.offer(event);
            } else {
                sendToDownStream(event);
            }
        }

        private void sendToDownStream(JetstreamEvent event) {
            eventSentInCurrentSecond ++;
            incrementEventSentCounter();
            sendSsnzEvent(event);
        }

        @Override
        public void sendSessionBeginEvent(int sessionType, Session session, JetstreamEvent sessionBeginEvent) {
            SessionizerCounterManager subCounter = counters.get(Integer.valueOf(sessionType));
            long lagTime = session.getCreationTime() - session.getFirstEventTimestamp();
            mainCounter.incsessionLagTime(lagTime);
            subCounter.incsessionLagTime(lagTime);
            maxSessionLagTime = Math.max(maxSessionLagTime, lagTime);

            mainCounter.incCreatedSession();
            subCounter.incCreatedSession();
            sendToDownStream(sessionBeginEvent);
        }

        @Override
        public void sendSessionEndEvent(int sessionType, Session session, JetstreamEvent sessionEndEvent) {
            SessionizerCounterManager subCounter = counters.get(Integer.valueOf(sessionType));
            if (session.getEventCount() > 1) {
                long duration = session.getLastModifiedTime() - session.getCreationTime();
                if (duration <= 1800000) {
                    mainCounter.incShortSessionCounter();
                    subCounter.incShortSessionCounter();
                }
                maxSessionDuration = Math.max(maxSessionDuration, duration);
                mainCounter.incSessionDuration(duration);
                subCounter.incSessionDuration(duration);
            }
            maxEventCount = Math.max(maxEventCount, session.getEventCount());
            if (session.getEventCount() == 1) {
                mainCounter.incSingleClickSessionCounter();
                subCounter.incSingleClickSessionCounter();
            }

            mainCounter.incExpiredSession();
            subCounter.incExpiredSession();
            sendToDownStream(sessionEndEvent);
        }

        private void subSessionize(JetstreamEvent event, Session session, String identifier, Sessionizer sessionizer, SessionizationInfo info) {
            sessionizer.handleSubSessionLogic(event, session, identifier, info.getTimestamp());
            sendRawEvent(event);
        }

        @Override
        public void sendSubSessionBeginEvent(int sessionType, SubSession subSession, JetstreamEvent sessionBeginEvent) {
            mainCounter.incCreatedSubSession();
            counters.get(Integer.valueOf(sessionType)).incCreatedSubSession();
            sendToDownStream(sessionBeginEvent);
        }

        @Override
        public void sendSubSessionEndEvent(int sessionType, SubSession subSession,JetstreamEvent sessionEndEvent) {
            SessionizerCounterManager subCounter = counters.get(Integer.valueOf(sessionType));
            if (subSession.getEventCount() > 1) {
                long duration = subSession.getLastModifiedTime() - subSession.getCreationTime();
                mainCounter.incSubSessionDuration(duration);
                subCounter.incSubSessionDuration(duration);
            }
            if (subSession.getEventCount() == 1) {
                mainCounter.incSingleClickSubSessionCounter();
                subCounter.incSingleClickSubSessionCounter();
            }
            mainCounter.incExpiredSubSession();
            subCounter.incExpiredSubSession();
            sendToDownStream(sessionEndEvent);
        }

        private void sessionLoadedFromRemoteStore(String identifier, String uid, JetstreamEvent event, Sessionizer sessionizer) {
            PendingEventHolder holder = pendingReadEvents.remove(uid);
            if (holder == null) {
                readResponseIgnoredCounter++;
                if (event.get(Constants.EVENT_PAYLOAD_SESSION_OBJ) != null) {
                    missedReadResponseCounter++;
                }
                // it is already timed out.
                return;
            } else {
                pendingReadsCounter--;
            }

            Queue<JetstreamEvent> q = holder.getQueue();
            Session session = (Session) event.get(Constants.EVENT_PAYLOAD_SESSION_OBJ);

            if (session != null) {
                readHitsCounter++;
            }
            JetstreamEvent firstEvent = q.poll();

            if (Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT.equals(firstEvent.getEventType())) {
                if (session != null && updateRemoteSession(uid, identifier, session, sessionizer)) {
                    session = null;
                }
                firstEvent = q.poll();
                if (firstEvent == null) {
                    return;
                }
            } else if (Constants.EVENT_TYPE_SESSION_TRANSFERED_EVENT.equals(firstEvent.getEventType())) {
                if (session == null) {
                    session = reconstructSession(firstEvent, uid);
                    if (session != null && updateRemoteSession(uid, identifier, session, sessionizer)) {
                        session = null;
                    }
                } else {
                    Session transferInSession = reconstructSession(firstEvent, uid);
                    if (transferInSession != null && transferInSession.getFirstEventTimestamp() != session.getFirstEventTimestamp()) {
                        transferInSession.setIdentifier(identifier);
                        transferInSession.setType(sessionizer.getType());
                        fireSessionEndMarkerEvent(transferInSession, sessionizer);
                        // The following update will update remote store to override transferred session.
                    }
                    if (updateRemoteSession(uid, identifier, session, sessionizer)) {
                        session = null;
                    }
                }

                firstEvent = q.poll();
                if (firstEvent == null) {
                    return;
                }
            }

            SessionizationInfo info = (SessionizationInfo) firstEvent.remove(CURRENT_SESSIOIZERINFO);
            handleRawEvent(uid, identifier, session, firstEvent, sessionizer, info);

            JetstreamEvent nextEvent = q.poll();
            if (nextEvent != null) {
                session = localSessionCache.get(uid);
                while (nextEvent != null) {
                    info = (SessionizationInfo) nextEvent.remove(CURRENT_SESSIOIZERINFO);
                    updateSessionOnly(session, nextEvent, identifier, sessionizer, info);
                    nextEvent = q.poll();
                }
                updateSessionOnStore(uid, session);
            }
        }

        private EsperController selector;

        private void updateConfig(SessionizerConfig config) {
            queryTimeOutInNano = TimeUnit.NANOSECONDS.convert(config.getReadQueryTimeout(), TimeUnit.MILLISECONDS);
            enableReadOptimization = config.getEnableReadOptimization();
            List<Sessionizer> createdSessionizers = new ArrayList<Sessionizer>();
            EsperController oldSelector = selector;
            Map<Integer, Sessionizer> oldSessionTypeToSessionerMap = sessionTypeToSessionerMap;
            boolean success = false;
            EsperController newSelector = null;

            try {
                Map<Integer, Sessionizer> newSessionTypeToSessionerMap;
                Map<String, Sessionizer> newSessionizerMap;


                if (this.newCompiledConfig == null) {
                    newSessionTypeToSessionerMap = new HashMap<Integer, Sessionizer>();
                    newSessionizerMap = new HashMap<String, Sessionizer>();
                    for (SessionProfile profile : config.getMainSessionProfiles()) {
                        Sessionizer sessionizer = new Sessionizer(taskId, profile, this, config.getMaxIdleTime(), esperCounter, extensions);
                        createdSessionizers.add(sessionizer);
                        newSessionTypeToSessionerMap.put(profile.getSessionType(), sessionizer);
                        newSessionizerMap.put(profile.getName(), sessionizer);
                        if (!counters.containsKey(sessionizer.getType())) {
                            counters.put(sessionizer.getType(), new SessionizerCounterManager());
                        }
                    }

                    newSelector = new EsperController(config.getEpl(), config.getRawEventDefinition(),
                            "SessionizerController" + System.currentTimeMillis(), config.getImports(),
                            esperCounter, newSessionizerMap.keySet());
                } else {
                    newSessionTypeToSessionerMap = newCompiledConfig.newSessionTypeToSessionerMap;
                    newSessionizerMap = newCompiledConfig.newSessionizerMap;
                    newSelector = newCompiledConfig.newSelector;
                    this.newCompiledConfig = null;
                }

                success = true;
                if (!intialized) {
                    intialized = true;
                }
                selector = newSelector;
                sessionTypeToSessionerMap = newSessionTypeToSessionerMap;
                sessionizerMap = newSessionizerMap;
                counters.keySet().retainAll(newSessionTypeToSessionerMap.keySet());

                if (oldSessionTypeToSessionerMap != null) {
                    for (Sessionizer s : oldSessionTypeToSessionerMap.values()) {
                        s.destroy();
                    }
                }

                if (oldSelector != null) {
                    oldSelector.destroy();
                }
            } finally {
                if (!success) {
                    if (!intialized) {
                        intializeFailed = true;
                    }
                    for (Sessionizer s : createdSessionizers) {
                        s.destroy();
                    }
                    if (newSelector != null) {
                        newSelector.destroy();
                    }
                }
            }
        }

        private void updateDynamicAttributes(Session session, JetstreamEvent event, Sessionizer sessionizer, SessionizationInfo info) {
            long eventTs = info.getTimestamp();
            sessionizer.updateDynamicAttributes(session, event, eventTs);
        }

        private void updateExpirationTime(Session session) {
            long expirationTime = session.getExpirationTime();
            if (session.getVersion() < Integer.MAX_VALUE) {
                session.setVersion(session.getVersion() + 1);
            }
            List<SubSession> subSessions = session.getSubSessions();
            if (subSessions != null) {
                for (int i = 0, t = subSessions.size(); i < t; i++) {
                    SubSession sub = subSessions.get(i);
                    if (sub.getExpirationTime() < expirationTime) {
                        expirationTime = sub.getExpirationTime();
                    }
                }
            }
            session.setFirstExpirationTime(expirationTime);
        }

        private boolean updateRemoteSession(String uid, String identifier, Session session, Sessionizer sessionizer) {
            long currentTime = System.currentTimeMillis();
            sessionizer.checkSubSessions(identifier, session, currentTime);
            if (session.getExpirationTime() <= currentTime) {
                sessionizer.sessionEnd(identifier, session);
                RemoteStoreProvider remoteDAO = provider;
                if (remoteDAO != null) {
                    remoteDAO.delete(session, uid);
                }
                return true;
            } else {
                updateSessionOnStore(uid, session);
                return false;
            }
        }

        private void updateSessionOnly(Session session, JetstreamEvent event, String identifier, Sessionizer sessionizer, SessionizationInfo info) {
            updateDynamicAttributes(session, event, sessionizer, info);
            subSessionize(event, session, identifier, sessionizer, info);
        }

        private void updateSessionOnStore(String uid, Session session) {
            updateExpirationTime(session);
            RemoteStoreProvider remoteDAO = provider;
            if (remoteDAO != null) {
                remoteDAO.update(session, uid);
            } else {
                session.setRemoteServerInfo(null);
            }
            forceUpdateLocalStore(uid, session);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionizerProcessor.class);
    private static final String JS_EVENT_TYPE = JetstreamReservedKeys.EventType.toString();

    private static final JetstreamEvent TIMER_EVENT = new JetstreamEvent();
    private static final JetstreamEvent CONTINUE_EXPIRATION_EVENT = new JetstreamEvent();

    private static final JetstreamEvent REFRESH_EVENT = new JetstreamEvent();

    private static final JetstreamEvent RESET_EVENT = new JetstreamEvent();
    private static final JetstreamEvent CONFIG_REFRESH_EVENT = new JetstreamEvent();
    private final SessionizerErrorManager errorManager = new SessionizerErrorManager();

    // This will use a dummy loopback producer to maintain the consistent
    // hashing history to determine the cluster changes.
    private ClusterManager clusterManager;

    private RemoteStoreProvider provider;
    public void setRemoteStoreProvider(RemoteStoreProvider provider) {
        this.provider = provider;
    }

    private SessionizerRunnable[] tasks;

    private ScheduledExecutorService timer;
    private LoopbackEventProducer loopbackEventProducer;

    private final AtomicLong exceptionCounter = new AtomicLong(0);
    private final AtomicLong queueFullCounter = new AtomicLong(0);
    private final AtomicLong totalOwnershipChangedSessionReceived = new AtomicLong(0);
    private final AtomicLong totalRestoredSessionReceived = new AtomicLong(0);
    private final AtomicLong responseQueueFullCounter = new AtomicLong(0);
    private final AtomicLong refreshCounter = new AtomicLong(0); //For testing purpose to sync memory between state.
    private ExecutorService pool;
    private int warnThresHold;
    private int errorThresHold;
    private long[] expirationInfo;

    private long[] eventCounters;
    private BlockingQueue<JetstreamEvent>[] requestQueues;
    private BlockingQueue<JetstreamEvent>[] responseQueues;
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    private final AtomicBoolean timerFlag = new AtomicBoolean(true);

    private SessionizerConfig config;
    private SessionizerConfig lastConfig;
    private final List<String> addedMBeans = new ArrayList<String>();
    private final EsperSessionizerCounter esperCounter = new EsperSessionizerCounter(this);
    private final Set<String> interEventTypes;

    private static final String AFFINITY_KEY = JetstreamReservedKeys.MessageAffinityKey.toString();
    public static final String SESSIONIZER_LIST = "__sessionizers";
    private static final String CURRENT_SESSIOIZERINFO = "__currentSessionizer";

    public SessionizerProcessor() {
        interEventTypes = new HashSet<String>();
        interEventTypes.add(Constants.EVENT_TYPE_SESSION_LOAD_EVENT);
        interEventTypes.add(Constants.EVENT_TYPE_SESSION_TRANSFERED_EVENT);
        interEventTypes.add(Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void afterPropertiesSet() throws Exception {
        SessionizerConfigValidator validator = new SessionizerConfigValidator(config, config);
        lastConfig = config;
        List<String> errors = validator.validate();
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("Config error: " + errors);
        }

        addedMBeans.add(Management.addBean(getBeanName(), this));
        addedMBeans.add(Management.addBean(errorManager));
        SessionizerEsperExceptionHandlerFactory.setErrorManager(errorManager);
        int threadNum = config.getThreadNum();
        int queueSize = config.getQueueSize();
        requestQueues = new BlockingQueue[threadNum];
        responseQueues = new BlockingQueue[threadNum];
        tasks = new SessionizerRunnable[threadNum];
        expirationInfo = new long[threadNum];
        eventCounters = new long[threadNum];

        addedMBeans.add(Management.addBean(config.getBeanName(), config));
        addedMBeans.add(Management.addBean(esperCounter));


        if (provider != null) {
            provider.init(this);
            provider.start();
        }

        warnThresHold = (int) (queueSize * 0.3);
        errorThresHold = (int) (queueSize * 0.8);
        pool = Executors.newFixedThreadPool(threadNum, new NameableThreadFactory("Sessionizer"));
        for (int i = 0; i < threadNum; i++) {
            requestQueues[i] = new SingleConsumerDisruptorQueue(queueSize);
            responseQueues[i] = new SingleConsumerDisruptorQueue(queueSize * 2);
            SessionizerRunnable runnable = new SessionizerRunnable(requestQueues[i], responseQueues[i], i);
            tasks[i] = runnable;
            pool.execute(runnable);
        }

        refreshConfig();
        int sleepCount = 120;
        for (int i = 0; i < threadNum; i++) {
            while (!tasks[i].intialized && !tasks[i].intializeFailed) {
                Thread.sleep(1000);
                sleepCount --;
                if (sleepCount == 0) {
                    break;
                }
            }

            Thread.sleep(1000);

            if (sleepCount == 0 || tasks[i].intializeFailed) {
                throw new Exception("Fail to initlaize sessionizer");
            }
        }

        timer = Executors.newScheduledThreadPool(2, new NameableThreadFactory("SessionizerTimer"));
        timer.scheduleWithFixedDelay(new LocalExpirationChecker(), 1000, 1000, TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(new LeakedRemoteSessionChecker(), 60000, 60000, TimeUnit.MILLISECONDS);
    }

    public boolean checkRemoteProvider(Class<?> clazz) {
        if (this.provider == null) {
            return false;
        } else {
            return this.provider.getClass().equals(clazz);
        }
    }

    public Map<Integer, SessionizerCounterManager> collectDetailStatistics() {
        Map<Integer, SessionizerCounterManager> sumCounterMap = new HashMap<Integer, SessionizerCounterManager>();
        for (SessionizerRunnable task : tasks) {
            Map<Integer, SessionizerCounterManager> mapPerThread = task.counters;
            for (Map.Entry<Integer, SessionizerCounterManager> e : mapPerThread.entrySet()) {
                SessionizerCounterManager sumCounter = sumCounterMap.get(e.getKey());
                if (sumCounter == null) {
                    sumCounter = new SessionizerCounterManager();
                    sumCounterMap.put(e.getKey(), sumCounter);
                }
                sumCounter.sum(e.getValue());
            }
        }
        return sumCounterMap;
    }

    public void disableRemoteProvider(Class<?> clazz) {
        if (this.provider != null && this.provider.getClass().equals(clazz)) {
            enableRemoteProvider(null);
        }
    }

    public void enableRemoteProvider(RemoteStoreProvider newProvider) {
        if (this.provider == newProvider) {
            return;
        }
        RemoteStoreProvider oldProvider = this.provider;
        if (oldProvider == newProvider) {
            return;
        } else if (oldProvider != null) {
            this.provider = null;
            oldProvider.stop();
        }
        if (newProvider != null) {
            try {
                newProvider.init(this);
                newProvider.start();
                this.provider = newProvider;
            } catch (RuntimeException ex) {
                newProvider.stop();
                throw ex;
            } catch (Error ex) {
                newProvider.stop();
                throw ex;
            }
        }
        refresh();
    }

    @ManagedAttribute
    public long getActiveSessionNum() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getSize();
        }
        return size;
    }

    @ManagedAttribute
    public long getAsyncReadFailureNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.asyncReadFailure;
        }
        return num;
    }

    @ManagedAttribute
    public long getAverageSessionDuration() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getSessionDurationTotal();
        }
        long sessionNum = getExpiredSessionNum() - getSingleClickSessionNum();
        if (sessionNum > 0) {
            return num / sessionNum;
        } else {
            return 0;
        }
    }

    @ManagedAttribute
    public long getAverageSessionLagTime() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getSessionLagTime();
        }

        long sessionNum = getCreatedSessionNum();
        if (sessionNum > 0) {
            return num / sessionNum;
        } else {
            return 0;
        }
    }

    @ManagedAttribute
    public long getAverageSubSessionDuration() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getSubSessionDurationTotal();
        }
        long sessionNum = getExpiredSubSessionNum() - getSingleClickSubSessionNum();
        if (sessionNum > 0) {
            return num / sessionNum;
        } else {
            return 0;
        }
    }

    @ManagedAttribute
    public long getCreatedSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getCreatedSession();
        }
        return num;
    }

    @ManagedAttribute
    public long getCreatedSubSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getCreatedSubSession();
        }
        return num;
    }

    @ManagedAttribute
    public String getEventDistributions() {
        return Arrays.toString(eventCounters);
    }

    @ManagedAttribute
    public long getEventSentInLastSecond() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.eventSentInLastSecond;
        }
        return num;
    }

    @ManagedAttribute
    public long getEvictedSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.sessionEvitCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getEvictedSubSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.subSessionEvitCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getExceptionNum() {
        return exceptionCounter.get();
    }

    @ManagedAttribute
    public long getExpirationDelay() {
        long currentTime = System.currentTimeMillis();
        long x = 0;
        for (int i = 0; i < expirationInfo.length; i++) {
            x += (currentTime - expirationInfo[i]);
        }
        return x / expirationInfo.length;
    }

    @ManagedAttribute
    public long getExpiredSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getExpiredSession();
        }
        return num;
    }

    @ManagedAttribute
    public long getExpiredSubSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getExpiredSubSession();
        }
        return num;
    }


    @ManagedAttribute
    public long getInvalidInternalEvents() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.invalidInternalEventCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getInvalidRawEvents() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.invalidRawEventCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getLongSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getLongSessionCounter();
        }
        return num;
    }

    @ManagedAttribute
    public int getMaxEventCount() {
        int size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(task.maxEventCount, size);
        }
        return size;
    }

    @ManagedAttribute
    public long getMaxEventInterval() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(task.maxEventInterval, size);
        }
        return size;
    }

    @ManagedAttribute
    public long getMaxEventSentInSecond() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.maxEventsSentPerSecond;
        }
        return num;
    }

    @ManagedAttribute
    public String getMaxEventSentInSecondDetail() {
        StringBuffer buf = new StringBuffer();
        for (SessionizerRunnable task : tasks) {
            buf.append(task.maxEventsSentPerSecond);
            buf.append(",");
        }
        return buf.toString();
    }
    @ManagedAttribute
    public long getMaxSessionDuration() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(task.maxSessionDuration, size);
        }
        return size;
    }

    @ManagedAttribute
    public long getMaxSessionExpirationDelay() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(task.maxSessionExpirationDelay, size);
        }
        return size;
    }

    @ManagedAttribute
    public long getMaxSessionLagTime() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(task.maxSessionLagTime, size);
        }
        return size;
    }

    @ManagedAttribute
    public int getMaxSessionSize() {
        int size = 0;
        for (SessionizerRunnable task : tasks) {
            size = Math.max(size, task.localSessionCache.getMaxItemSize());
        }
        return size;
    }

    @ManagedAttribute
    public long getOffHeapFreeMemory() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getFreeMemory();
        }
        return size;
    }

    @ManagedAttribute
    public long getOffHeapMaxMemory() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getMaxMemory();
        }
        return size;
    }

    @ManagedAttribute
    public long getOffHeapOOMErrorCount() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getOOMErrorCount();
        }
        return size;
    }

    @ManagedAttribute
    public long getOffHeapReservedMemory() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getReservedMemory();
        }
        return size;
    }

    @ManagedAttribute
    public long getOffHeapUsedMemory() {
        long size = 0;
        for (SessionizerRunnable task : tasks) {
            size += task.localSessionCache.getUsedMemory();
        }
        return size;
    }

    @ManagedAttribute
    public long getOndemandExpiredSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.onDemandExpiredSessionCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getPassthroughEvents() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.bypassEventCounter;
        }
        return num;
    }

    @Override
    @ManagedAttribute
    public int getPendingEvents() {
        int num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.requestQueue.size();
        }
        return num;
    }

    @ManagedAttribute
    public long getPendingReads() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.pendingReadsCounter;
        }
        return num;
    }

    private int getQueueIndex(String key) {
        int hash = key.hashCode();
        if (hash == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(hash) % requestQueues.length;
    }

    @ManagedAttribute
    public long getReadHitNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.readHitsCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getReadResponseIngoredNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.readResponseIgnoredCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getReadResponseMissedNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.missedReadResponseCounter;
        }
        return num;
    }

    @ManagedAttribute
    public long getReadTimeoutNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.readTimeoutCounter;
        }
        return num;
    }

    //For testing purpose
    public long getRefreshCount() {
        return refreshCounter.get();
    }

    @ManagedAttribute
    public long getShortLiveSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getShortSessionCounter();
        }
        return num;
    }

    @ManagedAttribute
    public long getSingleClickSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getSingleClickSessionCounter();
        }
        return num;
    }

    @ManagedAttribute
    public long getSingleClickSubSessionNum() {
        long num = 0;
        for (SessionizerRunnable task : tasks) {
            num += task.mainCounter.getSingleClickSubSessionCounter();
        }
        return num;
    }

    @ManagedAttribute
    public long getTotalReponseQueueFullDropped() {
        return responseQueueFullCounter.get();
    }

    @ManagedAttribute
    public long getTotalErrorCount() {
        return errorManager.getTotalErrorCount();
    }

    @ManagedAttribute
    public long getTotalOwnershipChangedSessionReceived() {
        return totalOwnershipChangedSessionReceived.get();
    }

    @ManagedAttribute
    public long getTotalOwnershipChangedSessionSent() {
        return loopbackEventProducer.getTotalOwnershipChangedSessionSent();
    }

    @ManagedAttribute
    public long getTotalRestoredSessionReceived() {
        return totalRestoredSessionReceived.get();
    }

    @ManagedAttribute
    public long getTotalRestoredSessionSent() {
        return loopbackEventProducer.getTotalRestoredSessionSent();
    }

    @ManagedAttribute
    public long getTotalQueueFullDrop() {
        return queueFullCounter.get();
    }

    public int identifierType(String uid) {
        return Integer.valueOf(uid.substring(0, uid.indexOf(':')));
    }

    public String identifierValue(String uid) {
        return uid.substring(uid.indexOf(':') + 1);
    }

    @ManagedAttribute
    public boolean isClusterLeader() {
        return clusterManager.isLeader();
    }

    public boolean isLiveConsumer(long clientId) {
        return clusterManager.isHostLive(clientId);
    }

    @Override
    public void remoteSessionLoaded(String uid, Session session, String ak) {
        JetstreamEvent event = new JetstreamEvent();
        event.setEventType(Constants.EVENT_TYPE_SESSION_LOAD_EVENT);
        int type = identifierType(uid);
        event.put(Constants.EVENT_PAYLOAD_SESSION_UNIQUEID, uid);
        event.put(Constants.EVENT_PAYLOAD_SESSION_OBJ, session);
        event.put(Constants.EVENT_PAYLOAD_SESSION_TYPE, Integer.valueOf(type));

        if (session != null) {
            session.setType(type);
            session.setIdentifier(identifierValue(uid));
        }

        int queueIndex = getQueueIndex(ak);
        try {
            responseQueues[queueIndex].put(event);
        } catch (InterruptedException e) {
            responseQueueFullCounter.incrementAndGet();
        }
    }

    @Override
    public void remoteSessionExpired(String uid, String ak, byte[] payload, byte[] metadata) {
        JetstreamEvent jsEvent = new JetstreamEvent();
        int type = identifierType(uid);
        jsEvent.setEventType(Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_UNIQUEID, uid);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_TYPE, Integer.valueOf(type));
        jsEvent.put(AFFINITY_KEY, ak);

        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_PAYLOAD, payload);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_METADATA, metadata);
        
            
        int queueIndex = getQueueIndex(ak);
        try {
            responseQueues[queueIndex].put(jsEvent);
        } catch (InterruptedException e) {
            responseQueueFullCounter.incrementAndGet();
        }
    }

    @Override
    @ManagedOperation
    public void pause() {
        if (isPaused()) {
            return;
        }

        changeState(ProcessorOperationState.PAUSE);
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {

            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

            SessionizerConfig newBean = (SessionizerConfig) (bcInfo.getApplicationContext().getBean(config.getBeanName()));
            if (newBean != lastConfig) {
                lastConfig = newBean;
                SessionizerConfigValidator validator = new SessionizerConfigValidator(config, newBean);
                List<String> errors = validator.validate();
                if (!errors.isEmpty()) {
                    throw new IllegalArgumentException("Config error: " + errors);
                }

                int readQueryTimeout = config.getReadQueryTimeout();
                boolean enableReadOptimization = config.getEnableReadOptimization();
                int maxIdleTime = config.getMaxIdleTime();
                List<SessionProfile> mainSessionProfiles = config.getMainSessionProfiles();
                EPL epl = config.getEpl();
                List<String> imports = config.getImports();
                EsperDeclaredEvents rawEventDefinition = config.getRawEventDefinition();

                config.setReadQueryTimeout(newBean.getReadQueryTimeout());
                config.setEnableReadOptimization(newBean.getEnableReadOptimization());
                config.setMaxIdleTime(newBean.getMaxIdleTime());
                config.setMainSessionProfiles(newBean.getMainSessionProfiles());
                config.setEpl(newBean.getEpl());
                config.setImports(newBean.getImports());
                config.setRawEventDefinition(newBean.getRawEventDefinition());

                boolean isSuccess = false;

                try {
                    for (SessionizerRunnable task : tasks) {
                        task.newCompiledConfig  = new CompiledConfig(config, task);
                    }
                    isSuccess = true;
                } finally {
                    if (!isSuccess) {
                        for (SessionizerRunnable task : tasks) {
                            if (task.newCompiledConfig != null) {
                                task.newCompiledConfig.destroy();
                                task.newCompiledConfig  = null;
                            }
                        }
                        config.setReadQueryTimeout(readQueryTimeout);
                        config.setEnableReadOptimization(enableReadOptimization);
                        config.setMaxIdleTime(maxIdleTime);
                        config.setMainSessionProfiles(mainSessionProfiles);
                        config.setEpl(epl);
                        config.setImports(imports);
                        config.setRawEventDefinition(rawEventDefinition);
                    }
                }
                refreshConfig();
            }
        }
    }

    public Session reconstructSession(JetstreamEvent event, String uid) {
        Session session = new Session();
        byte[] payload = (byte[]) event.get(Constants.EVENT_PAYLOAD_SESSION_PAYLOAD);
        if (payload == null) {
            return null;
        }
        byte[] metaData = (byte[]) event.get(Constants.EVENT_PAYLOAD_SESSION_METADATA);
        if (metaData == null) {
            return null;
        }
        if (!BinaryFormatSerializer.getInstance().setSessionPayload(session, ByteBuffer.wrap(payload))) {
            return null;
        }
        if (!BinaryFormatSerializer.getInstance().setSessionMetadata(session, ByteBuffer.wrap(metaData))) {
            return null;
        }

        session.setType(Integer.valueOf(uid.substring(0, uid.indexOf(':'))));
        session.setIdentifier(uid.substring(uid.indexOf(':') + 1));
        return session;
    }

    public void refresh() {
        for (int i = 0; i < requestQueues.length; i++) {
            requestQueues[i].offer(REFRESH_EVENT);
        }
    }

    public void refreshConfig() {
        for (int i = 0; i < requestQueues.length; i++) {
            requestQueues[i].offer(CONFIG_REFRESH_EVENT);
        }
    }

    @ManagedOperation
    public void resetHighWaterMark() {
        for (int i = 0; i < requestQueues.length; i++) {
            requestQueues[i].offer(RESET_EVENT);
        }
    }

    @Override
    @ManagedOperation
    public void resume() {
        if (isPaused()) {
            changeState(ProcessorOperationState.RESUME);
        }
    }

    public void sendAlert(String name, String msg, AlertListener.AlertStrength strength) {
        if (this.getAlertListener() != null) {
            getAlertListener().sendAlert(name, msg, strength);
        }
    }
    @Override
    public void sendEvent(JetstreamEvent inputEevent) throws EventException {
        // If remote store failed, just ignore and use local cache.
        // Exception will be captured on messaging layer, suppose no exception
        // throw on this code.

        if (isPaused() || shutdownFlag.get()) {
            super.incrementEventDroppedCounter();
            return;
        }

        JetstreamEvent event = inputEevent;
        boolean isRawEvent = !interEventTypes.contains(event.getEventType());
        String ak = (String) event.get(AFFINITY_KEY);
        if (ak == null) {
            ak = "";
        }

        int queueIndex = getQueueIndex(ak);
        if (isRawEvent) {
            super.incrementEventRecievedCounter();
            try {
                event = event.clone();
            } catch (CloneNotSupportedException e) {
                //do nothing
            }
        }

        if (!requestQueues[queueIndex].offer(event)) {
            queueFullCounter.incrementAndGet();
            if (isRawEvent && this.getAdviceListener() != null) {
                this.getAdviceListener().retry(event,  RetryEventCode.MSG_RETRY, "Fail to enqueue event");
            } else {
                super.incrementEventDroppedCounter();
            }
        }
    }

    private void sendSsnzEvent(JetstreamEvent event) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Send ssnz event: {}", event);
        }
        super.fireSendEvent(event);
    }

    public void setClusterManager(ClusterManager manager) {
        this.clusterManager = manager;
        if (manager != null) {
            manager.setSessionizer(this);
        }
    }

    public void setConfig(SessionizerConfig config) {
        this.config = config;
    }

    public void setLoopbackEventProducer(LoopbackEventProducer producer) {
        this.loopbackEventProducer = producer;
    }

    @Override
    public void shutDown() {
        if (shutdownFlag.compareAndSet(false, true)) {
            LOGGER.warn("Gracefully stop sessionizer");
            try {
                timer.shutdownNow();
                for (SessionizerRunnable task : tasks) {
                    task.running = false;
                }
                pool.shutdownNow();

                try {
                    //Wait 5 seconds for flushing pending events
                    pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }

                if (!pool.isTerminated()) {
                    LOGGER.error("Timed out when shutdown sessionizer thread pool");
                }

                long begin = System.currentTimeMillis();
                while ((System.currentTimeMillis() - begin) < 10 * 1000) {
                    if (timer.isTerminated() && pool.isTerminated()) {
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                RemoteStoreProvider l = provider;
                if (l != null) {
                    l.stop();
                }

                for (String s : addedMBeans) {
                    Management.removeBeanOrFolder(s);
                }
                addedMBeans.clear();
            } catch (Throwable ex) {
                //ignore
            }
            LOGGER.warn("Sessionizer gracefully stopped.");
        }
        
        LOGGER.warn("final events sent = " + getTotalEventsSent() +
                "final total events dropped =" + getTotalEventsDropped() +
                "final total events received =" + getTotalEventsReceived() +
                "current pending events =" + getPendingEvents());
    }

    public void shutdownTimer() {
        if (timerFlag.compareAndSet(true, false)) {
            LOGGER.warn("Stop sessionizer timer");
        }
    }

    @Override
    public String toString() {
        return getBeanName();
    }

    public long getMaxIdleTime() {
        return config.getMaxIdleTime();
    }

    @Hidden
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    @Hidden
    public LoopbackEventProducer getFailoverEventProducer() {
        return loopbackEventProducer;
    }

    @Hidden
    public SessionizerConfig getConfig() {
        return config;
    }

    @Hidden
    public SessionizerErrorManager getErrorManager() {
        return errorManager;
    }

    @Override
    public void registerError(Throwable ex, ErrorType type) {
        errorManager.registerError(ex, type);

    }

    @Override
    public void sendRemoteStoreAlert(String message, AlertStrength severity) {
        this.sendAlert("RemoteStore", message, severity);

    }

    @Override
    public void sendExpirationCheckEvent(String uid, String ak) {
        JetstreamEvent jsEvent = new JetstreamEvent();
        int type = identifierType(uid);
        jsEvent.setEventType(Constants.EVENT_TYPE_SESSION_EXPIRED_EVENT);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_UNIQUEID, uid);
        jsEvent.put(Constants.EVENT_PAYLOAD_SESSION_TYPE, Integer.valueOf(type));
        jsEvent.put(AFFINITY_KEY, ak);

        this.loopbackEventProducer.sendExpirationCheckEvent(jsEvent);
    }

    @Override
    public void registerError(String category, ErrorType type, String message) {
        errorManager.registerError(category, type, message);

    }
}
