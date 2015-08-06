/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.pulsar.sessionizer.config.SessionProfile;
import com.ebay.pulsar.sessionizer.config.SubSessionProfile;
import com.ebay.pulsar.sessionizer.esper.impl.EsperSessionizer;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;
import com.ebay.pulsar.sessionizer.spi.SessionizerExtension;

/**
 * Sessionizer impl.
 * 
 * @author xingwang
 *
 */
public class Sessionizer {
    private static final String AFFINITY_KEY = JetstreamReservedKeys.MessageAffinityKey.toString();

    private final SessionProfile mainSessionProfile;
    private final EsperSessionizer mainEsperSessionizer;
    private final Map<String, SubSessionProfile> subSessionProfileMap = new HashMap<String, SubSessionProfile>();
    private final Map<String, EsperSessionizer> subEsperSessionizerMap = new HashMap<String, EsperSessionizer>();
    private final int maxSessionIdleTime;
    private final int maxActiveTime;
    private final int defaultTtl;
    private final SsnzEventSender eventSender;

    public Sessionizer(int id, SessionProfile mainSessionProfile, SsnzEventSender eventSender, int maxSessionIdleTime, EsperSessionizerCounter esperCounter,
            List<SessionizerExtension> extensions) {
        this.eventSender = eventSender;
        this.mainSessionProfile = mainSessionProfile;
        this.maxSessionIdleTime = maxSessionIdleTime;
        this.maxActiveTime = mainSessionProfile.getMaxActiveTime();
        this.defaultTtl = mainSessionProfile.getDefaultTtl();
        if (mainSessionProfile.getSubSessionProfiles() != null) {
            for (SubSessionProfile profile : mainSessionProfile.getSubSessionProfiles()) {
                subSessionProfileMap.put(profile.getName(), profile);
            }
        }

        List<EsperSessionizer> createdSessionizers = new ArrayList<EsperSessionizer>();
        boolean success = false;
        try {
            if (mainSessionProfile.getEpl() != null) {
                mainEsperSessionizer = new EsperSessionizer(id, mainSessionProfile.getEpl(),
                        mainSessionProfile.getRawEventDefinition(),
                        "MainEsperSessionizer" + mainSessionProfile.getName() + System.currentTimeMillis(),
                        mainSessionProfile.getImports(), esperCounter, true, subSessionProfileMap.keySet(), extensions, maxSessionIdleTime);
                createdSessionizers.add(mainEsperSessionizer);
            } else {
                mainEsperSessionizer = null;
            }

            for (SubSessionProfile profile : subSessionProfileMap.values()) {
                if (profile.getEpl() != null) {
                    EsperSessionizer subEsperSessionizer = new EsperSessionizer(id, profile.getEpl(),
                            mainSessionProfile.getRawEventDefinition(), "SubEsperSessionizer" + profile.getName()
                            + System.currentTimeMillis(), profile.getImports(), esperCounter, false, null, extensions, maxSessionIdleTime);
                    subEsperSessionizerMap.put(profile.getName(), subEsperSessionizer);
                    createdSessionizers.add(subEsperSessionizer);
                }
            }
            success = true;
        } finally {
            if (!success) {
                for (EsperSessionizer s : createdSessionizers) {
                    s.destroy();
                }
            }
        }
    }


    public void checkSubSessions(String mainIdentifier, Session session, long currentTimestamp) {
        if (session == null) {
            return;
        }
        List<SubSession> subSessions = session.getSubSessions();
        if (subSessions == null) {
            return;
        }
        for (int i = subSessions.size() - 1; i >= 0; i--) {
            SubSession subSession = subSessions.get(i);

            if (subSession.getExpirationTime() <= currentTimestamp) {
                subSessionEnd(mainIdentifier, session, subSession);
                subSessions.remove(i);
            }
        }
    }

    private String concatTimestamp(String prefix, long timestamp) {
        StringBuilder builder = new StringBuilder(prefix.length() + 16);
        builder.append(prefix);
        String x = Long.toHexString(timestamp);
        for (int i = 16 - x.length(); i > 0; i--) {
            builder.append('0');
        }
        builder.append(x);
        return builder.toString();
    }

    private String concatTimestamp(String prefix, String prefix2, long timestamp) {
        StringBuilder builder = new StringBuilder(prefix.length() + prefix2.length() + 16);
        builder.append(prefix);
        builder.append(prefix2);
        String x = Long.toHexString(timestamp);
        for (int i = 16 - x.length(); i > 0; i--) {
            builder.append('0');
        }
        builder.append(x);
        return builder.toString();
    }

    public Session createNewSession(String identifier, JetstreamEvent event, long currentTimestamp, SessionizationInfo info) {
        Session session = new Session();
        session.setIdentifier(identifier);
        session.setAffinityKey((String) event.get(AFFINITY_KEY));
        if (session.getAffinityKey() == null) {
            session.setAffinityKey(identifier);
        }
        session.setType(getType());
        if (info.getTtl() > 0) {
            session.setTtl(Math.min(info.getTtl(), maxSessionIdleTime));
        } else {
            session.setTtl(Math.min(this.defaultTtl, maxSessionIdleTime));
        }

        long eventTimestamp = info.getTimestamp();
        session.setFirstEventTimestamp(eventTimestamp);

        session.setCreationTime(currentTimestamp);
        session.setLastModifiedTime(currentTimestamp);
        session.setExpirationTime(currentTimestamp + session.getTtl());
        session.setEventCount(0);
        initSessionData(event, session);

        if (mainSessionProfile.getBeginMarker() != null) {
            sessionBegin(session);
        }

        return session;
    }


    public void destroy() {
        if (mainEsperSessionizer != null) {
            mainEsperSessionizer.destroy();
        }

        for (EsperSessionizer p : subEsperSessionizerMap.values()) {
            p.destroy();
        }
    }

    public int getMaxActiveTime() {
        return maxActiveTime;
    }

    public int getType() {
        return mainSessionProfile.getSessionType();
    }

    public String getUniqueSessionIdentifier(String identifier) {
        return mainSessionProfile.getSessionType() + ":" + identifier;
    }

    public void handleSubSessionLogic(JetstreamEvent event, Session session, String mainIdentifier, long eventTimestamp) {
        Map<String, SessionizationInfo> subSessionizerMap = session.getSubSessionizerMap();
        if (subSessionizerMap != null) {
            try {
                for (Map.Entry<String, SessionizationInfo> e : subSessionizerMap.entrySet()) {
                    String name = e.getKey();
                    SessionizationInfo info = e.getValue();
                    processSubSession(event, session, mainIdentifier, info, subSessionProfileMap.get(name), eventTimestamp);
                }
            } finally {
                session.clearSubsessionizerInfo();
            }
        }
    }

    private void initSessionData(JetstreamEvent event, Session session) {
        long eventTimestamp = session.getFirstEventTimestamp();

        updateDynamicAttributes(session, event, eventTimestamp);
    }

    private void processSubSession(JetstreamEvent event, Session session, String mainIdentifier, SessionizationInfo info, SubSessionProfile profile, long eventTimestamp) {

        String subSessionIdenfifier = info.getIdentifier();
        SubSession subSession = session.getSubSession(subSessionIdenfifier, profile.getName());

        long currentTime = session.getLastModifiedTime();
        if (subSession == null) {
            subSession = new SubSession();
            subSession.setIdentifier(subSessionIdenfifier);
            subSession.setName(profile.getName());
            subSession.setCreationTime(currentTime);
            subSession.setEventCount(1);
            if (info.getTtl() > 0) {
                subSession.setTtl(Math.min(info.getTtl(), maxSessionIdleTime));
            } else {
                subSession.setTtl(Math.min(profile.getDefaultTtl(), maxSessionIdleTime));
            }
            if (subSession.getTtl() > session.getTtl()) {
                subSession.setTtl(session.getTtl());
            }
            subSession.setFirstEventTimestamp(eventTimestamp);

            subSession.setLastModifiedTime(currentTime);
            subSession.setExpirationTime(currentTime + subSession.getTtl());
            subSession.setSessionId(concatTimestamp(session.getIdentifier(), subSessionIdenfifier, subSession.getFirstEventTimestamp()));
            session.addSubSession(subSession);
        } else if (subSession.getExpirationTime() < session.getLastModifiedTime()) {
            subSessionEnd(mainIdentifier, session, subSession);
            subSession.setCreationTime(currentTime);
            subSession.setEventCount(1);
            if (info.getTtl() > 0) {
                subSession.setTtl(Math.min(info.getTtl(), maxSessionIdleTime));
            } else {
                subSession.setTtl(Math.min(profile.getDefaultTtl(), maxSessionIdleTime));
            }
            if (subSession.getTtl() > session.getTtl()) {
                subSession.setTtl(session.getTtl());
            }
            subSession.setFirstEventTimestamp(eventTimestamp);

            subSession.setLastModifiedTime(currentTime);
            subSession.setExpirationTime(currentTime + subSession.getTtl());
            subSession.setSessionId(concatTimestamp(session.getIdentifier(), subSessionIdenfifier, subSession.getFirstEventTimestamp()));
            if (subSession.getInitialAttributes() != null) {
                subSession.getInitialAttributes().clear();
            }
            if (subSession.getDynamicAttributes() != null) {
                subSession.getDynamicAttributes().clear();
            }
        } else {
            subSession.setSessionId(concatTimestamp(session.getIdentifier(), subSessionIdenfifier, subSession.getFirstEventTimestamp()));
            if (subSession.getEventCount() < Integer.MAX_VALUE) {
                subSession.setEventCount(subSession.getEventCount() + 1);
            }
            subSession.setExpirationTime(currentTime + subSession.getTtl());
            subSession.setLastModifiedTime(currentTime);
        }

        if (session.getExpirationTime() < subSession.getExpirationTime()) {
            session.setExpirationTime(subSession.getExpirationTime());
        }

        EsperSessionizer subEsperSessionizer = subEsperSessionizerMap.get(profile.getName());
        if (subEsperSessionizer != null  && subEsperSessionizer.isEventSupported(event)) {
            subEsperSessionizer.process(subSession, session, event);
        }

        if (subSession.getEventCount() == 1 && profile.getBeginMarker() != null) {
            JetstreamEvent sessionBeginEvent = new JetstreamEvent();
            sessionBeginEvent.setEventType(profile.getBeginMarker());
            Map<String, Object> initialAttributes = subSession.getInitialAttributes();
            if (initialAttributes != null) {
                sessionBeginEvent.putAll(initialAttributes);
            }

            if (profile.getSessionIdKey() != null) {
                sessionBeginEvent.put(profile.getSessionIdKey(), subSession.getSessionId());
            }

            if (mainSessionProfile.getSessionStartTimestampKey() != null) {
                sessionBeginEvent.put(mainSessionProfile.getSessionStartTimestampKey(), subSession.getCreationTime());
            }

            if (subEsperSessionizer != null && subEsperSessionizer.isEventSupported(sessionBeginEvent)) {
                subEsperSessionizer.process(subSession, session, sessionBeginEvent);
            }
            eventSender.sendSubSessionBeginEvent(mainSessionProfile.getSessionType(), subSession, sessionBeginEvent);
        }
    }

    public Session renewExpiredSession(String identifier, Session session, JetstreamEvent event, long currentTimestamp, SessionizationInfo info) {
        terminateSubSessions(identifier, session);
        sessionEnd(identifier, session);
        if (info.getTtl() > 0) {
            session.setTtl(Math.min(info.getTtl(), maxSessionIdleTime));
        } else {
            session.setTtl(Math.min(this.defaultTtl, maxSessionIdleTime));
        }

        session.setCreationTime(currentTimestamp);
        session.setAffinityKey((String) event.get(AFFINITY_KEY));
        if (session.getAffinityKey() == null) {
            session.setAffinityKey(identifier);
        }
        long eventTimestamp = info.getTimestamp();
        session.setFirstEventTimestamp(eventTimestamp);
        session.setLastModifiedTime(currentTimestamp);
        session.setExpirationTime(currentTimestamp + session.getTtl());
        if (session.getInitialAttributes() != null) {
            session.getInitialAttributes().clear();
        }
        if (session.getDynamicAttributes() != null) {
            session.getDynamicAttributes().clear();
        }

        session.setSessionId(null);
        session.setEventCount(0);
        initSessionData(event, session);
        if (mainSessionProfile.getBeginMarker() != null) {
            sessionBegin(session);
        }
        return session;

    }

    private void sessionBegin(Session session) {
        JetstreamEvent sessionBeginEvent = new JetstreamEvent();
        sessionBeginEvent.setEventType(mainSessionProfile.getBeginMarker());
        Map<String, Object> initialAttributes = session.getInitialAttributes();
        if (initialAttributes != null) {
            sessionBeginEvent.putAll(initialAttributes);
        }

        if (mainSessionProfile.getSessionIdKey() != null) {
            sessionBeginEvent.put(mainSessionProfile.getSessionIdKey(), session.getSessionId());
        }

        if (mainSessionProfile.getSessionStartTimestampKey() != null) {
            sessionBeginEvent.put(mainSessionProfile.getSessionStartTimestampKey(), session.getCreationTime());
        }

        if (mainEsperSessionizer != null && mainEsperSessionizer.isEventSupported(sessionBeginEvent)) {
            mainEsperSessionizer.process(session, sessionBeginEvent);
        }
        eventSender.sendSessionBeginEvent(mainSessionProfile.getSessionType(), session, sessionBeginEvent);
    }

    public void sessionEnd(String identifier, Session session) {
        if (mainSessionProfile.getEndMarker() == null) {
            return;
        }
        JetstreamEvent sessionEndEvent = new JetstreamEvent();
        sessionEndEvent.setEventType(mainSessionProfile.getEndMarker());
        Map<String, Object> initialAttributes = session.getInitialAttributes();
        if (initialAttributes != null) {
            sessionEndEvent.putAll(initialAttributes);
        }

        session.setSessionId(concatTimestamp(identifier, session.getFirstEventTimestamp()));

        if (mainSessionProfile.getSessionIdKey() != null) {
            sessionEndEvent.put(mainSessionProfile.getSessionIdKey(), session.getSessionId());
        }

        if (mainSessionProfile.getSessionStartTimestampKey() != null) {
            sessionEndEvent.put(mainSessionProfile.getSessionStartTimestampKey(), session.getCreationTime());
        }

        if (mainSessionProfile.getSessionEndTimestampKey() != null) {
            sessionEndEvent.put(mainSessionProfile.getSessionEndTimestampKey(), session.getLastModifiedTime());
        }

        if (mainSessionProfile.getSessionDurationKey() != null) {
            sessionEndEvent.put(mainSessionProfile.getSessionDurationKey(), session.getDuration());
        }

        if (mainSessionProfile.getSessionEventCountKey() != null) {
            sessionEndEvent.put(mainSessionProfile.getSessionEventCountKey(), session.getEventCount());
        }

        if (mainEsperSessionizer != null && mainEsperSessionizer.isEventSupported(sessionEndEvent)) {
            mainEsperSessionizer.process(session, sessionEndEvent);
        }
        eventSender.sendSessionEndEvent(mainSessionProfile.getSessionType(), session, sessionEndEvent);
    }

    public void subSessionEnd(String mainIdentifier, Session mainSession, SubSession subSession) {
        SubSessionProfile profile = subSessionProfileMap.get(subSession.getName());
        if (profile != null && profile.getEndMarker() != null) {
            String subSessionId = concatTimestamp(mainIdentifier, subSession.getIdentifier(), subSession.getFirstEventTimestamp());
            subSession.setSessionId(subSessionId);
            JetstreamEvent sessionEndEvent = new JetstreamEvent();
            sessionEndEvent.setEventType(profile.getEndMarker());

            if (subSession.getInitialAttributes() != null) {
                sessionEndEvent.putAll(subSession.getInitialAttributes());
            }

            if (profile.getSessionIdKey() != null) {
                sessionEndEvent.put(profile.getSessionIdKey(), subSession.getSessionId());
            }

            if (mainSessionProfile.getSessionStartTimestampKey() != null) {
                sessionEndEvent.put(mainSessionProfile.getSessionStartTimestampKey(), subSession.getCreationTime());
            }

            if (mainSessionProfile.getSessionEndTimestampKey() != null) {
                sessionEndEvent.put(mainSessionProfile.getSessionEndTimestampKey(), subSession.getLastModifiedTime());
            }

            if (mainSessionProfile.getSessionDurationKey() != null) {
                sessionEndEvent.put(mainSessionProfile.getSessionDurationKey(), subSession.getDuration());
            }

            if (mainSessionProfile.getSessionEventCountKey() != null) {
                sessionEndEvent.put(mainSessionProfile.getSessionEventCountKey(), subSession.getEventCount());
            }

            EsperSessionizer subEsperSessionizer = subEsperSessionizerMap.get(profile.getName());
            if (subEsperSessionizer != null && subEsperSessionizer.isEventSupported(sessionEndEvent)) {
                subEsperSessionizer.process(subSession, mainSession, sessionEndEvent);
            }

            eventSender.sendSubSessionEndEvent(mainSessionProfile.getSessionType(), subSession, sessionEndEvent);
        }
    }

    private void terminateSubSessions(String mainIdentifier, Session session) {
        List<SubSession> subSessions = session.getSubSessions();
        if (subSessions == null) {
            return;
        }
        for (int i = 0, t = subSessions.size(); i < t; i++) {
            SubSession subSession = subSessions.get(i);
            subSessionEnd(mainIdentifier, session, subSession);
        }
        subSessions.clear();
    }


    public void updateDynamicAttributes(Session session, JetstreamEvent event, long eventTs) {
        if (session.getEventCount() < Integer.MAX_VALUE) {
            session.setEventCount(session.getEventCount() + 1);
        }

        if (session.getBotType() > 0 && session.getBotEventCount() < Integer.MAX_VALUE) {
            session.setBotEventCount(session.getBotEventCount() + 1);
        }

        if (session.getSessionId() == null) {
            session.setSessionId(concatTimestamp(session.getIdentifier(), session.getFirstEventTimestamp()));
        }
        if (mainEsperSessionizer != null && mainEsperSessionizer.isEventSupported(event)) {
            mainEsperSessionizer.process(session, event);
        }
    }

    public void updateSessionId(Session session) {
        session.setSessionId(concatTimestamp(session.getIdentifier(), session.getFirstEventTimestamp()));
    }
}
