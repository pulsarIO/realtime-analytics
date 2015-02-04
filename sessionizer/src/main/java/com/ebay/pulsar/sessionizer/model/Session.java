/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.pulsar.sessionizer.impl.SessionizationInfo;

/**
 * The POJO definition of the Session.
 * 
 * A session will be identified by a guid and first event timestamp.
 * 
 * @author xingwang
 */
public class Session extends AbstractSession {
    private static final int INITIAL_SUB_SESSION_NUMBER = 4;

    private int type;
    private int version;
    private List<SubSession> subSessions;
    private int botType;
    private int botEventCount;
    private long firstExpirationTime;
    private long metadataLastModifiedTime;
    private String remoteServerInfo;

    private String affinityKey;

    // Temp attributes
    private Map<String, SessionizationInfo> subSessionizerMap;

    /**
     * Add sub session into main session.
     * 
     * @param sub
     */
    public void addSubSession(SubSession sub) {
        if (subSessions == null) {
            subSessions = new ArrayList<SubSession>(INITIAL_SUB_SESSION_NUMBER);
            subSessions.add(sub);
        } else {
            subSessions.add(sub);
        }
    }

    /**
     * Add subsessionizer hint to main session.
     * 
     * Temp persist the hint.
     * 
     * @param key
     * @param info
     */
    public void addSubSessionizerInfo(String key, SessionizationInfo info) {
        if (this.subSessionizerMap == null) {
            subSessionizerMap = new HashMap<String, SessionizationInfo>();
        }

        subSessionizerMap.put(key, info);
    }

    /**
     * Remote all subsessionizer hints.
     */
    public void clearSubsessionizerInfo() {
        subSessionizerMap = null;
    }

    /**
     * The affinity key used for this session.
     * 
     * @return
     */
    public String getAffinityKey() {
        return affinityKey;
    }

    /**
     * Bot event count.
     * 
     * @return
     */
    public int getBotEventCount() {
        return botEventCount;
    }

    /**
     * Bot type of the session.
     * 
     * 0 means non-bot session.
     * 
     * @return
     */
    public int getBotType() {
        return botType;
    }

    /**
     * The earliest expiration time of the subsessions and this session.
     * 
     * @return
     */
    public long getFirstExpirationTime() {
        return firstExpirationTime;
    }

    /**
     * The metadata last modified time.
     * 
     * @return
     */
    public long getMetadataLastModifiedTime() {
        return metadataLastModifiedTime;
    }

    /**
     * This is place holder attribute for session store integration in case
     * if any remote info should be associated on this session.
     * 
     * @return
     */
    public String getRemoteServerInfo() {
        return remoteServerInfo;
    }



    public SubSession getSubSession(String identifier, String name) {
        if (subSessions == null) {
            return null;
        } else {
            for (SubSession subSession : subSessions) {
                if (subSession.getIdentifier().equals(identifier) && subSession.getName().equals(name)) {
                    return subSession;
                }
            }
        }
        return null;
    }


    public Map<String, SessionizationInfo> getSubSessionizerMap() {
        return subSessionizerMap;
    }

    public List<SubSession> getSubSessions() {
        return subSessions;
    }

    public int getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }

    public void setAffinityKey(String affinityKey) {
        this.affinityKey = affinityKey;
    }

    public void setBotEventCount(int botEventCount) {
        this.botEventCount = botEventCount;
    }

    public void setBotType(int botType) {
        this.botType = botType;
    }

    public void setFirstExpirationTime(long firstExpirationTime) {
        this.firstExpirationTime = firstExpirationTime;
    }

    public void setMetadataLastModifiedTime(long metadataLastModifiedTime) {
        this.metadataLastModifiedTime = metadataLastModifiedTime;
    }

    public void setRemoteServerInfo(String remoteServerInfo) {
        this.remoteServerInfo = remoteServerInfo;
    }

    public void setSubSessions(List<SubSession> subSessions) {
        this.subSessions = subSessions;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
