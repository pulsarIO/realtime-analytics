/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.model;

import java.util.Map;

/**
 * POJO definition for session.
 * 
 * @author xingwang
 */
public class AbstractSession {
    //Persisted attributes
    private long creationTime; // System time
    private long firstEventTimestamp; // First Event timestamp.
    private int ttl;
    private long lastModifiedTime; // System time
    private long expirationTime;
    private int eventCount;
    private Map<String, Object> dynamicAttributes;
    private Map<String, Object> initialAttributes;

    // Temp attributes
    private String sessionId;
    private String identifier;

    /**
     * Session creation time.
     * 
     * @return
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Session duration in milliseconds.
     * 
     * Single click session duration will be 0.
     * 
     * @return
     */
    public long getDuration() {
        if (eventCount > 1) {
            return lastModifiedTime - creationTime;
        } else {
            return 0L;
        }
    }

    /**
     * Session payload.
     * 
     * A map for counters, variables. The value should be
     * primitive type or list of primitive types.
     * 
     * @return
     */
    public Map<String, Object> getDynamicAttributes() {
        return dynamicAttributes;
    }

    /**
     * Event count of the session.
     * 
     * @return
     */
    public int getEventCount() {
        return eventCount;
    }

    /**
     * Session expiration time.
     * 
     * Session TTL puls the last modified time.
     * 
     * @return
     */
    public long getExpirationTime() {
        return expirationTime;
    }

    /**
     * First event timestamp of the session.
     * 
     * @return
     */
    public long getFirstEventTimestamp() {
        return firstEventTimestamp;
    }

    /**
     * Session identifier.
     * 
     * @return
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Session metadata.
     * 
     * 
     * @return
     */
    public Map<String, Object> getInitialAttributes() {
        return initialAttributes;
    }

    /**
     * Session last modified time.
     * 
     * @return
     */
    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    /**
     * The session id.
     * 
     * Session id will be combined by session identifier and first event timestamp.
     * 
     * @return
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Session ttl.
     * 
     * @return
     */
    public int getTtl() {
        return ttl;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }
    public void setDynamicAttributes(Map<String, Object> dynamicAttributes) {
        this.dynamicAttributes = dynamicAttributes;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void setFirstEventTimestamp(long firstEventTimestamp) {
        this.firstEventTimestamp = firstEventTimestamp;
    }
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    public void setInitialAttributes(Map<String, Object> initialAttributes) {
        this.initialAttributes = initialAttributes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public void updateTtl(int ttl) {
        this.ttl = ttl;
        this.expirationTime = this.lastModifiedTime + ttl;
    }
}
