/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.config;

import java.util.List;

import com.ebay.jetstream.event.processor.esper.EsperDeclaredEvents;
/**
 * Sessionization profile for main session.
 * 
 * @author xingwang
 *
 */
public class SessionProfile extends CommonSessionProfile {
    private int sessionType;
    private EsperDeclaredEvents rawEventDefinition;
    private List<SubSessionProfile> subSessionProfiles;
    private int maxActiveTime = 24 * 60 * 60 * 1000;

    private String sessionStartTimestampKey = "_snst";
    private String sessionEndTimestampKey = "_snet";
    private String sessionDurationKey = "_sndn";
    private String sessionEventCountKey = "_snec";

    /**
     * The max active time for a session in milliseconds.
     * 
     * Default is one day.
     * 
     * @return
     */
    public int getMaxActiveTime() {
        return maxActiveTime;
    }

    /**
     * Event type definition.
     * 
     * If it includes session begin, end marker event type. then the session
     * begin, end marker event will be passed to Esper sessionizer.
     * 
     * @return
     */
    public EsperDeclaredEvents getRawEventDefinition() {
        return rawEventDefinition;
    }

    /**
     * Customize the session duration key.
     * 
     * Default is _sndn.
     * 
     * @return
     */
    public String getSessionDurationKey() {
        return sessionDurationKey;
    }

    /**
     * Customize the session end timestamp key.
     * 
     * Default is _snet.
     * 
     * @return
     */
    public String getSessionEndTimestampKey() {
        return sessionEndTimestampKey;
    }

    /**
     * Customize the session event count kye.
     * 
     * Default is _snec.
     * 
     * @return
     */
    public String getSessionEventCountKey() {
        return sessionEventCountKey;
    }

    /**
     * Customize the session start timestamp key.
     * 
     * Default is _snst.
     * 
     * @return
     */
    public String getSessionStartTimestampKey() {
        return sessionStartTimestampKey;
    }

    /**
     * Session profile id. Should be global unique.
     * 
     * It will be used to construct unique session id key for store.
     * 
     * @return
     */
    public int getSessionType() {
        return sessionType;
    }

    /**
     * Sub session profiles of this main sessionizer.
     * 
     * @return
     */
    public List<SubSessionProfile> getSubSessionProfiles() {
        return subSessionProfiles;
    }

    public void setMaxActiveTime(int maxActiveTime) {
        this.maxActiveTime = maxActiveTime;
    }

    public void setRawEventDefinition(EsperDeclaredEvents rawEventDefinition) {
        this.rawEventDefinition = rawEventDefinition;
    }

    public void setSessionDurationKey(String sessionDurationKey) {
        this.sessionDurationKey = sessionDurationKey;
    }

    public void setSessionEndTimestampKey(String sessionEndTimestampKey) {
        this.sessionEndTimestampKey = sessionEndTimestampKey;
    }

    public void setSessionEventCountKey(String sessionEventCountKey) {
        this.sessionEventCountKey = sessionEventCountKey;
    }

    public void setSessionStartTimestampKey(String sessionStartTimestampKey) {
        this.sessionStartTimestampKey = sessionStartTimestampKey;
    }

    public void setSessionType(int sessionType) {
        this.sessionType = sessionType;
    }

    public void setSubSessionProfiles(List<SubSessionProfile> profiles) {
        this.subSessionProfiles = profiles;
    }

}
