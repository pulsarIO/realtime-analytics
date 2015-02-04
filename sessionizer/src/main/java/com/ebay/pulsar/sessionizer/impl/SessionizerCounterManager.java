/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * Counters for each sessionization profile.
 * 
 * @author xingwang
 *
 */
public class SessionizerCounterManager implements XSerializable {
    private long rawEvents = 0;
    private long createdSession = 0;
    private long expiredSession = 0;
    private long expiredSubSession = 0;
    private long createdSubSession = 0;
    private long singleClickSessionCounter = 0;
    private long singleClickSubSessionCounter = 0;
    private long sessionDurationTotal = 0;
    private long subSessionDurationTotal = 0;
    private long shortSessionCounter = 0;
    private long longSessionCounter = 0;
    private long sessionLagTime = 0;

    public long getAverageSessionDuration() {
        long sessionNum = expiredSession - singleClickSessionCounter;
        if (sessionNum > 0) {
            return sessionDurationTotal / sessionNum;
        } else {
            return 0;
        }
    }
    @ManagedAttribute
    public long getAverageSessionLagTime() {
        long sessionNum = createdSession;
        if (sessionNum > 0) {
            return sessionLagTime / sessionNum;
        } else {
            return 0;
        }
    }
    @ManagedAttribute
    public long getAverageSubSessionDuration() {
        long sessionNum = expiredSubSession - singleClickSubSessionCounter;
        if (sessionNum > 0) {
            return subSessionDurationTotal / sessionNum;
        } else {
            return 0;
        }
    }

    public long getCreatedSession() {
        return createdSession;
    }

    public long getCreatedSubSession() {
        return createdSubSession;
    }
    public long getExpiredSession() {
        return expiredSession;
    }
    public long getExpiredSubSession() {
        return expiredSubSession;
    }
    public long getLongSessionCounter() {
        return longSessionCounter;
    }

    public long getRawEvents() {
        return rawEvents;
    }
    public long getSessionDurationTotal() {
        return sessionDurationTotal;
    }
    public long getSessionLagTime() {
        return sessionLagTime;
    }
    public long getShortSessionCounter() {
        return shortSessionCounter;
    }
    public long getSingleClickSessionCounter() {
        return singleClickSessionCounter;
    }
    public long getSingleClickSubSessionCounter() {
        return singleClickSubSessionCounter;
    }
    public long getSubSessionDurationTotal() {
        return subSessionDurationTotal;
    }
    public void incCreatedSession() {
        createdSession++;
    }
    public void incCreatedSubSession() {
        createdSubSession++;
    }

    public void incExpiredSession() {
        expiredSession++;
    }

    public void incExpiredSubSession() {
        expiredSubSession++;
    }
    public void incLongSessionCounter() {
        longSessionCounter++;
    }
    public void incRawEvents() {
        rawEvents++;
    }
    public void incSessionDuration(long duration) {
        this.sessionDurationTotal += duration;
    }
    public void incsessionLagTime(long lagTime) {
        this.sessionLagTime += lagTime;
    }
    public void incShortSessionCounter() {
        shortSessionCounter++;
    }
    public void incSingleClickSessionCounter() {
        singleClickSessionCounter++;
    }
    public void incSingleClickSubSessionCounter() {
        singleClickSubSessionCounter++;
    }

    public void incSubSessionDuration(long duration) {
        this.subSessionDurationTotal += duration;
    }

    public void sum(SessionizerCounterManager x) {
        this.rawEvents += x.rawEvents;
        this.createdSession += x.createdSession;
        this.expiredSession += x.expiredSession;
        this.expiredSubSession += x.expiredSubSession;
        this.createdSubSession += x.createdSubSession;
        this.singleClickSessionCounter += x.singleClickSessionCounter;
        this.singleClickSubSessionCounter += x.singleClickSubSessionCounter;
        this.sessionDurationTotal += x.sessionDurationTotal;
        this.subSessionDurationTotal += x.subSessionDurationTotal;
        this.shortSessionCounter += x.shortSessionCounter;
        this.longSessionCounter += x.longSessionCounter;
        this.sessionLagTime += x.sessionLagTime;
    }
}