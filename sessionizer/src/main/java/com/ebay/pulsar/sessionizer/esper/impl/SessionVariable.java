/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.impl;

import com.ebay.pulsar.sessionizer.model.AbstractSession;
import com.ebay.pulsar.sessionizer.model.Session;

/**
 * Session variable for the Esper.
 * 
 * @author xingwang
 *
 */
public class SessionVariable extends AttributeMapVariable {
    private AbstractSession data;
    private Session mainSession;

    public String getSessionId() {
        return data.getSessionId();
    }

    public long getDuration() {
        return data.getDuration();
    }
    public int getBotEventCount() {
        return mainSession.getBotEventCount();
    }

    public int getBotType() {
        return mainSession.getBotType();
    }

    public long getCreationTime() {
        return data.getCreationTime();
    }

    public int getEventCount() {
        return data.getEventCount();
    }

    public long getExpirationTime() {
        return data.getExpirationTime();
    }

    public long getFirstEventTimestamp() {
        return data.getFirstEventTimestamp();
    }

    public String getIdentifier() {
        return data.getIdentifier();
    }


    public long getLastModifiedTime() {
        return data.getLastModifiedTime();
    }


    public void resetSessionData(AbstractSession data, Session mainSession) {
        this.data = data;
        this.mainSession = mainSession;
    }
}
