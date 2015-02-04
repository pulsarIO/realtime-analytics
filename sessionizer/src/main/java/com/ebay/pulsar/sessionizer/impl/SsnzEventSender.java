/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;

/**
 * An internal interface between SessionizerProcessor and Sessionizer.
 * 
 * @author xingwang
 *
 */
public interface SsnzEventSender {
    void sendSessionBeginEvent(int sessionType, Session session, JetstreamEvent event);
    void sendSessionEndEvent(int sessionType, Session session, JetstreamEvent event);
    void sendSubSessionBeginEvent(int sessionType, SubSession subSession, JetstreamEvent event);
    void sendSubSessionEndEvent(int sessionType, SubSession subSession, JetstreamEvent event);
}
