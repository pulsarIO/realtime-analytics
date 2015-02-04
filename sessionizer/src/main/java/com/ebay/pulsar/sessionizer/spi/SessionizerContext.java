/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.spi;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.pulsar.sessionizer.model.AbstractSession;
import com.ebay.pulsar.sessionizer.model.Session;

/**
 * A context for sessionzier extension.
 * 
 * The extension listener can use the context method to manipulate the session or event.
 * 
 * @author xingwang
 *
 */
public class SessionizerContext {
    private static class ValueHolder<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }


    private final ValueHolder<Session> mainSession = new ValueHolder<Session>();

    private final ValueHolder<AbstractSession> currentSession = new ValueHolder<AbstractSession>();

    private final ValueHolder<JetstreamEvent> event = new ValueHolder<JetstreamEvent>();

    private final ValueHolder<Boolean> metadataChangeFlag = new ValueHolder<Boolean>();

    /**
     * Get current session instance.
     * 
     * @return
     */
    public AbstractSession getCurrentSession() {
        return currentSession.getValue();
    }

    /**
     * Get current input event.
     * 
     * @return
     */
    public JetstreamEvent getEvent() {
        return event.getValue();
    }

    /**
     * Get the main session.
     * 
     * @return
     */
    public Session getMainSession() {
        return mainSession.getValue();
    }

    /**
     * Whether the metadata changed.
     * @return
     */
    public boolean isMetadataChanged() {
        return this.metadataChangeFlag.getValue();
    }

    /**
     * Mark the main session metadata changed.
     */
    public void markMetadataChanged() {
        this.metadataChangeFlag.setValue(Boolean.TRUE);
    }

    /**
     * Invoked by sessionizer engine.
     * 
     * @param session
     */
    public void setCurrentSession(AbstractSession session) {
        this.currentSession.setValue(session);
        this.metadataChangeFlag.setValue(Boolean.FALSE);
    }

    /**
     * Invoked by sessionizer engine.
     * 
     * @param rawEvent
     */
    public void setEvent(JetstreamEvent rawEvent) {
        this.event.setValue(rawEvent);
    }

    /**
     * Invoked by sessionizer engine.
     * @param session
     */
    public void setMainSession(Session session) {
        this.mainSession.setValue(session);
    }
}
