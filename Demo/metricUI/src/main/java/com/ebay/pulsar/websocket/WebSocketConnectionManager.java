/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.websocket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.ErrorManager;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.pulsar.metric.util.JsonUtil;

@ManagedResource(objectName = "Event/Processor", description = "WebSocket Manager")
public class WebSocketConnectionManager extends AbstractNamedBean implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger("com.ebay.pulsar.websocket.WebSocketConnectionManager");

    private final LongCounter totalEventsSent = new LongCounter();
    private final LongEWMACounter eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
    private final Map<String, List<Queue<JetstreamEvent>>> lastBroadCastbatch = new ConcurrentHashMap<String, List<Queue<JetstreamEvent>>>();
    
    private volatile String lastSuccessMessage;
    
    private ErrorManager m_errors = new ErrorManager();
    private Set<MetricWebSocket> sockets;

    private WebSocketConnectionManager() {
        sockets = new CopyOnWriteArraySet<MetricWebSocket>();
    }
    
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
    }

    public void add(MetricWebSocket socket) {
        sockets.add(socket);
    }

    public void remove(MetricWebSocket socket) {
        sockets.remove(socket);
    }

    public void broadcast(String channel, List<Queue<JetstreamEvent>> eventsList) {
        lastBroadCastbatch.put(channel, eventsList);
        for (MetricWebSocket socket : sockets) {
            try {
                List<JetstreamEvent> eventsFilteredForSocket = new ArrayList<JetstreamEvent>();
                for (Queue<JetstreamEvent> events : eventsList) {
                    for (JetstreamEvent event : events) {
                        if(socket.getChannles().contains(event.getEventType())){
                            eventsFilteredForSocket.add(event);
                        }
                    }
                }
                if (eventsFilteredForSocket.size() > 0) {
                    String message = JsonUtil.toJson(eventsFilteredForSocket);
                    socket.getRemote().sendString(message);
                    lastSuccessMessage = message;
                }
                incrementEventSentCounter();
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage(), e);
                registerError(e);
            }
        }
    }
    
    public void broadcastToSession(MetricWebSocket socket) {
        for(String channel: lastBroadCastbatch.keySet()){
            List<Queue<JetstreamEvent>> eventsList = lastBroadCastbatch.get(channel);
            if(eventsList!=null && eventsList.size() > 0){
                try {
                    List<JetstreamEvent> eventsFilteredForSocket = new ArrayList<JetstreamEvent>();
                    for (Queue<JetstreamEvent> events : eventsList) {
                        for (JetstreamEvent event : events) {
                            if(socket.getChannles().contains(event.getEventType())){
                                eventsFilteredForSocket.add(event);
                            }
                        }
                    }
                    if (eventsFilteredForSocket.size() > 0) {
                        String message = JsonUtil.toJson(eventsFilteredForSocket);
                        socket.getRemote().sendString(message);
                        lastSuccessMessage = message;
                    }
                    incrementEventSentCounter();
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage(), e);
                    registerError(e);
                }
            }
        }
    }

    public void broadcast(String channel, JetstreamEvent event) {
        if (sockets.size() > 0) {
            String message = JsonUtil.toJson(event);
            for (MetricWebSocket socket : sockets) {
                try {
                    if(socket.getChannles().contains(event.getEventType())){
                        socket.getRemote().sendString(message);
                        lastSuccessMessage = message;
                        incrementEventSentCounter();
                    }
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage(), e);
                    registerError(e);
                }
            }
        }
    }
    
    public int getOpenSocketSize(){
        return sockets.size();
    }
    
    public String getLastSuccessMessage(){
        return lastSuccessMessage;
    }
    
    @ManagedOperation
    public void clearErrorList() {
        m_errors.clearErrorList();
    }

    public String getErrors() {
        return m_errors.toString();
    }

    public void registerError(Throwable t) {
        m_errors.registerError(t);
    }
    
    public void incrementEventSentCounter() {
        totalEventsSent.increment();
        eventsSentPerSec.increment();
    }

    public long getTotalEventsSent() {
        return totalEventsSent.get();
    }
    
    public long getEventsSentPerSec() {
        return eventsSentPerSec.get();
    }
    
}
