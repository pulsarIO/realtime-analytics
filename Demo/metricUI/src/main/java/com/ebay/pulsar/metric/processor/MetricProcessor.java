/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.processor;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.pulsar.metric.data.DataPoint;
import com.ebay.pulsar.websocket.WebSocketConnectionManager;

@ManagedResource(objectName = "Event/Processor", description = "MetricUI Processor")
public class MetricProcessor extends AbstractEventProcessor {
    private static int GENERIC_MAX_POINT = 1;
    private static int PAGE_VIEWS_POINT = 30;
    private Map<String, Deque<DataPoint>> dataBuffer = new ConcurrentHashMap<String, Deque<DataPoint>>();
    
    public WebSocketConnectionManager getWsConnectionManager() {
        return wsConnectionManager;
    }

    public void setWsConnectionManager(
            WebSocketConnectionManager wsConnectionManager) {
        this.wsConnectionManager = wsConnectionManager;
    }
    
    private WebSocketConnectionManager wsConnectionManager;

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        incrementEventRecievedCounter();
        String eventType = event.getEventType();

        Deque<DataPoint> dataByType = dataBuffer.get(eventType);
        if (dataByType == null) {
            dataByType = new ConcurrentLinkedDeque<DataPoint>();
            dataBuffer.put(eventType, dataByType);
        }

        Long currentTimestamp = System.currentTimeMillis();
        boolean isNewBatchOfEvents = false;

        if (dataByType.size() > 0
                && event.get("context_id") != dataByType.getLast().getEvents()
                        .peek().get("context_id"))
            isNewBatchOfEvents = true;

        //Flush old batchs
        if (isNewBatchOfEvents) {
            broadcastEventsByType(eventType, dataByType);
            incrementEventSentCounter();
        }
        
        if (dataByType.size() == 0 || isNewBatchOfEvents) {
            DataPoint dataPoint = new DataPoint(currentTimestamp);
            dataByType.add(dataPoint);
        }
        
        dataByType.getLast().addEvent(event);
        int maxLength = GENERIC_MAX_POINT;
        if(eventType.equalsIgnoreCase("MC_Metric") || eventType.equalsIgnoreCase("TwitterEventCount")){
            maxLength = PAGE_VIEWS_POINT;
        }
        if (dataByType.size() > maxLength) {
            dataByType.removeFirst();
        }
    }

    public void broadcastAllEvents() {
        for (Map.Entry<String, Deque<DataPoint>> entry : dataBuffer.entrySet()) {
            String eventType = entry.getKey();
            Deque<DataPoint> dataByType = entry.getValue();
            broadcastEventsByType(eventType, dataByType);
        }
    }

    private void broadcastEventsByType(String eventType,
            Deque<DataPoint> dataByType) {
        List<Queue<JetstreamEvent>> eventsList = new ArrayList<Queue<JetstreamEvent>>();
        if(eventType.equalsIgnoreCase("MC_Metric") || eventType.equalsIgnoreCase("TwitterEventCount")) {
            for(DataPoint dataPoint : dataByType) {
                eventsList.add(dataPoint.getEvents());
            }
        } else{
            eventsList.add(dataByType.getLast().getEvents());
        }
        if(wsConnectionManager != null)
            wsConnectionManager.broadcast(eventType, eventsList);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void shutDown() {
    }

    @Override
    public void pause() {
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
    }

    @Override
    public void resume() {
    }
    
}
