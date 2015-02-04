/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.ErrorManager;

/**
 * An enhanced error manager for sessionizer.
 * 
 * @author xingwang
 *
 */
@ManagedResource(objectName = "SessionizerErrorManager", description = "SessionizerErrorManager")
public class SessionizerErrorManager {

    public enum ErrorType {
        Unexpected,
        MissedData,
        RemoteStore,
        Configuration,
        Esper
    }
    private final ErrorManager m_errors = new ErrorManager();

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionizerErrorManager.class);
    private volatile boolean errorManagerEnabled = false;
    private final Map<String, ConcurrentHashMap<String, LongCounter>> errorCounterMap
    = new HashMap<String, ConcurrentHashMap<String, LongCounter>>();
    private final AtomicLong errorCount = new AtomicLong(0);
    private String lastErrorMessage;

    public SessionizerErrorManager() {
        for (ErrorType e : ErrorType.values()) {
            errorCounterMap.put(e.name(), new ConcurrentHashMap<String, LongCounter>());
        }
    }

    @ManagedOperation
    public void cleanErrors() {
        for (ConcurrentHashMap<String, LongCounter> v : errorCounterMap.values()) {
            v.clear();
        }
        errorCount.set(0);
        m_errors.clearErrorList();
    }

    @ManagedAttribute
    public Map<String, ConcurrentHashMap<String, LongCounter>> getErrorCounterMap() {
        return errorCounterMap;
    }

    @ManagedAttribute
    public ErrorManager getErrorManager() {
        return m_errors;
    }

    @ManagedAttribute
    public boolean getErrorManagerEnabled() {
        return errorManagerEnabled;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    @ManagedAttribute
    public long getTotalErrorCount() {
        return errorCount.get();
    }

    private long incCounter(String errorName, ErrorType type) {

        ConcurrentMap<String, LongCounter> map = errorCounterMap.get(type.name());
        LongCounter counter = map.get(errorName);
        if (counter == null) {
            counter = new LongCounter();

            LongCounter existed = map.putIfAbsent(errorName, counter);
            if (existed != null) {
                counter = existed;
            }
        }
        return counter.increment();
    }

    private void incCounter(Throwable ex, ErrorType type) {
        errorCount.incrementAndGet();

        long v = incCounter(ex.getClass().getName(), type);
        if (v % 1000000 == 1) {
            //print exception for first time.
            LOGGER.error("Exception count=" + v, ex);
        }
    }

    public void registerError(String category, ErrorType type, String message) {
        long v = incCounter(category, type);
        this.lastErrorMessage = message;
        if (v % 1000000 == 1) {
            // print exception for first time.
            LOGGER.warn("{} : {}", category, message);
        }
    }

    public void registerError(Throwable ex, ErrorType type) {
        incCounter(ex, type);
        if (errorManagerEnabled) {
            m_errors.registerError(ex);
        }
    }

    public void registerError(Throwable ex, JetstreamEvent event, ErrorType type) {
        incCounter(ex, type);
        if (errorManagerEnabled) {
            m_errors.registerError(ex, event);
        }
    }

    @ManagedOperation
    public void toggleErrorManager() {
        errorManagerEnabled = !errorManagerEnabled;
        if (!errorManagerEnabled) {
            m_errors.clearErrorList();
        }
    }
}
