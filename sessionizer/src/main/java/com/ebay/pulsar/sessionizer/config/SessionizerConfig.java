/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.config;

import java.util.List;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.esper.EPL;
import com.ebay.jetstream.event.processor.esper.EsperDeclaredEvents;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.pulsar.sessionizer.cache.MemoryCacheConfig;

/**
 * Configuration for sessionizer.
 * 
 * @author xingwang
 */
@ManagedResource(objectName = "Event/Processor", description = "SessionizerConfig")
public class SessionizerConfig extends AbstractNamedBean implements XSerializable, MemoryCacheConfig {
    private int threadNum = Runtime.getRuntime().availableProcessors();
    private int maxTimeSlots = 120 * 60;
    private int queueSize = 32768;
    private int nativeMemoryInGB = 1;
    private int memoryPageSize = 16777216; //16M
    private int blockSize = 512; //Better to align with CPU cache size. In existed prod env, it is 64 bytes.
    private int hashCapacity = 1 << 22; // 4M

    // Properties which can be dynamic changed.
    private int readQueryTimeout = 5000;
    private boolean enableReadOptimization = true;
    private int maxIdleTime = 30 * 60 * 1000; // Limitation: should less than
    // maxTimeSlots * 1000;
    private List<SessionProfile> mainSessionProfiles;


    private EsperDeclaredEvents rawEventDefinition;

    private EPL epl;

    private List<String> imports;

    /**
     * Block size for native memory.
     */
    @Override
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Whether enable read optimization by using consistent hashing listener.
     * 
     * @return
     */
    public boolean getEnableReadOptimization() {
        return enableReadOptimization;
    }

    /**
     * EPL for creating sessionization hints.
     * 
     * @return
     */
    public EPL getEpl() {
        return epl;
    }


    /**
     * Hash capacity of the session cache.
     */
    @Override
    public int getHashCapacity() {
        return hashCapacity;
    }

    /**
     * UDF imports for the EPL.
     * 
     * @return
     */
    public List<String> getImports() {
        return imports;
    }

    /**
     * Main sessionizer profiles.
     * @return
     */
    public List<SessionProfile> getMainSessionProfiles() {
        return mainSessionProfiles;
    }

    /**
     * Max session idle time.
     * 
     * @return
     */
    public int getMaxIdleTime() {
        return maxIdleTime;
    }


    /**
     * Max time slots, it limited the max session TTL.
     */
    @Override
    public int getMaxTimeSlots() {
        return maxTimeSlots;
    }


    /**
     * Memory page size for native cache.
     */
    @Override
    public int getMemoryPageSize() {
        return memoryPageSize;
    }

    /**
     * Cache memory capacity.
     * 
     * @return
     */
    public int getNativeMemoryInGB() {
        return nativeMemoryInGB;
    }


    /**
     * Page numer of the native memory.
     */
    @Override
    public int getPageNumer() {
        return (int) ((((long) nativeMemoryInGB) << 30) / threadNum / memoryPageSize);
    }

    /**
     * Queue size for the sessionizer processor.
     * 
     * @return
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Event definiton for EPL.
     * 
     * @return
     */
    public EsperDeclaredEvents getRawEventDefinition() {
        return rawEventDefinition;
    }

    /**
     * Query timeout for the session recovery (from remote store).
     * 
     * @return
     */
    public int getReadQueryTimeout() {
        return readQueryTimeout;
    }


    /**
     * Threads of the sessionizer processor.
     */
    @Override
    public int getThreadNum() {
        return threadNum;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public void setEnableReadOptimization(boolean enableReadOptimization) {
        this.enableReadOptimization = enableReadOptimization;
    }

    public void setEpl(EPL epl) {
        this.epl = epl;
    }

    public void setHashCapacity(int hashCapacity) {
        int x = 2; // normalize it
        while (x < hashCapacity) {
            x = x << 1;
        }
        this.hashCapacity = x;
    }

    public void setImports(List<String> imports) {
        this.imports = imports;
    }

    public void setMainSessionProfiles(List<SessionProfile> mainSessionProfiles) {
        this.mainSessionProfiles = mainSessionProfiles;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public void setMaxTimeSlots(int maxTimeSlots) {
        this.maxTimeSlots = maxTimeSlots;
    }

    public void setMemoryPageSize(int memoryPageSize) {
        this.memoryPageSize = memoryPageSize;
    }

    public void setNativeMemoryInGB(int nativeMemoryInGB) {
        this.nativeMemoryInGB = nativeMemoryInGB;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void setRawEventDefinition(EsperDeclaredEvents rawEventDefinition) {
        this.rawEventDefinition = rawEventDefinition;
    }

    public void setReadQueryTimeout(int readTimeout) {
        this.readQueryTimeout = readTimeout;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }
}