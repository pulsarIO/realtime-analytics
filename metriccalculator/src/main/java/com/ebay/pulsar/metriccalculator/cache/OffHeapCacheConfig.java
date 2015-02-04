/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.cache;

public class OffHeapCacheConfig {
    private int nativeMemoryInGB = 1;
    private int memoryPageSize = 8192;
    private int blockSize = 256; // Better to align with CPU cache size. In
                                 // existed prod env, it is 64 bytes.
    private int hashCapacity = 1 << 22; // 4M

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public int getHashCapacity() {
        return hashCapacity;
    }

    public void setHashCapacity(int hashCapacity) {
        this.hashCapacity = hashCapacity;
    }

    public int getMemoryPageSize() {
        return memoryPageSize;
    }

    public void setMemoryPageSize(int memoryPageSize) {
        this.memoryPageSize = memoryPageSize;
    }

    public int getNativeMemoryInGB() {
        return nativeMemoryInGB;
    }

    public void setNativeMemoryInGB(int nativeMemoryInGB) {
        this.nativeMemoryInGB = nativeMemoryInGB;
    }

    public int getPageNumer() {
        return (int) ((((long) nativeMemoryInGB) << 30) / memoryPageSize);
    }
}