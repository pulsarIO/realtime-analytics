/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cache;

/**
 * Sessionizer off-heap cache config.
 * 
 * @author xingwang
 *
 */
public interface MemoryCacheConfig {
    /**
     * Block size.
     * 
     * @return
     */
    int getBlockSize();

    /**
     * Hash capacity of the cache.
     * 
     * @return
     */
    int getHashCapacity();

    /**
     * Max time slots.
     * 
     * @return
     */
    int getMaxTimeSlots();

    /**
     * Memory page size.
     * 
     * @return
     */
    int getMemoryPageSize();

    /**
     * Total page numbers.
     * 
     * @return
     */
    int getPageNumer();

    /**
     * Thread number.
     * 
     * @return
     */
    int getThreadNum();
}
