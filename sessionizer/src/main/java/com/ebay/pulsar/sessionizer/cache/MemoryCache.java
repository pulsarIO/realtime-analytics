/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cache;

import com.ebay.pulsar.sessionizer.model.Session;

/**
 * Local off-heap cache.
 * 
 * It is a linked-hash cache with expiration support. It includes
 * a linked hash map and an address to blob map.
 * 
 * @author xingwang
 */
public interface MemoryCache {
    Session get(String key);

    long getReservedMemory();

    long getUsedMemory();

    long getFreeMemory();

    long getMaxMemory();

    long getSize();

    boolean put(String key, Session value);

    boolean remove(String key, Session value);

    Session removeExpired(long timestamp);

    Session removeFirst();

    long getOOMErrorCount();

    int getMaxItemSize();

    void resetMaxItemSize();
}
