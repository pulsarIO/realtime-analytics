/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;

public class OffHeapMemoryManagerRegistry {

    private static OffHeapMemoryManagerRegistry m_instance = new OffHeapMemoryManagerRegistry();

    private Map<String, OffHeapMemoryManager> memoryManagers = new HashMap<String, OffHeapMemoryManager>();

    private OffHeapMemoryManagerRegistry() {
    }

    public static OffHeapMemoryManagerRegistry getInstance() {
        return m_instance;
    }

    public void addMemoryManager(String name,
            OffHeapMemoryManager fffHeapMemoryManager) {
        memoryManagers.put(name, fffHeapMemoryManager);
    }

    public Map<String, OffHeapMemoryManager> getMemoryManagers() {
        return Collections.unmodifiableMap(memoryManagers);
    }
}
