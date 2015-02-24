/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ebay.jetstream.util.offheap.MapBuilder;
import com.ebay.jetstream.util.offheap.MapBuilder.Unit;
import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;
import com.ebay.pulsar.metriccalculator.metric.MCMetricGroupDemension;
import com.ebay.pulsar.metriccalculator.offheap.serializer.CounterSerializer;
import com.ebay.pulsar.metriccalculator.offheap.serializer.GroupDemensionSerializer;
import com.ebay.pulsar.metriccalculator.statistics.basic.Counter;

public class CacheManager {

    public static final int GROUPCOUNTER_INIT_SIZE = 20000;

    public static Map<MCMetricGroupDemension, Counter> getCounterCache() {
        return new ConcurrentHashMap<MCMetricGroupDemension, Counter>(
                GROUPCOUNTER_INIT_SIZE);
    }

    public static Map<MCMetricGroupDemension, Counter> getCounterOffHeapCache(
            String memoryManagerName, OffHeapCacheConfig config) {
        MapBuilder<MCMetricGroupDemension, Counter> builder = MapBuilder.newBuilder();
        GroupDemensionSerializer keySerializer = new GroupDemensionSerializer();
        CounterSerializer valueSerialize = new CounterSerializer();

        builder.withKeySerializer(keySerializer)
                .withValueSerializer(valueSerialize)
                .withBlockSize(config.getBlockSize())
                .withHashCapacity(config.getHashCapacity())
                .withPageSize(config.getMemoryPageSize())
                .withUnit(Unit.B)
                .withMaxMemory(
                        ((long) config.getPageNumer())
                                * config.getMemoryPageSize());
        OffHeapMemoryManager memoryManager = builder.getOffHeapMemoryManager();
        OffHeapMemoryManagerRegistry.getInstance().addMemoryManager(
                memoryManagerName, memoryManager);
        return builder.buildHashMap();
    }
}
