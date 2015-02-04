/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cache.impl;

import java.util.Map.Entry;

import com.ebay.jetstream.util.offheap.MapBuilder;
import com.ebay.jetstream.util.offheap.MapBuilder.Unit;
import com.ebay.jetstream.util.offheap.OffHeapCache;
import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;
import com.ebay.pulsar.sessionizer.cache.MemoryCache;
import com.ebay.pulsar.sessionizer.cache.MemoryCacheConfig;
import com.ebay.pulsar.sessionizer.model.Session;
/**
 * Off-heap thread local session store impl.
 * 
 * @author xingwang
 *
 */
public class SessionMemoryCache implements MemoryCache {
    private final SessionSerializer serializer;
    private final OffHeapCache<String, Session> cache;
    private final OffHeapMemoryManager memManager;

    public SessionMemoryCache(MemoryCacheConfig config) {
        serializer = new SessionSerializer();
        MapBuilder<String, Session> builder = MapBuilder.newBuilder();
        builder.withBlockSize(config.getBlockSize()).withHashCapacity(config.getHashCapacity())
        .withKeySerializer(DefaultSerializerFactory.getInstance().getStringSerializer()).withValueSerializer(serializer)
        .withPageSize(config.getMemoryPageSize()).withUnit(Unit.B).withMaxMemory(((long) config.getPageNumer()) * config.getMemoryPageSize());
        cache = builder.buildCache(config.getMaxTimeSlots());
        memManager = builder.getOffHeapMemoryManager();
    }

    private void setSessionKey(Session session, String uid) {
        session.setType(Integer.valueOf(uid.substring(0, uid.indexOf(':'))));
        session.setIdentifier(uid.substring(uid.indexOf(':') + 1));
    }

    @Override
    public Session get(String uid) {
        Session session = cache.get(uid);
        if (session != null) {
            setSessionKey(session, uid);
        }
        return session;
    }

    @Override
    public long getFreeMemory() {
        return memManager.getFreeMemory();
    }

    @Override
    public int getMaxItemSize() {
        return serializer.getMaxSessionSize();
    }

    @Override
    public long getMaxMemory() {
        return memManager.getMaxMemory();
    }

    @Override
    public long getOOMErrorCount() {
        return memManager.getOOMErrorCount();
    }

    @Override
    public long getReservedMemory() {
        return memManager.getReservedMemory();
    }

    @Override
    public long getSize() {
        return cache.size();
    }

    @Override
    public long getUsedMemory() {
        return memManager.getUsedMemory();
    }

    @Override
    public boolean put(String key, Session session) {
        return cache.put(key, session, session.getFirstExpirationTime());
    }

    @Override
    public boolean remove(String key, Session session) {
        return cache.remove(key);
    }

    @Override
    public Session removeExpired(long expirationTime) {
        Entry<String, Session> e = cache.removeExpiredData(expirationTime);
        if (e != null) {
            setSessionKey(e.getValue(), e.getKey());
            return e.getValue();
        } else {
            return null;
        }
    }

    @Override
    public Session removeFirst() {
        Entry<String, Session> e = cache.removeFirst();
        if (e != null) {
            setSessionKey(e.getValue(), e.getKey());
            return e.getValue();
        } else {
            return null;
        }
    }

    @Override
    public void resetMaxItemSize() {
        serializer.resetMaxItemSize();
    }

}
