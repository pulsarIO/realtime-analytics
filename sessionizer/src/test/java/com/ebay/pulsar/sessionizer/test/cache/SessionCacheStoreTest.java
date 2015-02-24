/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.test.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import com.ebay.pulsar.sessionizer.cache.impl.SessionMemoryCache;
import com.ebay.pulsar.sessionizer.config.SessionizerConfig;
import com.ebay.pulsar.sessionizer.model.Session;

/**
 * Test for session cache store.
 * 
 * @author xingwang
 *
 */
public class SessionCacheStoreTest {
    @Test
    public void testCache() {
        SessionizerConfig config = new SessionizerConfig();
        config.setNativeMemoryInGB(1);
        config.setThreadNum(2);
        SessionMemoryCache store = new SessionMemoryCache(config);

        String guid = UUID.randomUUID().toString();
        String uid = "1:" + guid;
        Assert.assertNull(store.get(uid));
        Session session = new Session();
        session.setCreationTime(System.currentTimeMillis());
        Map<String, Object> initialAttributes = new HashMap<String, Object>();
        initialAttributes.put("1", "2");
        initialAttributes.put("2", "018a0a64206514f8eabaf3a83350373f");
        initialAttributes.put("3", 5);
        initialAttributes.put("4", "6");
        initialAttributes.put("5", "1.2.3.4");
        initialAttributes.put("6", "xyz");
        initialAttributes.put("7", "123234324");
        initialAttributes.put("8","xxxxxxx");
        initialAttributes.put("9", "123.123.234.123");
        initialAttributes.put("10", "8");
        session.setInitialAttributes(initialAttributes);
        Map<String, Object> dynamicAttributes = new HashMap<String, Object>();
        dynamicAttributes.put("11", "xx");
        session.setDynamicAttributes(dynamicAttributes);
        session.setIdentifier(guid);
        session.setType(1);
        store.put(uid, session);
        ReflectionAssert.assertReflectionEquals(session, store.get(uid));
        store.remove(uid, store.get(uid));
        Assert.assertNull(store.get(uid));
    }

    @Test
    public void testCache2() {
        SessionizerConfig config = new SessionizerConfig();
        config.setNativeMemoryInGB(1);
        config.setThreadNum(2);
        SessionMemoryCache store = new SessionMemoryCache(config);

        String guid = UUID.randomUUID().toString();
        String uid = "1:" + guid;
        Assert.assertNull(store.get(uid));
        Session session = new Session();
        session.setCreationTime(System.currentTimeMillis());
        Map<String, Object> initialAttributes = new HashMap<String, Object>();
        initialAttributes.put("1", "2");
        initialAttributes.put("2", "018a0a64206514f8eabaf3a83350373f");
        initialAttributes.put("3", 5);
        initialAttributes.put("4", "6");
        initialAttributes.put("5", "1.2.3.4");
        initialAttributes.put("6", "xyz");
        initialAttributes.put("7", "123234324");
        initialAttributes.put("8","xxxxxxx");
        initialAttributes.put("9", "123.123.234.123");
        initialAttributes.put("10", "8");
        session.setInitialAttributes(initialAttributes);
        Map<String, Object> dynamicAttributes = new HashMap<String, Object>();
        dynamicAttributes.put("11", "xx");
        session.setDynamicAttributes(dynamicAttributes);
        session.setIdentifier(guid);
        session.setType(1);
        store.put(uid, session);
        ReflectionAssert.assertReflectionEquals(session, store.get(uid));
        int maxItemSize = store.getMaxItemSize();
        Assert.assertTrue(maxItemSize > 0);
        store.removeFirst();
        store.resetMaxItemSize();
        maxItemSize = store.getMaxItemSize();
        Assert.assertTrue(maxItemSize == 0);
        Assert.assertNull(store.get(uid));
    }
}
