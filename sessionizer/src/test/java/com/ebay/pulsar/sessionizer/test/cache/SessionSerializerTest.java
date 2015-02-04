/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.test.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import com.ebay.pulsar.sessionizer.cache.impl.SessionSerializer;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;
import com.ebay.pulsar.sessionizer.util.BinaryFormatSerializer;

/**
 * Test for session serializer.
 * 
 * @author xingwang
 *
 */
public class SessionSerializerTest {
    public static void main(String[] args) throws Exception {
        Session session = createSession();
        SessionSerializer s = new SessionSerializer();
        {
            ByteBuffer valueBuffer = s.serialize(session);
            System.out.println("Size:" + valueBuffer.limit());
            s.deserialize(valueBuffer, 0, valueBuffer.limit());
        }
        int count = 1000;
        long nanoTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            ByteBuffer valueBuffer = s.serialize(session);
            s.deserialize(valueBuffer, 0, valueBuffer.limit());
        }
        System.out.println("Time used per reuqest (ns): " + (System.nanoTime() - nanoTime) / count);
    }

    private static Session createSession() {
        Session session = new Session();
        Map<String, Object> initialAttributes = new HashMap<String, Object>();
        initialAttributes.put("1", "2");
        initialAttributes.put("2", UUID.randomUUID().toString());
        initialAttributes.put("3", 5);
        initialAttributes.put("4", "6");
        initialAttributes.put("5", "1.2.3.4");
        initialAttributes.put("6", "xyz");
        initialAttributes.put("7", "123234324");
        initialAttributes.put("8", "sfsdfsdfasdfsfsadf");
        initialAttributes.put("9", "123.123.234.123");
        initialAttributes.put("10", "8");
        initialAttributes.put("21", "");
        session.setInitialAttributes(initialAttributes);
        session.setCreationTime(System.currentTimeMillis());
        session.setEventCount(5);
        session.setFirstEventTimestamp(System.currentTimeMillis());
        session.setLastModifiedTime(System.currentTimeMillis());
        return session;
    }

    @Test
    public void testEmptySession() throws Exception {
        Session session = new Session();
        runTest(session);
    }

    @Test
    public void testNormalSession() throws Exception {
        Session session = createSession();
        runTest(session);
    }

    @Test
    public void testSubSession() throws Exception {
        Session session = createSession();
        SubSession subSession = new SubSession();
        subSession.setExpirationTime(System.currentTimeMillis());
        session.addSubSession(subSession);
        runTest(session);
    }

    @Test
    public void testSubSessions() throws Exception {
        Session session = createSession();
        SubSession subSession = new SubSession();
        subSession.setExpirationTime(System.currentTimeMillis());
        subSession.setIdentifier("1");
        subSession.setName("1");
        session.addSubSession(subSession);
        SubSession subSession2 = new SubSession();
        subSession2.setExpirationTime(System.currentTimeMillis());
        session.addSubSession(subSession2);
        subSession2.setIdentifier("2");
        subSession2.setName("2");
        Assert.assertEquals(subSession, session.getSubSession("1", "1"));
        Assert.assertEquals(subSession2, session.getSubSession("2", "2"));
        Assert.assertNull(session.getSubSession("2", "1"));
        Assert.assertNull(session.getSubSession("1", "2"));
        Assert.assertNull(session.getSubSession("3", "3"));
    }


    @Test
    public void testVersionCheck() throws Exception {
        Session session = createSession();
        ByteBuffer metaData1 = BinaryFormatSerializer.getInstance().getSessionMetadata(session);
        metaData1.put(0, (byte) (metaData1.get(0) + 1));
        ByteBuffer payload1 = BinaryFormatSerializer.getInstance().getSessionPayload(session);
        payload1.put(0, (byte) (payload1.get(0) + 1));

        Session session3 = new Session();
        Assert.assertFalse(BinaryFormatSerializer.getInstance().setSessionMetadata(session3, metaData1));
        Assert.assertFalse(BinaryFormatSerializer.getInstance().setSessionPayload(session3, payload1));
    }

    @Test
    public void testSubSession2() throws Exception {
        Session session = createSession();
        SubSession subSession = new SubSession();
        subSession.setExpirationTime(System.currentTimeMillis());
        Map<String, Object> initialAttributes = new HashMap<String, Object>();
        initialAttributes.put("1", "x");
        subSession.setInitialAttributes(initialAttributes);
        Map<String, Object> dynamicAttributes = new HashMap<String, Object>();
        dynamicAttributes.put("1", "x");
        subSession.setDynamicAttributes(dynamicAttributes);
        session.addSubSession(subSession);

        runTest(session);
    }

    private void runTest(Session session) throws Exception {
        checkSerializer(session);

        checkBinaryFormatSerializer(session);

        StringBuffer buf = new StringBuffer();
        Map<String, Object> initialAttributes = new HashMap<String, Object>();
        initialAttributes.put("11", buf.toString());
        session.setInitialAttributes(initialAttributes);
        for (int i = 0; i < 1000; i++) {
            buf.append(i % 10);

            initialAttributes.put("11", buf.toString());
            ByteBuffer metaData = BinaryFormatSerializer.getInstance().getSessionMetadata(session);
            Session t = new Session();
            BinaryFormatSerializer.getInstance().setSessionMetadata(t, metaData);
            Arrays.equals(metaData.array(), BinaryFormatSerializer.getInstance().getSessionMetadata(t).array());
            checkSerializer(session);
        }

        Map<String, Object> dynamicAttributes = new HashMap<String, Object>();
        dynamicAttributes.put("11", buf.toString());
        session.setDynamicAttributes(dynamicAttributes);
        for (int i = 0; i < 1000; i++) {
            buf.append(i % 10);

            dynamicAttributes.put("11", buf.toString());
            ByteBuffer payload = BinaryFormatSerializer.getInstance().getSessionPayload(session);
            Session t = new Session();
            BinaryFormatSerializer.getInstance().setSessionPayload(t, payload);
            Arrays.equals(payload.array(), BinaryFormatSerializer.getInstance().getSessionPayload(t).array());
            checkSerializer(session);
        }
    }

    private void checkBinaryFormatSerializer(Session session) {
        ByteBuffer metaData1 = BinaryFormatSerializer.getInstance().getSessionMetadata(session);
        ByteBuffer payload1 = BinaryFormatSerializer.getInstance().getSessionPayload(session);

        Session session3 = new Session();
        BinaryFormatSerializer.getInstance().setSessionMetadata(session3, metaData1);
        BinaryFormatSerializer.getInstance().setSessionPayload(session3, payload1);

        Arrays.equals(metaData1.array(), BinaryFormatSerializer.getInstance().getSessionMetadata(session3).array());
        Arrays.equals(payload1.array(), BinaryFormatSerializer.getInstance().getSessionPayload(session3).array());
    }

    private void checkSerializer(Session session) throws Exception {
        SessionSerializer s = new SessionSerializer();
        ByteBuffer valueBuffer = s.serialize(session);

        Session session2 = s.deserialize(valueBuffer, 0, valueBuffer.limit());
        ReflectionAssert.assertReflectionEquals(session, session2);
    }
}
