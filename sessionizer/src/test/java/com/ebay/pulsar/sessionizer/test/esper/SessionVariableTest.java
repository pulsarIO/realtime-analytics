/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.test.esper;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.ebay.pulsar.sessionizer.esper.impl.SessionVariable;
import com.ebay.pulsar.sessionizer.model.Session;

/**
 * Test for session variable.
 * 
 * @author xingwang
 *
 */
public class SessionVariableTest {

    @Test
    public void test() throws Exception {
        Session s = new Session();
        SessionVariable var = new SessionVariable();
        var.resetSessionData(s, s);

        s.setCreationTime(System.currentTimeMillis());
        s.setEventCount(123);
        s.setFirstEventTimestamp(s.getCreationTime() + 1);
        s.setIdentifier("sdfdsfsdf");
        s.setExpirationTime(s.getCreationTime() + 300000);

        Assert.assertEquals(var.getCreationTime(), s.getCreationTime());
        Assert.assertEquals(var.getEventCount(), s.getEventCount());
        Assert.assertEquals(var.getExpirationTime(), s.getExpirationTime());
        Assert.assertEquals(var.getFirstEventTimestamp(), s.getFirstEventTimestamp());
        Assert.assertEquals(var.getIdentifier(), s.getIdentifier());
        Assert.assertEquals(var.getLastModifiedTime(), s.getLastModifiedTime());

        HashMap<String, Object> attrs = new HashMap<String, Object>();
        s.setDynamicAttributes(attrs);
        var.resetAttributes(s.getDynamicAttributes());

        attrs.put("1", Boolean.TRUE);
        attrs.put("2", (byte) 12);
        attrs.put("3", 'a');
        attrs.put("4", new Date());
        attrs.put("5", 1.0);
        attrs.put("6", 5.0f);
        attrs.put("7", new ArrayList<Object>());
        attrs.put("9", "1232323");
        attrs.put("10", Integer.MAX_VALUE);
        attrs.put("11",  (short) 1);
        attrs.put("12",  Long.MAX_VALUE);
        Assert.assertEquals(var.getBoolean("a1"), s.getDynamicAttributes().get(1));
        Assert.assertEquals(var.getByte("a2"), s.getDynamicAttributes().get(2));
        Assert.assertEquals(var.getCharacter("a3"), s.getDynamicAttributes().get(3));
        Assert.assertEquals(var.getDate("a4"), s.getDynamicAttributes().get(4));
        Assert.assertEquals(var.getDouble("a5"), s.getDynamicAttributes().get(5));
        Assert.assertEquals(var.getFloat("a6"), s.getDynamicAttributes().get(6));
        Assert.assertEquals(var.getList("a7"), s.getDynamicAttributes().get(7));
        Assert.assertEquals(var.getString("a9"), s.getDynamicAttributes().get(9));
        Assert.assertEquals(var.getInt("a10"), s.getDynamicAttributes().get(10));
        Assert.assertEquals(var.getShort("a11"), s.getDynamicAttributes().get(11));
        Assert.assertEquals(var.getLong("a12"), s.getDynamicAttributes().get(12));
    }
}