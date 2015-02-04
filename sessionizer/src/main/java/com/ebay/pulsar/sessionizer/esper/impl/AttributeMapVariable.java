/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper for Esper variable.
 * 
 * @author xingwang
 *
 */
public class AttributeMapVariable {
    private Map<String, Object> attributes;

    @SuppressWarnings("unchecked")
    private <T> T getObject(String name, Class<T> clazz) {
        if (attributes != null) {
            Object object = attributes.get(name);
            if (object != null) {
                if (clazz.isAssignableFrom(object.getClass())) {
                    return (T) object;
                } else {
                    return null;
                }
            } else {
                return null;
            }

        } else {
            return null;
        }
    }

    public Boolean getBoolean(String name) {
        return getObject(name, Boolean.class);
    }

    public Byte getByte(String name) {
        return getObject(name, Byte.class);
    }

    public Character getCharacter(String name) {
        return getObject(name, Character.class);
    }

    public Date getDate(String name) {
        return getObject(name, Date.class);
    }

    public Double getDouble(String name) {
        return getObject(name, Double.class);
    }

    public Float getFloat(String name) {
        return getObject(name, Float.class);
    }

    public Integer getInt(String name) {
        return getObject(name, Integer.class);
    }

    @SuppressWarnings({"unchecked" })
    public List<Object> getList(String name) {
        return getObject(name, List.class);
    }

    public Long getLong(String name) {
        return getObject(name, Long.class);
    }

    public Short getShort(String name) {
        return getObject(name, Short.class);
    }

    public String getString(String name) {
        return getObject(name, String.class);
    }

    public void resetAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public boolean contains(String name) {
        return attributes.get(name) != null;
    }

    public boolean contains(String name, Object v) {
        List<Object> list = getList(name);
        if (list == null) {
            return false;
        } else {
            return list.contains(v);
        }
    }

    public Map<String, Object> filter(String[] names) {
        if (names == null) {
            return attributes;
        }
        Map<String, Object>  m = new HashMap<String, Object>();
        for (String name : names) {
            Object v = attributes.get(name);
            if (v != null) {
                m.put(name, v);
            }
        }
        return m;
    }

}
