/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cache.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.jetstream.util.offheap.serializer.util.PrimitiveTLVEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedIntEncoder;

/**
 * Encoder for byte map used by session.
 * 
 * Thread-safe.
 * 
 * @author xingwang
 *
 */
class LocalSessionAttributeMapEncoder {
    static class Mapping {
        private final List<String> idMapping = new ArrayList<String>();
        private final Map<String, Integer> nameMapping = new HashMap<String, Integer>();

        public Integer getId(String name) {
            Integer id = nameMapping.get(name);
            if (id == null) {
                id = idMapping.size();
                idMapping.add(name);
                nameMapping.put(name, id);
            }
            return id;
        }

        public String getName(Integer id) {
            return idMapping.get(id);
        }
    }

    private final PrimitiveTLVEncoder encoder = new PrimitiveTLVEncoder();
    private final UnsignedIntEncoder unsignedIntEncoder = new UnsignedIntEncoder();
    public Map<String, Object> decode(ByteBuffer buffer, Mapping mapping) {
        int length = buffer.getInt();
        if (length == 0) {
            return null;
        }
        Map<String, Object> attributes = new HashMap<String, Object>(length, 2.0f); //avoid rehash
        for (int i = 0, t = length; i < t; i++) {
            Integer id = unsignedIntEncoder.decode(buffer);
            String name = mapping.getName(id);
            attributes.put(name, encoder.decode(buffer));
        }
        return attributes;
    }

    public void encode(Map<String, Object> dynamicAttributes, ByteBuffer buffer, Mapping mapping) {
        if (dynamicAttributes == null || dynamicAttributes.isEmpty()) {
            buffer.putInt(0); // EMPTY
        } else {
            int position = buffer.position();
            buffer.putInt(0); //place holder.
            int count = 0;
            for (Map.Entry<String, Object> e : dynamicAttributes.entrySet()) {
                Object value = e.getValue();
                if (value != null) {
                    Integer id = mapping.getId(e.getKey());
                    unsignedIntEncoder.encode(id, buffer);
                    encoder.encode(value, buffer);
                    count ++;
                }
            }
            buffer.putInt(position, count);
        }
    }
}
