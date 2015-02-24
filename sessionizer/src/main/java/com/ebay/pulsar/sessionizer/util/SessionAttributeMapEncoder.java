/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.PrimitiveTLVEncoder;

/**
 * Encoder for session attributes.
 * 
 * It is thread-safe.
 * 
 * @author xingwang
 *
 */
public class SessionAttributeMapEncoder {
    private final PrimitiveTLVEncoder encoder = new PrimitiveTLVEncoder();
    private final AsciiFirstStringEncoder stringEncoder = new AsciiFirstStringEncoder();
    public Map<String, Object> decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length == 0) {
            return null;
        }
        Map<String, Object> attributes = new HashMap<String, Object>(length, 2.0f); //avoid rehash
        for (int i = 0, t = length; i < t; i++) {
            String id = stringEncoder.decode(buffer);
            attributes.put(id, encoder.decode(buffer));
        }
        return attributes;
    }

    public void encode(Map<String, Object> dynamicAttributes, ByteBuffer buffer) {
        if (dynamicAttributes == null || dynamicAttributes.isEmpty()) {
            buffer.putInt(0); // EMPTY
        } else {
            int position = buffer.position();
            buffer.putInt(0); //place holder.
            int count = 0;
            for (Map.Entry<String, Object> e : dynamicAttributes.entrySet()) {
                Object value = e.getValue();
                if (value != null) {
                    stringEncoder.encode(e.getKey(), buffer);
                    encoder.encode(value, buffer);
                    count ++;
                }
            }
            buffer.putInt(position, count);
        }
    }
}
