/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cache.impl;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedIntEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedLongEncoder;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;

/**
 * Offheap serializer for session object.
 * 
 * @author xingwang
 *
 */
public final class SessionSerializer implements OffHeapSerializer<Session> {
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private ByteBuffer valueBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    private final AsciiFirstStringEncoder encoder = new AsciiFirstStringEncoder();
    private final LocalSessionAttributeMapEncoder bytePrimitiveEncoder = new LocalSessionAttributeMapEncoder();
    private final LocalSessionAttributeMapEncoder.Mapping initialAttributeMapping = new LocalSessionAttributeMapEncoder.Mapping();
    private final LocalSessionAttributeMapEncoder.Mapping dynamiceAttributeMapping = new LocalSessionAttributeMapEncoder.Mapping();
    private final UnsignedLongEncoder unsignedLongEncoder = new UnsignedLongEncoder();
    private final UnsignedIntEncoder unsignedIntEncoder = new UnsignedIntEncoder();
    private int maxSessionSize = 0;

    @Override
    public Session deserialize(ByteBuffer buf, int startPos, int valueLength) {
        Session v = new Session();
        v.setFirstEventTimestamp(unsignedLongEncoder.decode(buf));

        v.setCreationTime(unsignedLongEncoder.decode(buf));
        v.setLastModifiedTime(unsignedLongEncoder.decode(buf));
        v.setExpirationTime(unsignedLongEncoder.decode(buf));
        v.setFirstExpirationTime(unsignedLongEncoder.decode(buf));
        v.setTtl(unsignedIntEncoder.decode(buf));
        v.setEventCount(unsignedIntEncoder.decode(buf));
        v.setBotEventCount(unsignedIntEncoder.decode(buf));
        v.setBotType(unsignedIntEncoder.decode(buf));
        v.setVersion(unsignedIntEncoder.decode(buf));

        v.setAffinityKey(encoder.decode(buf));
        v.setMetadataLastModifiedTime(unsignedLongEncoder.decode(buf));
        v.setRemoteServerInfo(encoder.decode(buf));
        readBlob(v, buf);
        return v;
    }

    public int getMaxSessionSize() {
        return maxSessionSize;
    }

    private void readBlob(Session v, ByteBuffer buffer) {
        int subSize = buffer.getInt();
        if (subSize != -1) {
            List<SubSession> subSessions = new ArrayList<SubSession>(subSize);
            for (int i = 0; i < subSize; i++) {
                SubSession sub = new SubSession();
                sub.setIdentifier(encoder.decode(buffer));
                sub.setName(initialAttributeMapping.getName(unsignedIntEncoder.decode(buffer)));
                sub.setFirstEventTimestamp(unsignedLongEncoder.decode(buffer));

                sub.setCreationTime(unsignedLongEncoder.decode(buffer));
                sub.setLastModifiedTime(unsignedLongEncoder.decode(buffer));
                sub.setExpirationTime(unsignedLongEncoder.decode(buffer));
                sub.setTtl(unsignedIntEncoder.decode(buffer));

                sub.setEventCount(buffer.getInt());
                sub.setDynamicAttributes(bytePrimitiveEncoder.decode(buffer, dynamiceAttributeMapping));
                sub.setInitialAttributes(bytePrimitiveEncoder.decode(buffer, initialAttributeMapping));
                subSessions.add(sub);
            }
            v.setSubSessions(subSessions);
        }
        v.setDynamicAttributes(bytePrimitiveEncoder.decode(buffer, dynamiceAttributeMapping));
        v.setInitialAttributes(bytePrimitiveEncoder.decode(buffer, initialAttributeMapping));
    }

    @Override
    public ByteBuffer serialize(Session v) {
        ByteBuffer buf = valueBuffer;
        buf.clear();
        while (true) {
            try {
                unsignedLongEncoder.encode(v.getFirstEventTimestamp(), buf);

                unsignedLongEncoder.encode(v.getCreationTime(), buf);
                unsignedLongEncoder.encode(v.getLastModifiedTime(), buf);
                unsignedLongEncoder.encode(v.getExpirationTime(), buf);
                unsignedLongEncoder.encode(v.getFirstExpirationTime(), buf);
                unsignedIntEncoder.encode(v.getTtl(), buf);
                unsignedIntEncoder.encode(v.getEventCount(), buf);
                unsignedIntEncoder.encode(v.getBotEventCount(), buf);
                unsignedIntEncoder.encode(v.getBotType(), buf);
                unsignedIntEncoder.encode(v.getVersion(), buf);

                encoder.encode(v.getAffinityKey(), buf);
                unsignedLongEncoder.encode(v.getMetadataLastModifiedTime(), buf);
                encoder.encode(v.getRemoteServerInfo(), buf);
                writeBlob(v, buf);
                buf.flip();
                maxSessionSize = Math.max(maxSessionSize, buf.limit());
                break;
            } catch (BufferOverflowException ex) {
                buf = ByteBuffer.allocate(buf.capacity() * 2);
                valueBuffer = buf;
            }
        }
        return buf;
    }



    private void writeBlob(Session v, ByteBuffer buffer) {
        List<SubSession> subSessions = v.getSubSessions();
        if (subSessions == null || subSessions.isEmpty()) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(subSessions.size());
            for (int i = 0, t = subSessions.size(); i < t; i++) {
                SubSession sub = subSessions.get(i);
                encoder.encode(sub.getIdentifier(), buffer);
                unsignedIntEncoder.encode(initialAttributeMapping.getId(sub.getName()), buffer);

                unsignedLongEncoder.encode(sub.getFirstEventTimestamp(), buffer);

                unsignedLongEncoder.encode(sub.getCreationTime(), buffer);
                unsignedLongEncoder.encode(sub.getLastModifiedTime(), buffer);
                unsignedLongEncoder.encode(sub.getExpirationTime(), buffer);
                unsignedIntEncoder.encode(sub.getTtl(), buffer);

                buffer.putInt(sub.getEventCount());
                bytePrimitiveEncoder.encode(sub.getDynamicAttributes(), buffer, dynamiceAttributeMapping);
                bytePrimitiveEncoder.encode(sub.getInitialAttributes(), buffer, initialAttributeMapping);
            }
        }
        bytePrimitiveEncoder.encode(v.getDynamicAttributes(), buffer, dynamiceAttributeMapping);
        bytePrimitiveEncoder.encode(v.getInitialAttributes(), buffer, initialAttributeMapping);

    }

    public void resetMaxItemSize() {
        maxSessionSize = 0;
    }
}