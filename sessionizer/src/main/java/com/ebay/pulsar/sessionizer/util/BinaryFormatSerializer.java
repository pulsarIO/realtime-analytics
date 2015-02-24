/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedIntEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedLongEncoder;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;

/**
 * Serialize the Session to binary format.
 * 
 * Serialize the session into the bytes and reconstruct from bytes to session. It
 * has a version support and reject the data if version is not match.
 * 
 * @author xingwang
 *
 */
public final class BinaryFormatSerializer {
    private static final byte SESSION_VERSION = 0;
    private static final BinaryFormatSerializer INSTANCE = new BinaryFormatSerializer();
    public static BinaryFormatSerializer getInstance() {
        return INSTANCE;
    }

    private final AsciiFirstStringEncoder encoder = new AsciiFirstStringEncoder();
    private final SessionAttributeMapEncoder bytePrimitiveEncoder = new SessionAttributeMapEncoder();
    private final UnsignedLongEncoder unsignedLongEncoder = new UnsignedLongEncoder();
    private final UnsignedIntEncoder unsignedIntEncoder = new UnsignedIntEncoder();

    private BinaryFormatSerializer () {
        // singletion
    }

    /**
     * Serialize the session metadata to a bytebuffer.
     * 
     * The bytebuffer will be reused, the caller should copy out the contents
     * when pass the content.
     * 
     * @param session
     * @return
     */
    public ByteBuffer getSessionMetadata(Session session) {
        ByteBuffer buffer = ByteBufferUtil.getThreadLocalByteBuffer();
        while (true) {
            try {
                buffer.clear();

                buffer.put(SESSION_VERSION);
                bytePrimitiveEncoder.encode(session.getInitialAttributes(), buffer);

                buffer.flip();
                break;
            } catch (BufferOverflowException ex) {
                buffer = ByteBufferUtil.enlargeThreadLocalByteBuffer();
            }
        }

        ByteBuffer bufferCopy = ByteBuffer.allocate(buffer.limit());
        bufferCopy.put(buffer);
        bufferCopy.clear();
        return bufferCopy;
    }

    /**
     * Serialize the session payload to a bytebuffer.
     * 
     * The bytebuffer will be reused, the caller should copy out the contents
     * when pass the content.
     * 
     * @param session
     * @return
     */
    public ByteBuffer getSessionPayload(Session session) {
        ByteBuffer buffer = ByteBufferUtil.getThreadLocalByteBuffer();
        while (true) {
            try {
                buffer.clear();


                buffer.put(SESSION_VERSION);

                unsignedLongEncoder.encode(session.getFirstEventTimestamp(), buffer);

                unsignedLongEncoder.encode(session.getCreationTime(), buffer);
                unsignedLongEncoder.encode(session.getLastModifiedTime(), buffer);
                unsignedLongEncoder.encode(session.getExpirationTime(), buffer);
                unsignedLongEncoder.encode(session.getFirstExpirationTime(), buffer);
                unsignedIntEncoder.encode(session.getTtl(), buffer);
                unsignedIntEncoder.encode(session.getEventCount(), buffer);
                unsignedIntEncoder.encode(session.getBotEventCount(), buffer);
                unsignedIntEncoder.encode(session.getBotType(), buffer);
                unsignedIntEncoder.encode(session.getVersion(), buffer);

                encoder.encode(session.getAffinityKey(), buffer);
                unsignedLongEncoder.encode(session.getMetadataLastModifiedTime(), buffer);
                encoder.encode(session.getRemoteServerInfo(), buffer);
                bytePrimitiveEncoder.encode(session.getDynamicAttributes(), buffer);
                List<SubSession> subSessions = session.getSubSessions();
                if (subSessions == null || subSessions.isEmpty()) {
                    buffer.putInt(-1);
                } else {
                    buffer.putInt(subSessions.size());
                    for (int i = 0, t = subSessions.size(); i < t; i++) {
                        SubSession sub = subSessions.get(i);
                        encoder.encode(sub.getIdentifier(), buffer);
                        encoder.encode(sub.getName(), buffer);

                        unsignedLongEncoder.encode(sub.getFirstEventTimestamp(), buffer);

                        unsignedLongEncoder.encode(sub.getCreationTime(), buffer);
                        unsignedLongEncoder.encode(sub.getLastModifiedTime(), buffer);
                        unsignedLongEncoder.encode(sub.getExpirationTime(), buffer);
                        unsignedIntEncoder.encode(sub.getTtl(), buffer);

                        buffer.putInt(sub.getEventCount());
                        bytePrimitiveEncoder.encode(sub.getDynamicAttributes(), buffer);
                        bytePrimitiveEncoder.encode(sub.getInitialAttributes(), buffer);
                    }
                }

                buffer.flip();
                break;
            } catch (BufferOverflowException ex) {
                buffer = ByteBufferUtil.enlargeThreadLocalByteBuffer();
            }
        }
        ByteBuffer bufferCopy = ByteBuffer.allocate(buffer.limit());
        bufferCopy.put(buffer);
        bufferCopy.clear();
        return bufferCopy;
    }


    /**
     * Deserialize the metadata and set the content to the session.
     * 
     * Return false when version did not match.
     * 
     * @param session
     * @param metaData
     * @return
     */
    public boolean setSessionMetadata(Session session, ByteBuffer metaData) {
        byte version = metaData.get();
        if (version != SESSION_VERSION) {
            return false;
        }
        session.setInitialAttributes(bytePrimitiveEncoder.decode(metaData));
        return true;
    }

    /**
     * Deserialize the payload and set the content to the session.
     * 
     * Return false when version did not match.
     * 
     * @param session
     * @param metaData
     * @return
     */
    public boolean setSessionPayload(Session session, ByteBuffer payload) {
        byte version = payload.get();
        if (version != SESSION_VERSION) {
            return false;
        }
        session.setFirstEventTimestamp(unsignedLongEncoder.decode(payload));

        session.setCreationTime(unsignedLongEncoder.decode(payload));
        session.setLastModifiedTime(unsignedLongEncoder.decode(payload));
        session.setExpirationTime(unsignedLongEncoder.decode(payload));
        session.setFirstExpirationTime(unsignedLongEncoder.decode(payload));
        session.setTtl(unsignedIntEncoder.decode(payload));
        session.setEventCount(unsignedIntEncoder.decode(payload));
        session.setBotEventCount(unsignedIntEncoder.decode(payload));
        session.setBotType(unsignedIntEncoder.decode(payload));
        session.setVersion(unsignedIntEncoder.decode(payload));

        session.setAffinityKey(encoder.decode(payload));
        session.setMetadataLastModifiedTime(unsignedLongEncoder.decode(payload));
        session.setRemoteServerInfo(encoder.decode(payload));
        session.setDynamicAttributes(bytePrimitiveEncoder.decode(payload));

        int subSize = payload.getInt();
        if (subSize != -1) {
            List<SubSession> subSessions = new ArrayList<SubSession>(subSize);
            for (int i = 0; i < subSize; i++) {
                SubSession sub = new SubSession();
                sub.setIdentifier(encoder.decode(payload));
                sub.setName(encoder.decode(payload));
                sub.setFirstEventTimestamp(unsignedLongEncoder.decode(payload));

                sub.setCreationTime(unsignedLongEncoder.decode(payload));
                sub.setLastModifiedTime(unsignedLongEncoder.decode(payload));
                sub.setExpirationTime(unsignedLongEncoder.decode(payload));
                sub.setTtl(unsignedIntEncoder.decode(payload));

                sub.setEventCount(payload.getInt());
                sub.setDynamicAttributes(bytePrimitiveEncoder.decode(payload));
                sub.setInitialAttributes(bytePrimitiveEncoder.decode(payload));
                subSessions.add(sub);
            }
            session.setSubSessions(subSessions);
        }
        return true;
    }
}
