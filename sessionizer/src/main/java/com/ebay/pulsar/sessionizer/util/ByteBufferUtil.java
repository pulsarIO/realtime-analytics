/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.util;

import java.nio.ByteBuffer;

/**
 * Use thread local bytebuffer to reduce memory footprint.
 * 
 * @author xingwang
 *
 */
public final class ByteBufferUtil {
    private ByteBufferUtil() {
        //Utility class
    }
    private static final ThreadLocal<ByteBuffer> TMP_BUFFER = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(4096);
        }
    };

    /**
     * Return the thread local byte buffer.
     * 
     * @return
     */
    public static ByteBuffer getThreadLocalByteBuffer() {
        return TMP_BUFFER.get();
    }

    /**
     * Double the thread local buffer capacity.
     * 
     * @return
     */
    public static ByteBuffer enlargeThreadLocalByteBuffer() {
        TMP_BUFFER.set(ByteBuffer
                .allocate(TMP_BUFFER.get().capacity() * 2));
        return TMP_BUFFER.get();
    }
}
