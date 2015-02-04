/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.offheap.serializer;


import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedLongEncoder;
import com.ebay.pulsar.metriccalculator.statistics.basic.AvgCounter;

public class AvgCounterSerializer implements OffHeapSerializer<AvgCounter> {
    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };

    private final UnsignedLongEncoder encoder = new UnsignedLongEncoder();

    @Override
    public AvgCounter deserialize(ByteBuffer data, int pos, int length) {
        AvgCounter counter = new AvgCounter();
        counter.setTotalTime(new AtomicLong(encoder.decode(data)));
        counter.setSnapshotTimeValue(encoder.decode(data));
        counter.setOldsnapshotTimeValue(encoder.decode(data));
        counter.setTotalValue(new AtomicLong(encoder.decode(data)));
        counter.setSnapshotValue(encoder.decode(data));
        counter.setOldsnapshotValue(encoder.decode(data));
        return counter;
    }

    @Override
    public ByteBuffer serialize(AvgCounter v) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        while (true) {
            try {
                encoder.encode(v.getTotalTime().get(), buffer);
                encoder.encode(v.getSnapshotTimeValue(), buffer);
                encoder.encode(v.getOldsnapshotTimeValue(), buffer);

                encoder.encode(v.getTotalValue().get(), buffer);
                encoder.encode(v.getSnapshotValue(), buffer);
                encoder.encode(v.getOldsnapshotValue(), buffer);
                buffer.flip();
                break;
            } catch (BufferOverflowException ex) {
                buffer = ByteBuffer.allocate(buffer.capacity() * 2);
                keyBuffer.set(buffer);
            }
        }
        return buffer;
    }
}
