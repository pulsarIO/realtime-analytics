/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.offheap.serializer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;
import com.ebay.jetstream.util.offheap.serializer.util.UnsignedLongEncoder;
import com.ebay.pulsar.metriccalculator.statistics.basic.Counter;

public class CounterSerializer implements OffHeapSerializer<Counter> {

    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };

    private final UnsignedLongEncoder encoder = new UnsignedLongEncoder();
    private final AsciiFirstStringEncoder stringEncoder = new AsciiFirstStringEncoder();

    @Override
    public Counter deserialize(ByteBuffer data, int pos, int length) {
        Counter counter = new Counter();
        counter.setTotalValue(new AtomicLong(encoder.decode(data)));
        counter.setSnapshotValue(encoder.decode(data));
        counter.setOldsnapshotValue(encoder.decode(data));
        counter.setLastCounterTime(stringEncoder.decode(data));
        return counter;
    }

    @Override
    public ByteBuffer serialize(Counter v) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        while (true) {
            try {
                encoder.encode(v.getTotalValue().get(), buffer);
                encoder.encode(v.getSnapshotValue(), buffer);
                encoder.encode(v.getOldsnapshotValue(), buffer);
                stringEncoder.encode(v.getLastCounterTime(), buffer);
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
