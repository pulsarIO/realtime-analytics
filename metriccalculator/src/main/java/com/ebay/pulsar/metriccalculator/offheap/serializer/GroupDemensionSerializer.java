/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.offheap.serializer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;
import com.ebay.pulsar.metriccalculator.metric.MCMetricGroupDemension;

/**
 * An offheap serializer implementation for GroupDemension.
 * 
 */
public class GroupDemensionSerializer implements
        OffHeapSerializer<MCMetricGroupDemension> {
    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };

    private final AsciiFirstStringEncoder stringEncoder = new AsciiFirstStringEncoder();

    @Override
    public MCMetricGroupDemension deserialize(ByteBuffer data, int pos,
            int length) {

        String metrciName = stringEncoder.decode(data);
        String groupId = stringEncoder.decode(data);
        int size = data.getInt();
        Map<String, String> demensions = null;
        if (size > 0) {
            demensions = new HashMap<String, String>();
            for (int i = 0; i < size; i++) {
                demensions.put(stringEncoder.decode(data),
                        stringEncoder.decode(data));
            }
        }
        MCMetricGroupDemension groupDemension = null;
        if (demensions != null)
            groupDemension = new MCMetricGroupDemension(metrciName, groupId,
                    demensions);
        else
            groupDemension = new MCMetricGroupDemension(metrciName, groupId);
        return groupDemension;
    }

    @Override
    public ByteBuffer serialize(MCMetricGroupDemension groupDemension) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        while (true) {
            try {
                stringEncoder.encode(groupDemension.getMetricName(), buffer);
                stringEncoder.encode(groupDemension.getGroupId(), buffer);
                Map<String, String> demensions = groupDemension.getDimensions();
                if (demensions != null) {
                    buffer.putInt(groupDemension.getDimensions().size());
                } else {
                    buffer.putInt(-1);
                }
                if (demensions != null) {
                    Iterator<Entry<String, String>> itr = demensions.entrySet()
                            .iterator();
                    while (itr.hasNext()) {
                        Map.Entry<String, String> entry1 = itr.next();
                        stringEncoder.encode(entry1.getKey(), buffer);
                        stringEncoder.encode(entry1.getValue(), buffer);
                    }
                }
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
