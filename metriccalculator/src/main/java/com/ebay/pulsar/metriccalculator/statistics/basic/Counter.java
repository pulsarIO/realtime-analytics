/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.statistics.basic;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;

public class Counter implements Externalizable {
    private AtomicLong totalValue = new AtomicLong(0);
    private long oldsnapshotValue;
    private long snapshotValue;
    private String lastCounterTime;

    public Counter() {
    }

    public long getOldsnapshotValue() {
        return oldsnapshotValue;
    }

    public void setOldsnapshotValue(long oldsnapshotValue) {
        this.oldsnapshotValue = oldsnapshotValue;
    }

    public long getSnapshotValue() {
        return snapshotValue;
    }

    public void setSnapshotValue(long snapshotValue) {
        this.snapshotValue = snapshotValue;
    }

    public void setTotalValue(AtomicLong totalValue) {
        this.totalValue = totalValue;
    }

    public void inc() {
        totalValue.incrementAndGet();
    }

    public long getAndIncrement() {
        return totalValue.getAndIncrement();
    }

    public void inc(long v) {
        totalValue.addAndGet(v);
    }

    public long getLastDeltaValue() {
        return (snapshotValue - oldsnapshotValue);
    }

    public void mark() {
        oldsnapshotValue = snapshotValue;
        snapshotValue = totalValue.get();
    }

    public AtomicLong getTotalValue() {
        return totalValue;
    }

    public String getLastCounterTime() {
        return lastCounterTime;
    }

    public void setLastCounterTime(String lastCounterTime) {
        this.lastCounterTime = lastCounterTime;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(totalValue.get());
        out.writeLong(oldsnapshotValue);
        out.writeLong(snapshotValue);
        out.writeObject(lastCounterTime);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        totalValue = new AtomicLong(in.readLong());
        oldsnapshotValue = in.readLong();
        snapshotValue = in.readLong();
        Object obj = in.readObject();
        if (obj != null)
            lastCounterTime = (String) obj;
    }
}