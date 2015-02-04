/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.statistics.basic;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;

public final class AvgCounter extends Counter implements Externalizable {
    private AtomicLong totalTime = new AtomicLong(0);
    private long oldsnapshotTimeValue;
    private long snapshotTimeValue;

    public AvgCounter() {
    }

    public void inc(long v, long time) {
        super.inc(v);
        totalTime.addAndGet(time);
    }

    public void mark() {
        super.mark();
        oldsnapshotTimeValue = snapshotTimeValue;
        snapshotTimeValue = totalTime.get();
    }

    public long getAvgValue() {
        long c = super.getTotalValue().get();
        if (c > 0) {
            return totalTime.get() / c;
        } else {
            return 0L;
        }
    }

    public AtomicLong getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(AtomicLong totalTime) {
        this.totalTime = totalTime;
    }

    public long getOldsnapshotTimeValue() {
        return oldsnapshotTimeValue;
    }

    public void setOldsnapshotTimeValue(long oldsnapshotTimeValue) {
        this.oldsnapshotTimeValue = oldsnapshotTimeValue;
    }

    public long getSnapshotTimeValue() {
        return snapshotTimeValue;
    }

    public void setSnapshotTimeValue(long snapshotTimeValue) {
        this.snapshotTimeValue = snapshotTimeValue;
    }

    public long getLatestAvgValue() {
        if (super.getLastDeltaValue() > 0) {
            return (snapshotTimeValue - oldsnapshotTimeValue)
                    / super.getLastDeltaValue();
        } else {
            return 0L;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(totalTime.get());
        out.writeLong(oldsnapshotTimeValue);
        out.writeLong(snapshotTimeValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        totalTime = new AtomicLong(in.readLong());
        oldsnapshotTimeValue = in.readLong();
        snapshotTimeValue = in.readLong();
    }
}