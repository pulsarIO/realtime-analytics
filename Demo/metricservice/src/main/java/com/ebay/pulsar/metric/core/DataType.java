/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

public enum DataType {

    RAW, MAX, MIN, AVG;

    public static DataType valueOf(int type) {
        switch (type) {
        case 0:  return RAW;
        case 1 : return MAX;
        case 2 : return MIN;
        case 3 : return AVG;
        default: throw new IllegalArgumentException(type + " is not a supported " +
            DataType.class.getSimpleName());
        }
    }

}
