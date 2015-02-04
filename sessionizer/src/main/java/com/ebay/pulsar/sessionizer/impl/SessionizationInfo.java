/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.impl;

/**
 * Hints for sessionizer processor.
 * 
 * @author xingwang
 *
 */
public class SessionizationInfo {
    private int ttl = 30 * 60 * 1000;
    private long timestamp;
    private String identifier;
    private String name;
    public String getIdentifier() {
        return identifier;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getTtl() {
        return ttl;
    }
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
