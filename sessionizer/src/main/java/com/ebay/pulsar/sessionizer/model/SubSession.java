/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.model;

/**
 * AppSession POJO.
 * 
 * It is a sub session of the Session.
 * 
 * @author xingwang
 */
public class SubSession extends AbstractSession {

    private String name;

    /**
     * Sub session name.
     * 
     * @return
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
