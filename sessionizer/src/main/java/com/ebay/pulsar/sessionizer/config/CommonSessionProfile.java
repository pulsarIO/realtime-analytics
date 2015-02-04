/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.config;

import java.util.List;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.esper.EPL;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * Common config for both main session and sub session.
 * 
 * @author xingwang
 *
 */
public class CommonSessionProfile extends AbstractNamedBean implements XSerializable {

    private String name;
    private String beginMarker;
    private String endMarker;
    private String sessionIdKey = "_snid";
    private int defaultTtl = 30 * 60 * 1000;

    private EPL epl;
    private List<String> imports;
    /**
     * Event type for session begin event.
     * 
     * @return
     */
    public String getBeginMarker() {
        return beginMarker;
    }

    /**
     * Default ttl for the session.
     * 
     * @return
     */
    public int getDefaultTtl() {
        return defaultTtl;
    }

    /**
     * Event type for session end event.
     * 
     * @return
     */
    public String getEndMarker() {
        return endMarker;
    }

    /**
     * Epl for the sessionization.
     * 
     * @return
     */
    public EPL getEpl() {
        return epl;
    }

    /**
     * UDF imports for EPL.
     * 
     * @return
     */
    public List<String> getImports() {
        return imports;
    }

    /**
     * Name of the profile, should be unique in the context.
     * 
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Customize the key for session id.
     * 
     * Default is _snid.
     * 
     * @return
     */
    public String getSessionIdKey() {
        return sessionIdKey;
    }

    public void setBeginMarker(String beginMarker) {
        this.beginMarker = beginMarker;
    }
    public void setDefaultTtl(int defaultTtl) {
        this.defaultTtl = defaultTtl;
    }
    public void setEndMarker(String endMarker) {
        this.endMarker = endMarker;
    }
    public void setEpl(EPL epl) {
        this.epl = epl;
    }
    public void setImports(List<String> imports) {
        this.imports = imports;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setSessionIdKey(String sessionIdKey) {
        this.sessionIdKey = sessionIdKey;
    }
}
