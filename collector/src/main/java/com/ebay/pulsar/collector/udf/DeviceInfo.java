/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.udf;

public class DeviceInfo {

    private String deviceCategory;
    private String osFamily;
    private String osVersion;
    private String userAgent;
    private String userAgentType;
    private String userAgentFamily;
    private String userAgentVersion;

    public String getDeviceCategory() {
        return deviceCategory;
    }
    public void setDeviceCategory(String deviceCategory) {
        this.deviceCategory = deviceCategory;
    }
    public String getOsFamily() {
        return osFamily;
    }
    public void setOsFamily(String osFamily) {
        this.osFamily = osFamily;
    }
    public String getOsVersion() {
        return osVersion;
    }
    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }
    public String getUserAgent() {
        return userAgent;
    }
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
    public String getUserAgentType() {
        return userAgentType;
    }
    public void setUserAgentType(String userAgentType) {
        this.userAgentType = userAgentType;
    }
    public String getUserAgentFamily() {
        return userAgentFamily;
    }
    public void setUserAgentFamily(String userAgentFamily) {
        this.userAgentFamily = userAgentFamily;
    }
    public String getUserAgentVersion() {
        return userAgentVersion;
    }
    public void setUserAgentVersion(String userAgentVersion) {
        this.userAgentVersion = userAgentVersion;
    }
}