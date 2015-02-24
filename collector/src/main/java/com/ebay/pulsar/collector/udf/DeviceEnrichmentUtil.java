/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.udf;

import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.springframework.beans.factory.InitializingBean;

public class DeviceEnrichmentUtil implements InitializingBean {
    private static UserAgentStringParser parser;

    private static final DeviceEnrichmentUtil INSTANCE = new DeviceEnrichmentUtil();

    public static DeviceEnrichmentUtil getInstance() {
        return INSTANCE;
    }

    private DeviceEnrichmentUtil() {
        //singleton
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        parser = UADetectorServiceFactory.getCachingAndUpdatingParser();
    }

    public static DeviceInfo getDeviceInfo(String userAgent) {
        DeviceInfo deviceInfo = new DeviceInfo();
        if (userAgent == null) {
            return deviceInfo;
        }

        ReadableUserAgent agent = parser.parse(userAgent);

        String deviceCategory = agent.getDeviceCategory().getName();

        OperatingSystem operatingSystem = agent.getOperatingSystem();
        String osFamily = operatingSystem.getFamilyName();
        String osVersion = operatingSystem.getVersionNumber().toVersionString();


        String userAgentName =  agent.getName();
        String userAgentType = agent.getTypeName();
        String userAgentFamily = agent.getFamily().getName();
        String userAgentVersion = agent.getVersionNumber().toVersionString();

        deviceInfo.setDeviceCategory(deviceCategory);
        deviceInfo.setOsFamily(osFamily);
        deviceInfo.setOsVersion(osVersion);
        deviceInfo.setUserAgent(userAgentName);
        deviceInfo.setUserAgentType(userAgentType);
        deviceInfo.setUserAgentFamily(userAgentFamily);
        deviceInfo.setUserAgentVersion(userAgentVersion);

        return deviceInfo;
    }
}