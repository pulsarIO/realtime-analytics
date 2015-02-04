/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector;

import com.ebay.pulsar.collector.udf.DeviceEnrichmentUtil;
import com.ebay.pulsar.collector.udf.DeviceInfo;
import com.ebay.pulsar.collector.udf.GeoEnrichmentUtil;
import com.ebay.pulsar.collector.udf.GeoInfo;

public class EnrichmentSimulator {    
    public static void main(String[] args) throws Exception {
    	GeoEnrichmentUtil geoEnrichmentUtil = GeoEnrichmentUtil.getInstance();
    	geoEnrichmentUtil.setGeoDBFilePath("buildsrc/geodb/GeoLite2-City.mmdb");
    	geoEnrichmentUtil.afterPropertiesSet();
    	@SuppressWarnings("static-access")
		GeoInfo geoInfo = geoEnrichmentUtil.getGeoInfo("192.128.10.5");
    	 	
    	System.out.println("countryIsoCode: " + geoInfo.getCountryIsoCode());
    	System.out.println("country: " + geoInfo.getCountry());
    	System.out.println("region: " + geoInfo.getRegion());
    	System.out.println("city: " + geoInfo.getCity());
    	System.out.println("continent: " + geoInfo.getContinent());
    	System.out.println("postalCode: " + geoInfo.getPostalCode());
    	System.out.println("latitude: " + geoInfo.getLatitude());
    	System.out.println("longitude: " + geoInfo.getLongitude());
    	
    	DeviceEnrichmentUtil deviceEnrichmentUtil = DeviceEnrichmentUtil.getInstance();
    	deviceEnrichmentUtil.afterPropertiesSet();
    	@SuppressWarnings("static-access")
    	DeviceInfo deviceInfo = deviceEnrichmentUtil.getDeviceInfo("Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53");
    	
    	System.out.println("deviceCategory: " + deviceInfo.getDeviceCategory());
    	System.out.println("osFamily: " + deviceInfo.getOsFamily());
    	System.out.println("osVersion: " + deviceInfo.getOsVersion());
    	System.out.println("userAgent: " + deviceInfo.getUserAgent());
    	System.out.println("userAgentType: " + deviceInfo.getUserAgentType());
    	System.out.println("userAgentFamily: " + deviceInfo.getUserAgentFamily());
    	System.out.println("userAgentVersion: " + deviceInfo.getUserAgentVersion());
    }
}