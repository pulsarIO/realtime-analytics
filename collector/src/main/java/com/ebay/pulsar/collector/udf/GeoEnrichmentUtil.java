/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.udf;

import java.io.File;
import java.net.InetAddress;

import org.springframework.beans.factory.InitializingBean;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;

public class GeoEnrichmentUtil implements InitializingBean {

    private String geoDBFilePath;
    private static DatabaseReader reader;

    private static final GeoEnrichmentUtil INSTANCE = new GeoEnrichmentUtil();

    public static GeoEnrichmentUtil getInstance() {
        return INSTANCE;
    }

    private GeoEnrichmentUtil() {
        //singleton
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        File database = new File(geoDBFilePath);
        reader = new DatabaseReader.Builder(database).build();
    }

    public static GeoInfo getGeoInfo(String ipAddress) {
        GeoInfo geoInfo = new GeoInfo();
        if (ipAddress == null) {
            return geoInfo;
        }
        try{
            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            CityResponse response = reader.city(inetAddress);

            Country country = response.getCountry();
            Continent continent = response.getContinent();

            City city = response.getCity();
            Postal postal = response.getPostal();
            Location location = response.getLocation();

            Subdivision subdivision = response.getMostSpecificSubdivision();
            geoInfo.setRegion(subdivision.getName());
            geoInfo.setCountryIsoCode(country.getIsoCode());
            geoInfo.setCountry(country.getName());
            geoInfo.setContinent(continent.getCode());
            geoInfo.setCity(city.getName());
            geoInfo.setPostalCode(postal.getCode());
            geoInfo.setLatitude(location.getLatitude());
            geoInfo.setLongitude(location.getLongitude());

            return geoInfo;
        } catch(Exception ex){
            return null;
        }
    }

    public void setGeoDBFilePath(String geoDBFilePath) {
        this.geoDBFilePath = geoDBFilePath;
    }
}