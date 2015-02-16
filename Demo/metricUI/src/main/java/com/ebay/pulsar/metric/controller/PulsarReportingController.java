/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.controller;

import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

@Controller
@RequestMapping("/")
public class PulsarReportingController {
    
    private static String MetricServer = System.getProperty("metricserver.host") != null ? System.getProperty("metricserver.host") : "localhost";
    private static int MetricSeverPort = System.getProperty("metricserver.port") != null ? Integer.parseInt((System.getProperty("metricserver.port"))) : 8083;
    private static String MetricCalculator = System.getProperty("metriccalculator.host") != null ? System.getProperty("metriccalculator.host") : "localhost";
    private static int MetricCalculatorPort = System.getProperty("metriccalculator.port") != null ? Integer.parseInt((System.getProperty("metriccalculator.port"))) : 9999;
    private static String MetricCalculatorPath = "/Event/Processor/MCEPL";
    
    @Autowired
    private RestTemplate restTemplate;
    
    @RequestMapping(value={""}, method=RequestMethod.GET)
    public String loadIndex() {
         return "forward:/index.html" ;
    }
    
    @RequestMapping(value={"pulsar/metric","pulsar/counter"}, method=RequestMethod.GET)
    @ResponseBody
    public String listMetrics(HttpMethod method, HttpServletRequest request,
            HttpServletResponse response) throws URISyntaxException {
        URI uri = new URI("http", null, MetricServer, MetricSeverPort, request.getRequestURI(), request.getQueryString(), null);
        ResponseEntity<String> responseEntity =
            restTemplate.exchange(uri, method, HttpEntity.EMPTY, String.class);
        return responseEntity.getBody();
    }
    
    @SuppressWarnings("rawtypes")
    @RequestMapping(value="pulsar/metriccalculator", method=RequestMethod.GET)
    public  ResponseEntity viewMetricCalculator(HttpMethod method, HttpServletRequest request,
            HttpServletResponse response) throws URISyntaxException {
        URI url = new URI("http", null, MetricCalculator, MetricCalculatorPort, MetricCalculatorPath, request.getQueryString(), null);
    	ResponseEntity<String> responseEntity =
                restTemplate.exchange(url, method, HttpEntity.EMPTY, String.class);
        return responseEntity;
    }
    
    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
}
