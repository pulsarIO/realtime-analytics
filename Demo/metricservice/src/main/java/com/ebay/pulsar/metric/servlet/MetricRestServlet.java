/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.type.TypeFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.event.channel.messaging.http.inbound.InboundRTBDEventServlet;
import com.ebay.jetstream.management.Management;
import com.ebay.pulsar.metric.core.Counter;
import com.ebay.pulsar.metric.core.MetricsService;
import com.ebay.pulsar.metric.core.RawNumericMetric;
import com.ebay.pulsar.metric.impl.cassandra.QueryParam;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;

public class MetricRestServlet extends InboundRTBDEventServlet implements InitializingBean, BeanNameAware {
    private static final String QUERY_CASSANDRA_ERROR = "Query Cassandra Error.";
    private static final String QUERY_CASSANDRA_TIMEOUT = "Query Cassandra Timeout.";

    private static class UTF8InputStreamReaderWrapper extends  InputStreamReader {

        public UTF8InputStreamReaderWrapper(InputStream in) throws UnsupportedEncodingException {
            super(in, Charsets.UTF_8);
        }

        public UTF8InputStreamReaderWrapper(InputStream in, String encoding) throws UnsupportedEncodingException {
            super(in, encoding);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private static final long serialVersionUID = 7299139314716222008L;
    private static final String PATH_PING = "/pulsar/ping";
    private static final String PATH_COUNTER = "/pulsar/counter";
    private static final String PATH_METRICGROUP = "/pulsar/metric";
    
    private final ServletStats stats;
    private final MetricsService metricService;
    private final ObjectWriter counterResultWriter;
    private final ObjectWriter metricResultWriter;
    
    private String beanName;
    public MetricRestServlet(MetricsService service) {
        super();
        metricService = service;
        stats = new ServletStats();
        counterResultWriter = new ObjectMapper().typedWriter(TypeFactory.collectionType(List.class, Counter.class));
        metricResultWriter = new ObjectMapper().typedWriter(TypeFactory.collectionType(List.class, RawNumericMetric.class));
    }
    
    private void ping(HttpServletRequest request, String pathInfo,
            HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_OK);
    }

    private void getCounters(final HttpServletRequest request, final String pathInfo, final HttpServletResponse response) throws Exception {
        String queryString = request.getQueryString();
        QueryParam queryParam =  getCassandraQueryParam(queryString);
        ListenableFuture<List<Counter>> future = metricService.findCounters((String)queryParam.getParameters().get(QueryParam.METRIC_NAME), (String)queryParam.getParameters().get(QueryParam.GROUP_ID));
        try{
            List<Counter> counters = future.get(5000, TimeUnit.MILLISECONDS);
            String result = counterResultWriter.writeValueAsString(counters);
            response.getWriter().print(result);
            response.setContentLength(result.length());
            response.setStatus(HttpServletResponse.SC_OK);      
        }catch (TimeoutException e){
            response.getWriter().print(QUERY_CASSANDRA_TIMEOUT);
            response.setContentLength(QUERY_CASSANDRA_TIMEOUT.length());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);   
        }catch (ExecutionException e){
            response.getWriter().print(QUERY_CASSANDRA_ERROR);
            response.setContentLength(QUERY_CASSANDRA_ERROR.length());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);   
        }
    }
    
    private void getMetrics(final HttpServletRequest request, final String pathInfo, final HttpServletResponse response) throws Exception {
        String queryString = request.getQueryString();
        QueryParam queryParam =  getCassandraQueryParam(queryString);
        ListenableFuture<List<RawNumericMetric>> future = metricService.findData(queryParam);
        
        try{
            List<RawNumericMetric> metrics = future.get(5000, TimeUnit.MILLISECONDS);
            String result = metricResultWriter.writeValueAsString(metrics);
            response.getWriter().print(result);
            response.setContentLength(result.length());
            response.setStatus(HttpServletResponse.SC_OK);      
        }catch (TimeoutException e){
            response.getWriter().print(QUERY_CASSANDRA_TIMEOUT);
            response.setContentLength(QUERY_CASSANDRA_TIMEOUT.length());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);   
        }catch (ExecutionException e){
            response.getWriter().print(QUERY_CASSANDRA_ERROR);
            response.setContentLength(QUERY_CASSANDRA_ERROR.length());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);   
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Management.removeBeanOrFolder(beanName, stats);
        Management.addBean(beanName, stats);
    }

    private String readRequest(HttpServletRequest request) {
        try {
            StringBuffer sb = new StringBuffer();
            if (request.getInputStream() != null) {
                request.getInputStream().reset();
                InputStreamReader isr = new UTF8InputStreamReaderWrapper(request.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String thisLine;
                while ((thisLine = br.readLine()) != null) {
                    sb.append(thisLine);
                }
                br.close();
            }
            return sb.toString();
        } catch (Throwable ex) {
            return null;
        }
    }

    private String readRequestHead(HttpServletRequest request) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append(request.getMethod()).append(" ");
            sb.append(request.getProtocol()).append(" ");
            sb.append(request.getPathInfo()).append("\n");
            // Jetstream getHeaderNames has issues.
            Enumeration<String> headerNames = request.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String name = headerNames.nextElement();
                sb.append(name).append(": ");
                sb.append(request.getHeader(name)).append("\n");
            }
            return sb.toString();
        } catch (Throwable ex) {
            return null;
        }
    }


    @Override
    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletInputStream inputStream = request.getInputStream();
        if (inputStream != null) {
            inputStream.mark(Integer.MAX_VALUE);
        }
        try {
            String pathInfo = request.getPathInfo();
            if (pathInfo.startsWith(PATH_PING)) {
                ping(request, pathInfo, response);
            } else if (pathInfo.startsWith(PATH_COUNTER)) {
                stats.incQueryRequestCount();
                getCounters(request, pathInfo, response);
            } else if (pathInfo.startsWith(PATH_METRICGROUP)) {
                stats.incQueryRequestCount();
                getMetrics(request, pathInfo, response);
            } else {
                stats.incInvalidRequestCount();
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (Throwable ex) {
            String requestTxt = readRequest(request);
            stats.setLastFailedRequest(readRequestHead(request) + requestTxt);
            stats.registerError(ex);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }finally{
            response.addHeader("Access-Control-Allow-Origin", "*");
            response.addHeader("Access-Control-Allow-Methods", "*");
            response.addHeader("Access-Control-Allow-Headers", "Content-Type");
        }
    }
    
    private QueryParam getCassandraQueryParam(String queryString){
        QueryParam cassandraQueryParam = QueryParam.build();
        String decodedParam = URLDecoder.decode(queryString);
        String [] paramList = decodedParam.split("&");
        for(String paramSplit : paramList){
            int index = paramSplit.indexOf("=");
            if(index > 0){
                String key = paramSplit.substring(0, index).trim();
                String value = paramSplit.substring(index+1).trim();
                if(key.equalsIgnoreCase(QueryParam.COLUMN_FAMILYNAME)){
                    cassandraQueryParam.columnFamilyName(value);
                }else if(key.equalsIgnoreCase(QueryParam.START_TIME)){
                    cassandraQueryParam.startTime(Long.valueOf(value));
                }else if(key.equalsIgnoreCase(QueryParam.END_TIME)){
                    cassandraQueryParam.endTime(Long.valueOf(value));
                }else if(key.equalsIgnoreCase(QueryParam.METRIC_TIME)){
                    cassandraQueryParam.metricTime(Long.valueOf(value));
                }else if(key.equalsIgnoreCase(QueryParam.METRIC_NAME)){
                    cassandraQueryParam.metricName(value);
                }else if(key.equalsIgnoreCase(QueryParam.GROUP_ID)){
                    cassandraQueryParam.groupId(value);
                }else if(key.equalsIgnoreCase(QueryParam.TOPN)){
                    cassandraQueryParam.topN(Integer.valueOf(value));
                }else{
                    cassandraQueryParam.addParam(key, value);
                }
            }
        }
        return cassandraQueryParam;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }
}
