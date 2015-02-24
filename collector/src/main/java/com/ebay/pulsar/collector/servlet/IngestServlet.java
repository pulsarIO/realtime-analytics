/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.messaging.http.inbound.InboundRESTChannel;
import com.ebay.jetstream.event.channel.messaging.http.inbound.InboundRTBDEventServlet;
import com.ebay.jetstream.management.Management;
import com.ebay.pulsar.collector.validation.ValidationResult;
import com.ebay.pulsar.collector.validation.Validator;
import com.google.common.base.Charsets;

/**
 * This is a servlet which receive the json format event and feed the
 * events into rest inbound channel.
 * 
 * @author xingwang
 *
 */
public class IngestServlet extends InboundRTBDEventServlet implements InitializingBean, BeanNameAware {

    private static final TypeReference<List<Map<String, Object>>> TYPE_LIST_OF_MAP = new TypeReference<List<Map<String, Object>>>() { };
    private static final TypeReference<Map<String, Object>> TYPE_FLATMAP = new TypeReference<Map<String, Object>>(){};

    // This will allow the jackson use stream to read data but without clear the buffer when it close the stream.
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

    private static final long serialVersionUID = 7299139314716222006L;
    private static final String PATH_INGEST = "/pulsar/ingest/";
    private static final String PATH_BATCH_INGEST = "/pulsar/batchingest/";

    private final ObjectMapper mapper;
    private InboundRESTChannel inboundChannel;
    private final ServletStats stats;
    private String beanName;
    private Validator validator;
    private final ObjectWriter validateReusltWriter;
    public void setValidator(Validator validator) {
        this.validator = validator;
    }

    public IngestServlet() {
        super();
        mapper = new ObjectMapper();
        validateReusltWriter = mapper.typedWriter(ValidationResult.class);
        stats = new ServletStats();
    }

    private void add(HttpServletRequest request, String pathInfo, HttpServletResponse response) throws Exception {
        String eventType = pathInfo.substring(pathInfo.lastIndexOf('/') + 1);


        UTF8InputStreamReaderWrapper reader;

        if (request.getCharacterEncoding() != null) {
            reader = new UTF8InputStreamReaderWrapper(request.getInputStream(), request.getCharacterEncoding());
        } else {
            reader = new UTF8InputStreamReaderWrapper(request.getInputStream());
        }

        Map<String, Object> values = mapper.readValue(reader, TYPE_FLATMAP);

        if (validator != null) {
            ValidationResult result = validator.validate(values, eventType);
            if (result == null || result.isSuccess()) {
                JetstreamEvent event = createEvent(values, eventType);
                inboundChannel.onMessage(event);
                response.setStatus(HttpServletResponse.SC_OK);
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(validateReusltWriter.writeValueAsString(result));
            }
        } else {
            JetstreamEvent event = createEvent(values, eventType);
            inboundChannel.onMessage(event);
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    private void batchAdd(HttpServletRequest request, String pathInfo, HttpServletResponse response) throws Exception {
        String eventType = pathInfo.substring(pathInfo.lastIndexOf('/') + 1);


        UTF8InputStreamReaderWrapper reader;

        if (request.getCharacterEncoding() != null) {
            reader = new UTF8InputStreamReaderWrapper(request.getInputStream(), request.getCharacterEncoding());
        } else {
            reader = new UTF8InputStreamReaderWrapper(request.getInputStream());
        }

        List<Map<String, Object>> eventList = mapper.readValue(reader, TYPE_LIST_OF_MAP);
        List<ValidationResult> failedEvents = null;
        for (Map<String, Object> values : eventList) {
            if (validator != null) {
                ValidationResult result = validator.validate(values, eventType);
                if (result == null || result.isSuccess()) {
                    JetstreamEvent event = createEvent(values, eventType);
                    inboundChannel.onMessage(event);
                } else {
                    if (failedEvents == null) {
                        failedEvents = new ArrayList<ValidationResult>();
                    }
                    failedEvents.add(result);
                }
            } else {
                JetstreamEvent event = createEvent(values, eventType);
                inboundChannel.onMessage(event);
            }
        }
        if (failedEvents == null) {
            response.setStatus(HttpServletResponse.SC_OK);
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            PrintWriter printWriter = response.getWriter();
            printWriter.write("[");
            for (int i = 0, t = failedEvents.size() -1 ; i < t; i++) {
                printWriter.write(validateReusltWriter.writeValueAsString(failedEvents.get(i)));
                printWriter.write(",");
            }
            printWriter.write(validateReusltWriter.writeValueAsString(failedEvents.get(failedEvents.size() -1)));
            printWriter.write("]");
        }

    }


    @Override
    public void afterPropertiesSet() throws Exception {
        Management.removeBeanOrFolder(beanName, stats);
        Management.addBean(beanName, stats);
    }

    private JetstreamEvent createEvent(Map<String, Object> data, String eventType) {
        JetstreamEvent event = new JetstreamEvent(data);
        event.setEventType(eventType);
        return event;
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
            if (pathInfo.startsWith(PATH_INGEST)) {
                stats.incIngestRequestCount();
                add(request, pathInfo, response);
            } else if (pathInfo.startsWith(PATH_BATCH_INGEST)) {
                stats.incBatchIngestRequestCount();
                batchAdd(request, pathInfo, response);
            } else {
                stats.incInvalidRequestCount();
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (Throwable ex) {
            String requestTxt = readRequest(request);
            stats.setLastFailedRequest(readRequestHead(request) + requestTxt);
            stats.registerError(ex);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void setInboundChannel(InboundRESTChannel inboundChannel) {
        this.inboundChannel = inboundChannel;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }
}
