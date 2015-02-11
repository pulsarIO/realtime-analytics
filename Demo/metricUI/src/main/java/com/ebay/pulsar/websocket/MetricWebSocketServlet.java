/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.websocket;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import com.ebay.pulsar.metric.server.SpringWebContextHolder;

public class MetricWebSocketServlet extends WebSocketServlet {
    private static final long serialVersionUID = -4260093440316144872L;
    
    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.register(MetricWebSocket.class);
        MetricWebSocketCreator webSocketCreator = SpringWebContextHolder.wac.getBean("MetricWebSocketCreator", MetricWebSocketCreator.class);
        factory.setCreator(webSocketCreator);
    }
}
