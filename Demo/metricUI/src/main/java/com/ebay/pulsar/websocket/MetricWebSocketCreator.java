/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.websocket;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;

public class MetricWebSocketCreator implements WebSocketCreator {

    private WebSocketConnectionManager wsConnectionManager;
    
    @Override
    public Object createWebSocket(ServletUpgradeRequest req,
            ServletUpgradeResponse resp) {
        // Borrow WebSocket sub-protocols as input for channels and filters,
        // which are at position 0 and 1 in the sub-protocols array
        MetricWebSocket socket = new MetricWebSocket(req.getSubProtocols().get(0), req.getSubProtocols().get(1), wsConnectionManager);
        resp.setAcceptedSubProtocol(req.getSubProtocols().get(0));
        return socket;
    }
    
    public WebSocketConnectionManager getWsConnectionManager() {
        return wsConnectionManager;
    }

    public void setWsConnectionManager(
            WebSocketConnectionManager wsConnectionManager) {
        this.wsConnectionManager = wsConnectionManager;
    }

}
