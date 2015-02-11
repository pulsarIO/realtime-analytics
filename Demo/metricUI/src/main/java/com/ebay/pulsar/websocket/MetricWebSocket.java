/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.websocket;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

public class MetricWebSocket extends WebSocketAdapter {
    private volatile Set<String> _channels;
    private volatile Map<String, Object> _filters;
    private volatile WebSocketConnectionManager connectionManager ;

    public MetricWebSocket(String channels, String filters, WebSocketConnectionManager connectionManager) {
        Set<String> newChannels = new HashSet<String>();
        String[] propArray = channels.split(PulsarEventConstant.AND);
        for (String channel : propArray) {
            newChannels.add(channel);
        }

        _channels = newChannels;

        Map<String, Object> newFilters = new HashMap<String, Object>();
        propArray = filters.split(PulsarEventConstant.AND);
        for (String nvPair : propArray) {
            int eqpos = nvPair.indexOf(PulsarEventConstant.EQUAL);
            if (eqpos > 0) {
                String key = nvPair.substring(0, eqpos);
                String value = nvPair.substring(eqpos
                        + PulsarEventConstant.EQUAL.length(), nvPair.length());
                if (value != null && !value.isEmpty()) {
                    if (key.equals(PulsarEventConstant.SAMPLING)) {
                        int sample = 0;
                        try {
                            sample = Integer.parseInt(value);
                        } catch (NumberFormatException e) {
                            //
                        }
                        newFilters.put(key, sample);
                    } else {
                        newFilters.put(key, value);
                    }
                }
            }
        }

        this._filters = newFilters;
        this.connectionManager = connectionManager;
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        connectionManager.add(this);
        connectionManager.broadcastToSession(this);
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        connectionManager.remove(this);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        connectionManager.registerError(cause);
    }

    public Map<String, Object> getFilters() {
        return _filters;
    }

    public Set<String> getChannles() {
        return _channels;
    }
}
