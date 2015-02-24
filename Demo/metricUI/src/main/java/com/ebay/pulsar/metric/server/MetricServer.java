/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.server;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.ContextStoppedEvent;

import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.util.CommonUtils;

public class MetricServer implements ApplicationListener<ApplicationEvent>, ApplicationContextAware{

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricServer.class.getName());
    private ApplicationContext applicationContext;
    private Server s_server = null;

    public int getPort() {
        return s_port;
    }

    public void setPort(int s_port) {
        this.s_port = s_port;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {

        if (event instanceof ContextStartedEvent) {
            this.startStandAlone();
        } else if (event instanceof ContextClosedEvent || event instanceof ContextStoppedEvent) {
            this.stopStandAlone();
        }
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    private int s_port;
    private static final String WEB_HOME_ENV = "JETSTREAM_HOME";

    private String getBaseUrl() {
        String root = ConfigUtils.getPropOrEnv(WEB_HOME_ENV);
        if (root == null) {
            throw new RuntimeException(WEB_HOME_ENV + " is not specified.");
        }

        return root + "/webapp";
    }

    public void startStandAlone() {
        try {
            WebAppContext context = new WebAppContext();
            String baseUrl = getBaseUrl();
            LOGGER.info("Metric server baseUrl: " + baseUrl);
            context.setDescriptor(baseUrl + "/WEB-INF/web.xml");
            context.setResourceBase(baseUrl);
            context.setContextPath("/");
            context.setParentLoaderPriority(true);
            context.setAttribute("JetStreamRoot", applicationContext);
            Server s_server = new Server(s_port);
            s_server.setHandler(context);

            LOGGER.info( "Metric server started, listening on port " + s_port);
            s_server.start();
            running.set(true);
        } catch (Throwable t) {
            throw CommonUtils.runtimeException(t);
        }
    }

    public void stopStandAlone() {
        if (s_server != null) {
            try {
                s_server.stop();
                s_server = null;
                running.set(false);
            } catch (Exception e) {
            }
        }
    }



    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.applicationContext = applicationContext;
    }
}
