package com.ebay.pulsar.metric.server;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringWebContextHolder implements ApplicationContextAware{
    public static ApplicationContext wac;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
       SpringWebContextHolder.wac = applicationContext;
    }
}
