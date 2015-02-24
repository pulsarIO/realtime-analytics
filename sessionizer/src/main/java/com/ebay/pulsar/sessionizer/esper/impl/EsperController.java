/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.esper.AbstractEventType;
import com.ebay.jetstream.event.processor.esper.EPL;
import com.ebay.jetstream.event.processor.esper.EsperDeclaredEvents;
import com.ebay.jetstream.event.processor.esper.MapEventType;
import com.ebay.jetstream.event.processor.esper.StringEventType;
import com.ebay.pulsar.sessionizer.esper.annotation.DebugSession;
import com.ebay.pulsar.sessionizer.esper.annotation.DecorateEvent;
import com.ebay.pulsar.sessionizer.impl.EsperSessionizerCounter;
import com.ebay.pulsar.sessionizer.impl.SessionizationInfo;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.AnnotationAttribute;
import com.espertech.esper.client.soda.AnnotationPart;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.event.MappedEventBean;
import com.espertech.esper.event.WrapperEventBean;

/**
 * Utilize Esper to control the sessionizer.
 * 
 * The output are hints to sessinizer processor.
 * 
 * @author xingwang
 *
 */
public class EsperController {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsperController.class);
    
    private static final class SessionizerCounterListener implements UpdateListener {
        private final String name;
        private final String field;
        private final boolean audit;
        private final EsperSessionizerCounter esperCounter;

        public SessionizerCounterListener(String name, String field, boolean audit, EsperSessionizerCounter esperCounter) {
            this.name = name;
            this.audit = audit;
            this.field = field;
            this.esperCounter = esperCounter;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
                for (int i = 0; i < newEvents.length; i++) {
                    Map<String, Object> eventMap = getEventMap(newEvents[i]);
                    if (eventMap != null) {
                        if (field == null || "".equals(field)) {
                            esperCounter.addCount(name, 1);
                        } else {
                            esperCounter.addCount(name + "-" + eventMap.get(field), 1);
                        }

                        if (audit) {
                            LOGGER.info("Event of {}, detail: {}", name, eventMap);
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getEventMap(EventBean bean) {
        if (bean instanceof MappedEventBean) {
            return ((MappedEventBean) bean).getProperties();
        } else if (bean instanceof WrapperEventBean) {
            WrapperEventBean wrappedBean = (WrapperEventBean) bean;
            if (wrappedBean.getUnderlyingEvent() instanceof MappedEventBean) {
                return ((MappedEventBean) (wrappedBean.getUnderlyingEvent())).getProperties();
            } else {
                return wrappedBean.getUnderlyingMap();
            }
        }
        return null;
    }

    private static final class DecorateEventListener implements UpdateListener {
        private JetstreamEvent event;

        public void reset(JetstreamEvent srcEvent) {
            this.event = srcEvent;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    for (Map.Entry<String, Object> e : eventMap.entrySet()) {
                        if (e.getValue() != null) {
                            if (e.getValue() instanceof Map) {
                                event.putAll((Map)e.getValue());
                            } else {
                                event.put(e.getKey(), e.getValue());
                            }
                        }
                    }
                }
            }
        }
    }

    private static final class SessionizerListener implements UpdateListener {
        private final String name;
        private final Map<String, SessionizationInfo> sessionizationMap;

        public SessionizerListener(String name, Map<String, SessionizationInfo> sessionizationMap) {
            this.name = name;
            this.sessionizationMap = sessionizationMap;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    Object identifier = eventMap.get(HINT_PK);
                    if (identifier instanceof String) {
                        SessionizationInfo info = new SessionizationInfo();
                        info.setIdentifier((String) identifier);
                        Object timestamp = eventMap.get(HINT_TIMESTAMP);
                        if (timestamp instanceof Long) {
                            info.setTimestamp((Long) timestamp);
                        } else {
                            info.setTimestamp(System.currentTimeMillis());
                        }
                        Object ttl = eventMap.get(HINT_TTL);
                        if (ttl instanceof Integer) {
                            info.setTtl((Integer) ttl);
                        }

                        info.setName(name);
                        this.sessionizationMap.put(name, info);
                    }
                }
            }
        }
    }

    private static final String HINT_TTL = "_duration_";
    private static final String HINT_PK = "_pk_";
    private static final String HINT_TIMESTAMP = "_timestamp_";

    private final EPServiceProvider esperService;
    private final Map<String, SessionizationInfo> sessionizationMap = new HashMap<String, SessionizationInfo>();
    private final DecorateEventListener eventListener;

    public EsperController(EPL eplStatements, EsperDeclaredEvents eventDefinition, String baseName,
            List<String> imports, EsperSessionizerCounter esperCounter, Set<String> sessionizerNames) {
        EPL epl = eplStatements;
        LOGGER.info("EPL Event definition {}", eventDefinition);

        Configuration config = createConfig(eventDefinition);
        config.getEngineDefaults().getExceptionHandling().addClass(SessionizerEsperExceptionHandlerFactory.class);
        String name = EsperController.class.getName() + "-" + baseName + "-" + Thread.currentThread().getId();
        esperService = EPServiceProviderManager.getProvider(name, config);
        if (imports != null) {
            for (String importName : imports) {
                esperService.getEPAdministrator().getConfiguration().addImport(importName);
            }
        }

        esperService.getEPAdministrator().getConfiguration().addImport(DebugSession.class);
        esperService.getEPAdministrator().getConfiguration().addImport(DecorateEvent.class);
        esperService.getEPAdministrator().getConfiguration()
        .addImport(com.ebay.pulsar.sessionizer.esper.annotation.Session.class);

        esperService.getEPAdministrator().getConfiguration().addImport(com.ebay.jetstream.epl.EPLUtilities.class);
        esperService.getEPAdministrator().getConfiguration().addImport(com.ebay.jetstream.epl.EPLUtils.class);
        esperService.getEPAdministrator().getConfiguration()
        .addPlugInSingleRowFunction("toJson", "com.ebay.jetstream.epl.EPLUtils", "toJsonString");
        esperService.getEPAdministrator().getConfiguration()
        .addPlugInSingleRowFunction("fromJson", "com.ebay.jetstream.epl.EPLUtils", "fromJsonString");

        eventListener = new DecorateEventListener();

        for (String s : epl.getStatements()) {
            EPStatement statement = esperService.getEPAdministrator().createEPL(s);
            EPStatementObjectModel model = esperService.getEPAdministrator().compileEPL(s);
            List<AnnotationPart> annots = model.getAnnotations();
            for (AnnotationPart part : annots) {
                if (DebugSession.class.getSimpleName().equals(part.getName())) {
                    String prefix = (String) getAnnotationValue(part, "counter");
                    String field = (String) getAnnotationValue(part, "colname");
                    Boolean auditValue = (Boolean) getAnnotationValue(part, "audit");
                    if (auditValue == null) {
                        auditValue = Boolean.FALSE;
                    }
                    statement.addListener(new SessionizerCounterListener(prefix, field, auditValue, esperCounter));
                }
                if (DecorateEvent.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(eventListener);
                }
                if (com.ebay.pulsar.sessionizer.esper.annotation.Session.class.getSimpleName().equals(part.getName())) {
                    String sessionizerName = (String) part.getAttributes().get(0).getValue();
                    if (!sessionizerNames.contains(sessionizerName)) {
                        throw new IllegalArgumentException("The sessionizer referenced by EPL " + sessionizerName
                                + " not configured.");
                    }
                    statement.addListener(new SessionizerListener(sessionizerName, sessionizationMap));
                }
            }
        }

    }

    private static Object getAnnotationValue(AnnotationPart part, String name) {
        List<AnnotationAttribute> attrs = part.getAttributes();
        Iterator<AnnotationAttribute> it = attrs.iterator();
        while (it.hasNext()) {
            AnnotationAttribute attr = it.next();
            if (name.endsWith(attr.getName())) {
                return attr.getValue();
            }
        }
        return null;
    }

    public void destroy() {
        esperService.destroy();
    }

    private Configuration createConfig(EsperDeclaredEvents declaredEvents) {
        Configuration config = new Configuration();
        config.setMetricsReportingDisabled();
        config.getEngineDefaults().getThreading().setThreadPoolInbound(false);
        config.getEngineDefaults().getThreading().setThreadPoolOutbound(false);
        config.getEngineDefaults().getThreading().setThreadPoolRouteExec(false);
        config.getEngineDefaults().getThreading().setThreadPoolTimerExec(false);
        config.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
        if (declaredEvents != null) {
            for (AbstractEventType type : declaredEvents.getEventTypes()) {
                String strAlias = type.getEventAlias();
                if (type instanceof MapEventType) {
                    config.addEventType(strAlias, ((MapEventType) type).getEventFields());
                }
                if (type instanceof StringEventType) {
                    config.addEventType(strAlias, ((StringEventType) type).getEventClassName());
                }
            }
        }

        return config;
    }

    public Map<String, SessionizationInfo> process(JetstreamEvent event) {
        sessionizationMap.clear();
        eventListener.reset(event);
        esperService.getEPRuntime().sendEvent(event, event.getEventType());
        return sessionizationMap;
    }

}
