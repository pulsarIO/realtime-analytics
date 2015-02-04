/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import com.ebay.pulsar.sessionizer.esper.annotation.AppendState;
import com.ebay.pulsar.sessionizer.esper.annotation.DebugSession;
import com.ebay.pulsar.sessionizer.esper.annotation.DecorateEvent;
import com.ebay.pulsar.sessionizer.esper.annotation.UpdateCounter;
import com.ebay.pulsar.sessionizer.esper.annotation.UpdateDuration;
import com.ebay.pulsar.sessionizer.esper.annotation.UpdateMetadata;
import com.ebay.pulsar.sessionizer.esper.annotation.UpdateState;
import com.ebay.pulsar.sessionizer.impl.EsperSessionizerCounter;
import com.ebay.pulsar.sessionizer.impl.SessionizationInfo;
import com.ebay.pulsar.sessionizer.impl.SessionizerProcessor;
import com.ebay.pulsar.sessionizer.model.Session;
import com.ebay.pulsar.sessionizer.model.SubSession;
import com.ebay.pulsar.sessionizer.spi.SessionizerContext;
import com.ebay.pulsar.sessionizer.spi.SessionizerExtension;
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
 * Utilize esper do customized sessionization logic.
 * 
 * @author xingwang
 *
 */
public class EsperSessionizer {
    private static final class AppendStateListener implements UpdateListener {
        private final SessionizerContext context;
        private final String name;
        private final String colName;
        private final boolean unique;
        private final int maxLength;

        public AppendStateListener(SessionizerContext context, String name, String colName, boolean unique, int maxLength) {
            this.context = context;
            this.name = name;
            this.colName = colName;
            this.unique = unique;
            this.maxLength = maxLength;

        }


        @SuppressWarnings("unchecked")
        public void append(Object value) {
            if (value == null) {
                return;
            }
            Map<String, Object> attributes = context.getCurrentSession().getDynamicAttributes();
            Object existedValue = attributes.get(name);
            List<Object> list = null;
            if (existedValue instanceof List) {
                list = (List<Object>) existedValue;
            }
            if (list == null) {
                list = new ArrayList<Object>();
                attributes.put(name, list);
            }
            if (unique && list.contains(value)) {
                return;
            }
            if (maxLength > 0 && list.size() >= maxLength) {
                list.remove(0);
            }
            list.add(value);
        }


        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                for (int i = 0; i < newEvents.length; i++) {
                    Map<String, Object> eventMap = getEventMap(newEvents[i]);
                    if (eventMap != null) {
                        Object v = eventMap.get(colName);
                        if (v != null) {
                            append(v);
                        }
                    }
                }
            }
        }
    }



    private static Object normailizeValue(Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) value;
            if (values.size() == 1) {
                return values.get(0).toString();
            }
            StringBuilder b = new StringBuilder();
            b.append(values.get(0));
            for (int i = 1, t = values.size(); i < t; i++) {
                b.append(",");
                b.append(values.get(i));
            }
            return b.toString();
        } else {
            return value;
        }
    }

    private static final class DecorateEventListener implements UpdateListener {
        private final SessionizerContext context;

        public DecorateEventListener(SessionizerContext context) {
            this.context = context;
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    JetstreamEvent event = context.getEvent();
                    for (Map.Entry<String, Object> e : eventMap.entrySet()) {
                        if (e.getValue() != null) {
                            if (e.getValue() instanceof Map) {
                                for (Map.Entry<String, Object> nestede : ((Map<String, Object>)e.getValue()).entrySet()) {
                                    if (nestede.getValue() != null) {
                                        event.put(nestede.getKey(),  normailizeValue(nestede.getValue()));
                                    }
                                }
                            } else if (e.getValue() != null) {
                                event.put(e.getKey(),  normailizeValue(e.getValue()));
                            }
                        }
                    }
                }
            }
        }
    }

    private static final class SessionCounterListener implements UpdateListener {
        private final String name;
        private final SessionizerContext context;

        public SessionCounterListener(String name, SessionizerContext context) {
            this.context = context;
            this.name = name;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> dynamicAttributes = context.getCurrentSession().getDynamicAttributes();
                Integer existedValue = (Integer) dynamicAttributes.get(name);
                if (existedValue == null) {
                    dynamicAttributes.put(name, newEvents.length);
                } else {
                    dynamicAttributes.put(name, newEvents.length + existedValue);
                }
            }
        }
    }

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


    private static final class SessionMetadataListener implements UpdateListener {
        private final SessionizerContext context;

        public SessionMetadataListener(SessionizerContext context) {
            this.context = context;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    Map<String, Object> metadataAttributes = context.getCurrentSession().getInitialAttributes();
                    for (Map.Entry<String, Object> e : eventMap.entrySet()) {
                        if (e.getValue() == null) {
                            metadataAttributes.remove(e.getKey());
                        } else {
                            metadataAttributes.put(e.getKey(), e.getValue());
                        }
                        context.markMetadataChanged();
                    }
                }
            }
        }
    }

    private static final class SessionStateListener implements UpdateListener {
        private final SessionizerContext context;

        public SessionStateListener(SessionizerContext context) {
            this.context = context;
        }


        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    Map<String, Object> dynamicAttributes = context.getCurrentSession().getDynamicAttributes();
                    for (Map.Entry<String, Object> e : eventMap.entrySet()) {
                        if (e.getValue() == null) {
                            dynamicAttributes.remove(e.getKey());
                        } else {
                            dynamicAttributes.put(e.getKey(), e.getValue());
                        }
                    }
                }
            }
        }
    }

    private static final class SubSessionizerListener implements UpdateListener {
        private final String name;
        private final SessionizerContext context;

        public SubSessionizerListener(String name, SessionizerContext context) {
            this.name = name;
            this.context = context;
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
                        Object ttl = eventMap.get(HINT_TTL);
                        if (ttl instanceof Integer) {
                            info.setTtl((Integer) ttl);
                        }

                        info.setName(name);
                        context.getMainSession().addSubSessionizerInfo(name, info);
                    }
                }
            }
        }

    }

    private static final class UpdateTTLListener implements UpdateListener {
        private final SessionizerContext context;
        private final int maxTTL;
        public UpdateTTLListener(SessionizerContext context, int maxTTL) {
            this.context = context;
            this.maxTTL = maxTTL;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null && newEvents.length > 0) {
                Map<String, Object> eventMap = getEventMap(newEvents[0]);
                if (eventMap != null) {
                    Object ttl = eventMap.get(HINT_TTL);
                    if (ttl instanceof Integer) {
                        int ttlvalue = (Integer) ttl;

                        if (ttlvalue <= maxTTL) {
                            if (context.getCurrentSession() != context.getMainSession()) {
                                if (ttlvalue <= context.getMainSession().getTtl()) {
                                    context.getCurrentSession().updateTtl(ttlvalue);
                                }
                            } else {
                                context.getCurrentSession().updateTtl(ttlvalue);
                            }
                        }
                    }
                }
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EsperSessionizer.class);
    
    private static final String VAR_METADATA_NAME = "metadata";
    private static final String VAR_SESSION_NAME = "session";
    private static final String VAR_PARENT_METADATA_NAME = "parentMetadata";
    private static final String VAR_PARENT_SESSION_NAME = "parentSession";

    private static final String HINT_TTL = "_duration_";
    private static final String HINT_PK = "_pk_";

    private static Object getAnnotationValue(AnnotationPart part, String name, Object defaultValue) {
        List<AnnotationAttribute> attrs = part.getAttributes();
        Iterator<AnnotationAttribute> it = attrs.iterator();
        while (it.hasNext()) {
            AnnotationAttribute attr = it.next();
            if (name.equals(attr.getName())) {
                return attr.getValue();
            }
        }
        return defaultValue;
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

    private final Set<String> eventTypes;
    private final EPServiceProvider esperService;
    private final SessionVariable sessionVariable = new SessionVariable();
    private final AttributeMapVariable metadataVariable = new AttributeMapVariable();
    private final SessionVariable parentSessionVariable = new SessionVariable();
    private final AttributeMapVariable parentMetadataVariable = new AttributeMapVariable();
    private final SessionizerContext context = new SessionizerContext();
    private final int maxTTL;

    public EsperSessionizer(int id, EPL eplStatements, EsperDeclaredEvents eventDefinition, String baseName, List<String> imports,
            EsperSessionizerCounter esperCounter, boolean isMainSessionizer, Set<String> subSessionNames,
            List<SessionizerExtension> extensions, int maxTTL) {
        this.maxTTL =maxTTL;
        EPL epl = eplStatements;
        eventTypes = new HashSet<String>();
        LOGGER.info("EPL Event definition {}", eventDefinition);

        Configuration config = createConfig(eventDefinition);
        config.getEngineDefaults().getExceptionHandling().addClass(SessionizerEsperExceptionHandlerFactory.class);
        String name = EsperSessionizer.class.getName() + "-" + baseName + "-" + id;
        esperService = EPServiceProviderManager.getProvider(name, config);
        if (imports != null) {
            for (String importName : imports) {
                esperService.getEPAdministrator().getConfiguration().addImport(importName);
            }
        }
        esperService.getEPAdministrator().getConfiguration().addImport(DecorateEvent.class);
        esperService.getEPAdministrator().getConfiguration().addImport(UpdateState.class);
        esperService.getEPAdministrator().getConfiguration().addImport(UpdateMetadata.class);
        esperService.getEPAdministrator().getConfiguration().addImport(UpdateCounter.class);
        esperService.getEPAdministrator().getConfiguration().addImport(UpdateDuration.class);
        esperService.getEPAdministrator().getConfiguration().addImport(AppendState.class);
        esperService.getEPAdministrator().getConfiguration().addImport(DebugSession.class);

        if (isMainSessionizer) {
            esperService.getEPAdministrator().getConfiguration().addImport(com.ebay.pulsar.sessionizer.esper.annotation.SubSession.class);
        }

        if (extensions != null) {
            for (SessionizerExtension ext : extensions) {
                esperService.getEPAdministrator().getConfiguration().addImport(ext.getAnnotation());
            }
        }

        esperService.getEPAdministrator().getConfiguration().addImport(com.ebay.jetstream.epl.EPLUtilities.class);
        esperService.getEPAdministrator().getConfiguration().addImport(com.ebay.jetstream.epl.EPLUtils.class);
        esperService.getEPAdministrator().getConfiguration().
        addPlugInSingleRowFunction("toJson", "com.ebay.jetstream.epl.EPLUtils", "toJsonString");
        esperService.getEPAdministrator().getConfiguration().
        addPlugInSingleRowFunction("fromJson", "com.ebay.jetstream.epl.EPLUtils", "fromJsonString");

        esperService.getEPAdministrator().getConfiguration().
        addVariable(VAR_SESSION_NAME, SessionVariable.class.getName(), sessionVariable, true);
        esperService.getEPAdministrator().getConfiguration().
        addVariable(VAR_METADATA_NAME, AttributeMapVariable.class.getName(), metadataVariable, true);
        if (!isMainSessionizer) {
            esperService.getEPAdministrator().getConfiguration().
            addVariable(VAR_PARENT_SESSION_NAME, SessionVariable.class.getName(), parentSessionVariable, true);
            esperService.getEPAdministrator().getConfiguration().
            addVariable(VAR_PARENT_METADATA_NAME, AttributeMapVariable.class.getName(), parentMetadataVariable, true);
        }


        processAnnotations(esperCounter, isMainSessionizer, subSessionNames, extensions, epl);

    }

    private void processAnnotations(EsperSessionizerCounter esperCounter, boolean isMainSessionizer,
            Set<String> subSessionNames, List<SessionizerExtension> extensions, EPL epl) {
        for (String s : epl.getStatements()) {
            EPStatement statement = esperService.getEPAdministrator().createEPL(s);
            EPStatementObjectModel model = esperService.getEPAdministrator().compileEPL(s);
            List<AnnotationPart> annots = model.getAnnotations();
            for (AnnotationPart part : annots) {
                if (isMainSessionizer && com.ebay.pulsar.sessionizer.esper.annotation.SubSession.class.getSimpleName().equals(part.getName())) {
                    String sessionizerName = (String) part.getAttributes().get(0).getValue();
                    if (!subSessionNames.contains(sessionizerName)) {
                        throw new IllegalArgumentException("The sub sessionizer referenced by EPL " + sessionizerName + " not configured.");
                    }
                    statement.addListener(new SubSessionizerListener(sessionizerName, context));
                }
                if (isMainSessionizer && UpdateDuration.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new UpdateTTLListener(context, maxTTL));
                }
                if (DecorateEvent.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new DecorateEventListener(context));
                }
                if (AppendState.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new AppendStateListener(context, (String) getAnnotationValue(part, "name", null),
                            (String) getAnnotationValue(part, "colname", null),
                            (Boolean) getAnnotationValue(part, "unique", true),
                            (Integer) getAnnotationValue(part, "maxlength", 0)));
                }
                if (UpdateState.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new SessionStateListener(context));
                }
                if (UpdateMetadata.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new SessionMetadataListener(context));
                }
                if (UpdateCounter.class.getSimpleName().equals(part.getName())) {
                    statement.addListener(new SessionCounterListener((String) part.getAttributes().get(0).getValue(), context));
                }
                if (DebugSession.class.getSimpleName().equals(part.getName())) {
                    String prefix = (String) getAnnotationValue(part, "counter", null);
                    String field = (String) getAnnotationValue(part, "colname", null);
                    Boolean auditValue = (Boolean) getAnnotationValue(part, "audit", false);
                    statement.addListener(new SessionizerCounterListener(prefix, field, auditValue, esperCounter));
                }
                if (extensions != null) {
                    for (SessionizerExtension ext : extensions) {
                        if (ext.getAnnotation().getSimpleName().equals(part.getName())) {
                            statement.addListener(ext.createUpdateListener(context, part, model));
                        }
                    }
                }
            }
        }
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
                    this.eventTypes.add(strAlias);
                }
            }
        }

        return config;
    }

    public void destroy() {
        esperService.destroy();
    }

    public boolean isEventSupported(JetstreamEvent event) {
        return eventTypes.contains(event.getEventType());
    }

    public void process(Session session, JetstreamEvent event) {
        context.setCurrentSession(session);
        context.setMainSession(session);
        context.setEvent(event);
        if (session.getDynamicAttributes() == null) {
            session.setDynamicAttributes(new HashMap<String, Object>());
        }
        if (session.getInitialAttributes() == null) {
            session.setInitialAttributes(new HashMap<String, Object>());
        }
        sessionVariable.resetAttributes(session.getDynamicAttributes());
        sessionVariable.resetSessionData(session, session);
        metadataVariable.resetAttributes(session.getInitialAttributes());
        processEPL(event);
        if (context.isMetadataChanged()) {
            session.setMetadataLastModifiedTime(session.getLastModifiedTime());
        }
    }

    public void process(SubSession subSession, Session session, JetstreamEvent event) {
        context.setCurrentSession(subSession);
        context.setMainSession(session);
        context.setEvent(event);
        if (subSession.getDynamicAttributes() == null) {
            subSession.setDynamicAttributes(new HashMap<String, Object>());
        }
        if (subSession.getInitialAttributes() == null) {
            subSession.setInitialAttributes(new HashMap<String, Object>());
        }
        sessionVariable.resetAttributes(subSession.getDynamicAttributes());
        sessionVariable.resetSessionData(subSession, session);
        metadataVariable.resetAttributes(subSession.getInitialAttributes());
        parentMetadataVariable.resetAttributes(session.getInitialAttributes());
        parentSessionVariable.resetAttributes(session.getDynamicAttributes());
        parentSessionVariable.resetSessionData(session, session);
        processEPL(event);
    }

    private void processEPL(JetstreamEvent event) {
        Object sesionizerList = event.remove(SessionizerProcessor.SESSIONIZER_LIST);
        try {
            esperService.getEPRuntime().sendEvent(event, event.getEventType());
        } finally {
            if (sesionizerList != null) {
                event.put(SessionizerProcessor.SESSIONIZER_LIST, sesionizerList);
            }
        }
    }

}
