package com.ebay.pulsar.metriccalculator.esper.aggregator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.service.AggregationValidationContext;
import com.google.common.base.Splitter;

public class TopKNestedAggregator implements AggregationMethod {

    StreamSummary<Object> m_counter;
    Integer m_capacity;
    Integer m_topElementCnt = new Integer(10);
    Splitter splitter;

    @Override
    public void enter(Object value) {

        Object[] params = (Object[]) value;

        if (m_counter == null) {
            m_capacity = (Integer) params[0];
            m_topElementCnt = (Integer) params[1];
            m_counter = new StreamSummary<Object>(m_capacity.intValue());
        }

        if (splitter == null) {
            if (params[3] instanceof Character) {
                splitter = Splitter.on((Character) params[3]);
            } else if (params[3] instanceof String) {
                splitter = Splitter.on((String) params[3]);
            }
        }
        if (params[2] instanceof String && splitter != null) {
            Iterable<String> it = splitter.split((String) params[2]);
            for (String nested : it) {
                m_counter.offer(nested);
            }
        } else {
            m_counter.offer(params[2]);
        }
    }

    @Override
    public void leave(Object value) {

    }

    @Override
    public Object getValue() {
        Map<Object, Long> topN = new LinkedHashMap<Object, Long>();
        if ((m_counter == null))
            return topN;

        List<Counter<Object>> topCounters = m_counter.topK(m_topElementCnt);

        for (Counter<Object> counter : topCounters) {
            topN.put(counter.getItem(), counter.getCount());
        }
        return topN;
    }

    @Override
    public Class getValueType() {

        return Map.class;
    }

    @Override
    public void clear() {
        m_counter = null;

    }

    // @Override
    public void validate(AggregationValidationContext validationContext) {

    }

}
