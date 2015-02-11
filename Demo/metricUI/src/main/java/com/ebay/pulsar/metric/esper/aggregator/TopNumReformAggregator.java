package com.ebay.pulsar.metric.esper.aggregator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;

public class TopNumReformAggregator implements AggregationMethod {

    StreamSummary<Object> m_counter;
    Integer m_capacity;
    Integer m_topElementCnt = new Integer(10);

    @SuppressWarnings("unchecked")
    @Override
    public void enter(Object value) {
        Object[] params = (Object[]) value;

        if (m_counter == null) {
            m_capacity = (Integer) params[0];
            m_topElementCnt = (Integer) params[1];
            m_counter = new StreamSummary<Object>(m_capacity.intValue());
        }

        if (params[2] instanceof Map<?, ?>) {
            Map<Object, Long> topCounters = (Map<Object, Long>) params[2];
            Set<Object> topKeySet = topCounters.keySet();
            for (Object key : topKeySet) {
                m_counter.offer(key, topCounters.get(key).intValue());
            }
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

}
