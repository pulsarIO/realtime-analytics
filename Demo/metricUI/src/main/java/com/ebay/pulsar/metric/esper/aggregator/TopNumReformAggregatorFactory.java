package com.ebay.pulsar.metric.esper.aggregator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.hook.AggregationFunctionFactory;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.service.AggregationValidationContext;

public class TopNumReformAggregatorFactory implements
        AggregationFunctionFactory {

    private static final Logger LOGGER = LoggerFactory
            .getLogger("com.ebay.pulsar.metric.esper.aggregator.TopNumReformAggregatorFactory");

    @Override
    public void setFunctionName(String functionName) {
    }

    @Override
    public void validate(AggregationValidationContext validationContext) {
        if (validationContext.getParameterTypes().length != 3)
            LOGGER.error("TopK Aggregation Function requires 3 parameters viz. max capacity, # top elements and element, - topNumReform(maxCapacit, topElements, element)");
    }

    @Override
    public AggregationMethod newAggregator() {
        return new TopNumReformAggregator();
    }

    @Override
    public Class getValueType() {
        return Map.class;
    }

}
