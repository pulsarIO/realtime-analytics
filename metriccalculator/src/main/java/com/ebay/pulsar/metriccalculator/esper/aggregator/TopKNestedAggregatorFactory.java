package com.ebay.pulsar.metriccalculator.esper.aggregator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.hook.AggregationFunctionFactory;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.service.AggregationValidationContext;

public class TopKNestedAggregatorFactory implements AggregationFunctionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.pulsar.metriccalculator.esper.aggregator");

    @Override
    public void setFunctionName(String functionName) {
        // nothing needed here
    }

    // @Override
    public void validate(AggregationValidationContext validationContext) {
        if (validationContext.getParameterTypes().length != 4)
            LOGGER.error( "TopK Aggregation Function requires 4 parameters viz. max capacity, # top elements and element and split string, - topKNested(maxCapacit, topElements, element, split)");
    }

    @Override
    public AggregationMethod newAggregator() {
        return new TopKNestedAggregator();
    }

    @Override
    public Class getValueType() {
        return Map.class;
    }
}
