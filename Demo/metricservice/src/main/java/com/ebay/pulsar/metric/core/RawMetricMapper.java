/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metric.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class RawMetricMapper {

    public RawNumericMetric map(Row row) {
        RawNumericMetric metricRow =  new RawNumericMetric(row.getString(0), row.getString(1), row.getDate(2).getTime(), row.getInt(3));
        ColumnDefinitions columeDef = row.getColumnDefinitions();
        List<Definition> columeDefList = columeDef.asList();
        Map<String, String> tagMap = new HashMap<String, String>();
        for(Definition def: columeDefList){
            if(def.getName().startsWith("tag_")){
                tagMap.put(def.getName(), row.getString(def.getName()));
            }
        }
        
        if(tagMap.size()>0){
            metricRow.setTagMap(tagMap);
        }
        return metricRow;
    }

    public List<RawNumericMetric> map(ResultSet resultSet) {

        List<RawNumericMetric> metrics = new ArrayList<RawNumericMetric>();

        for (Row row : resultSet) {
            try{
                metrics.add(map(row));
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
        return metrics;
    }
}
