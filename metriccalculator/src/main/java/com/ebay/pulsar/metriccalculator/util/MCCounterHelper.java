/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.util;

import java.util.Map;
import java.util.Set;

import com.ebay.jetstream.event.JetstreamEvent;

public class MCCounterHelper {
    public static String COUNTEREVENT = "MC_Metric";
    public static String MultiCOUNTEREVENT = "MC_MultiMetric";
    public static String COUNTERGROUPBYEVENT = "group";
    public static String COUNTERGROUPBYEVENTTAG = "tag";
    public static String COUNTERGROUPBYMETICTIMEEVENT = "mctimegroup";

    public static String TAG_METRICTIME = "tag_" + MCConstant.METRIC_TIME;

    public static String AVG = "avg";

    public static boolean isMCCounterEvent(JetstreamEvent event) {
        if ((event != null) && event.getEventType().contains(COUNTEREVENT)) {
            return true;
        }
        return false;
    }

    public static boolean isMCMultiCounterEvent(JetstreamEvent event) {
        if ((event != null) && event.getEventType().contains(MultiCOUNTEREVENT)) {
            return true;
        }
        return false;
    }

    public static boolean isGroupByCounterEvent(JetstreamEvent event) {
        if ((event != null)
                && event.getEventType().contains(COUNTERGROUPBYEVENT)) {
            return true;
        }
        return false;
    }

    public static boolean isGroupByCounterEventWithTag(JetstreamEvent event,
            Map<String, String> tags) {
        boolean result = false;
        if ((event != null)
                && event.getEventType().contains(COUNTERGROUPBYEVENT)) {
            Set<String> keys = event.keySet();
            String[] keyArray = keys.toArray(new String[0]);
            for (int i = 0; i < keyArray.length; i++) {
                if (keyArray[i].contains(COUNTERGROUPBYEVENTTAG)) {
                    result = true;
                    if (tags != null) {
                        tags.put(keyArray[i],
                                String.valueOf(event.get(keyArray[i])));
                    }
                }
            }

            if (event.getEventType().contains(COUNTERGROUPBYMETICTIMEEVENT)) {
                tags.put(TAG_METRICTIME,
                        String.valueOf(event.get(MCConstant.METRIC_TIME)));
            }
        }
        return result;
    }

    public static boolean isAvgEvent(String metricName) {
        if (metricName.toLowerCase().contains(AVG)) {
            return true;
        }
        return false;
    }
}
