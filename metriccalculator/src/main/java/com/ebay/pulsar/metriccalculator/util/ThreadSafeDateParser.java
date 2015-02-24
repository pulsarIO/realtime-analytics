/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class ThreadSafeDateParser {
    private static final ThreadLocal<Map<String, DateFormat>> PARSERS = new ThreadLocal<Map<String, DateFormat>>() {
        protected Map<String, DateFormat> initialValue() {
            return new HashMap<String, DateFormat>();
        }
    };

    static private final DateFormat getParser(final String Pattern,
            final TimeZone zone) {
        Map<String, DateFormat> parserMap = PARSERS.get();
        DateFormat df = parserMap.get(Pattern);
        if (null == df) {
            // if parser for the same pattern does not exist yet, create one and
            // save it into map

            df = new SimpleDateFormat(Pattern);
            df.setTimeZone(zone);
            parserMap.put(Pattern, df);
        }
        return df;
    }

    static public Date parse(final String StrDate, final String Pattern,
            final TimeZone zone) throws ParseException {
        return getParser(Pattern, zone).parse(StrDate);
    }

    static public String format(final Date date, final String Pattern,
            final TimeZone zone) throws ParseException {
        return getParser(Pattern, zone).format(date);
    }
}
