/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
 */
package com.ebay.pulsar.metric.util;

import java.io.IOException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public final class JsonUtil {

    private static ObjectWriter ow = new ObjectMapper().writer();

    public static <T> String toJson(T t) {
        String json = null;
        try {
            json = ow.writeValueAsString(t);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }

    /** Cannot instantiate. */
    private JsonUtil() {
    }

}
