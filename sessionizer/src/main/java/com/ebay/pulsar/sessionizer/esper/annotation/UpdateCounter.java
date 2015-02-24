/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.annotation;

/**
 * Increment a session counter with the selected event count.
 * 
 * The sessionizer will auto create a counter if it does not exist.
 * 
 * @author xingwang
 *
 */
public @interface UpdateCounter {
    /**
     * Counter name.
     * 
     * @return
     */
    String value();
}

