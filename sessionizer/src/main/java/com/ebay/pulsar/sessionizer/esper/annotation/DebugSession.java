/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.annotation;

/**
 * This only used for debugging.
 * 
 * There is a counter to count the statement be executed. And if the colname
 * is available, it will create a counter for each value and use the counter name
 * as prefix. The audit will be used to log the event detail.
 * 
 * 
 * @author xingwang
 *
 */
public @interface DebugSession {
    /**
     * Counter name.
     * 
     * @return
     */
    String counter();
    String colname() default "";
    boolean audit() default false;
}
