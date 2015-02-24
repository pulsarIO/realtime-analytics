/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.annotation;

/**
 * This annotation to be used to maintain a session variable with a list type.
 * 
 * It will append the selected column value into the list, and if the list reach
 * the max length, it will evict the oldest one.
 * 
 * @author xingwang
 *
 */
public @interface AppendState {
    /**
     * Variable name.
     * 
     * @return
     */
    String name();

    /**
     * Column name to select value.
     * 
     * @return
     */
    String colname();

    /**
     * Indicate whether the value must be unique.
     * 
     * @return
     */
    boolean unique() default true;

    /**
     * Max length of the list. Any value less or equals 0 means no limitation.
     * @return
     */
    int maxlength() default 0;
}
