/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.annotation;

/**
 * An annotation to provide hint to run sessionizer on sub session.
 * 
 * The value of the annotation is a sub session profile name, and the EPL statement
 * should return the sub session identifier (_pk_) of the event, and it must be a String.
 * And it can also return a optional tag _duration_ with Long type to indicate max inactivity
 * time of the sub session, if it is not available, it will use default ttl from the sub session profile.
 * 
 * @author xingwang
 *
 */
public @interface SubSession {
    /**
     * The name of the sub session profile.
     * 
     * @return
     */
    String value();
}

