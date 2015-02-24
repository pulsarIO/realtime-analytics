/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.annotation;

/**
 * An annotation to provide hint for the sessionizer.
 * 
 * The value of the annotation is a session profile name, and the EPL statement
 * should return the session identifier (_pk_) of the event, and it must be a String.
 * And it can also return two optional tags: _timestamp_ to indicate the Event timestamp,
 * the _timestamp_ type must be Long, if it is not available, it will use system current time;
 * _duration_ to indicate max inactivity time of the session, if it is not available, it will
 * use default ttl from the session profile.
 * 
 * @author xingwang
 *
 */
public @interface Session {
    /**
     * The name of the session profile.
     * 
     * @return
     */
    String value();
}

