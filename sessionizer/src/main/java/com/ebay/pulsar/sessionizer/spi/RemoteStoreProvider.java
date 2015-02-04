/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.spi;

import com.ebay.pulsar.sessionizer.model.Session;


/**
 * Provide remote session store.
 * 
 * The integration point between sessionizer and external remote store.
 * The provider provide a layer to decouple external store with sessionizer.
 * 
 * @author xingwang
 */
public interface RemoteStoreProvider {
    /**
     * Init the remote store.
     * 
     * Invoked by sessionizer processor when sessionizer processor initialized.
     * 
     * @param callback
     */
    void init(RemoteStoreCallback callback);

    /**
     * Start the provider.
     * 
     * Invoked by sessionizer processor after the init method.
     */
    void start();

    /**
     * Stop the provider.
     * 
     * Invoked by sessionizer processor when shutdown.
     */
    void stop();


    /**
     * Send heart beat check of the current client.
     * 
     * @param timestamp
     */
    void updateHeartbeat(long timestamp);


    /**
     * Check leaked expired sessions on remote store.
     */
    void checkExpiredSession();

    /**
     * Async load the session by the key.
     * 
     * affinityKey is used for send response back to sessionizer processor.
     * 
     * @param key
     * @param affinityKey
     * @return
     */
    boolean asyncLoad(String key, String affinityKey);

    /**
     * Whether support async load.
     * 
     * @return
     */
    boolean asyncLoadSupport();

    /**
     * Insert a new session.
     * 
     * @param session
     * @param key
     */
    void insert(Session session, String key);

    /**
     * Load a session by key.
     * 
     * @param key
     * @return
     */
    Session load(String key);

    /**
     * Delete the session.
     * 
     * @param session
     * @param key
     */
    void delete(Session session, String key);

    /**
     * Update the session.
     * 
     * @param session
     * @param key
     */
    void update(Session session, String key);

}
