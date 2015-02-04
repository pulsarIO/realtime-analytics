/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.spi;

import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.pulsar.sessionizer.impl.SessionizerErrorManager.ErrorType;
import com.ebay.pulsar.sessionizer.model.Session;

/**
 * Callback for remote store provider.
 * 
 * @author xingwang
 */
public interface RemoteStoreCallback {

    /**
     * Register error into sessionizer error manager.
     * 
     * @param ex
     * @param type
     */
    void registerError(Throwable ex, ErrorType type);

    /**
     * Register error into sessionizer error manager.
     * 
     * @param category
     * @param type
     * @param message
     */
    void registerError(String category, ErrorType type, String message);

    /**
     * Send alert for remote store. Utilized the sessionizer alert listener.
     * 
     * @param message
     * @param severity
     */
    void sendRemoteStoreAlert(String message, AlertStrength severity);

    /**
     * Notify a remote session loaded.
     * 
     * @param uniqueSessionId
     * @param sojSession
     * @param affinityKey
     */
    void remoteSessionLoaded(String uniqueSessionId, Session sojSession, String affinityKey);

    /**
     * Send a remote session expired check event across the cluster.
     * 
     * The node which received the event should load from remote store and check whether it expired or not.
     * 
     * @param uniqueSessionId
     * @param affinityKey
     */
    void sendExpirationCheckEvent(String uniqueSessionId, String affinityKey);
    
    /**
     * Notify the session is expired and the session should be handled by current node.
     * 
     * @param uid
     * @param payload
     * @param metadata
     * @param ak
     */
    void remoteSessionExpired(String uid,  String affinityKey, byte[] payload, byte[] metadata);
}
