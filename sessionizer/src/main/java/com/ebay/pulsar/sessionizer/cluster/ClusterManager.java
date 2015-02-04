/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.cluster;

import com.ebay.pulsar.sessionizer.impl.SessionizerProcessor;

/**
 * Sessionizer cluster manager.
 * 
 * It use a loopback consistent hash ring to determine the whole cluster info.
 * 
 * @author xingwang
 *
 */
public interface ClusterManager {
    /**
     * Max idle time of the session.
     * 
     * @return
     */
    long getMaxIdleTime();

    /**
     * The current host Id.
     * 
     * @return
     */
    long getHostId();

    /**
     * Check whether the ownership changed recently on the key.
     * 
     * @param affinityKey
     * @return
     */
    <T> boolean isOwnershipChangedRecently(T affinityKey);

    /**
     * Check whether current node has the ownership of the key.
     * 
     * @param affinityKey
     * @return
     */
    <T> boolean hasOwnership(T affinityKey);

    /**
     * Is current node the leader.
     * 
     * @return
     */
    boolean isLeader();

    /**
     * Check whether the host specified by the hostId live or not
     * @param hostId
     * @return
     */
    boolean isHostLive(long hostId);

    /**
     * Inject the sessionizer processor.
     * 
     * @param processor
     */
    void setSessionizer(SessionizerProcessor processor);

    /**
     * Get the host failure time.
     * 
     * @param hostId
     * @return
     */
    long getHostFailureTime(long hostId);
}
