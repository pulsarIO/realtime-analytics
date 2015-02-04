/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.util;

/**
 * Constants used by sessionizer.
 * 
 * @author xingwang
 */
public interface Constants {
    //Tags used by internal events
    /**
     * Session instance key.
     */
    String EVENT_PAYLOAD_SESSION_OBJ = "SessionObj";
    /**
     * Session type key.
     */
    String EVENT_PAYLOAD_SESSION_TYPE = "SessionType";
    /**
     * Session unique id key.
     */
    String EVENT_PAYLOAD_SESSION_UNIQUEID = "SessionUniqueId";
    /**
     * Session payload key.
     */
    String EVENT_PAYLOAD_SESSION_PAYLOAD = "SessionPayloadInBytes";
    /**
     * Session metadata key.
     */
    String EVENT_PAYLOAD_SESSION_METADATA = "SessionMetadataInBytes";

    // Internal event type
    /**
     * Transferred session between sessionizer nodes.
     */
    String EVENT_TYPE_SESSION_TRANSFERED_EVENT = "_IntraSessionExpiredEvent";
    /**
     * Remote session expired event.
     */
    String EVENT_TYPE_SESSION_EXPIRED_EVENT = "_RemoteSessionExpired";
    /**
     * Session loaded from remote store event.
     */
    String EVENT_TYPE_SESSION_LOAD_EVENT = "_SessionLoaded";
}
