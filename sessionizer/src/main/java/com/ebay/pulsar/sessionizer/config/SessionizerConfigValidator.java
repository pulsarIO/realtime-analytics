/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Validator for sessionizer config.
 * 
 * @author xingwang
 *
 */
public class SessionizerConfigValidator {
    private final SessionizerConfig oldConfig;
    private final SessionizerConfig config;

    public SessionizerConfigValidator(SessionizerConfig oldConfig, SessionizerConfig config) {
        this.oldConfig = oldConfig;
        this.config = config;
    }

    public List<String> validate() {
        List<String> errors = new ArrayList<String>();
        if (config.getMaxIdleTime() <= 0) {
            errors.add("maxIdleTime must be positive");
        }
        if (config.getMaxIdleTime() > oldConfig.getMaxTimeSlots() * 1000) {
            errors.add("maxIdleTime is too big, should less than maxTimeSlots * 1000");
        }
        if (config.getEpl() == null || config.getRawEventDefinition() == null) {
            errors.add("must specify EPL and event definition to enable sessionization");
        }
        if (config.getMainSessionProfiles() == null) {
            errors.add("mainSessionProfile is null");
            return errors;
        }  else {
            List<Integer> sessionTypes = new ArrayList<Integer>();
            List<String> sessionNames = new ArrayList<String>();
            for (SessionProfile mp : config.getMainSessionProfiles()) {
                if (mp.getName() == null) {
                    errors.add("session profile name is null");
                    continue;
                }
                if (mp.getEpl() != null && mp.getRawEventDefinition() == null) {
                    errors.add("must specify event definition to for sessinizer "  + mp.getName());
                    continue;
                }
                if (mp.getMaxActiveTime() < mp.getDefaultTtl()) {
                    errors.add("maxActiveTime must be greater than defaultTtl");
                }
                if (sessionTypes.contains(mp.getSessionType())) {
                    errors.add("duplicate session type id " + mp.getSessionType());
                } else {
                    sessionTypes.add(mp.getSessionType());
                }
                if (sessionNames.contains(mp.getName())) {
                    errors.add("duplicate session name " + mp.getName());
                } else {
                    sessionNames.add(mp.getName());
                }

                List<SubSessionProfile> subSessionProfiles = mp.getSubSessionProfiles();
                if (subSessionProfiles != null) {
                    List<String> subProfilerIds = new ArrayList<String>();
                    for (SubSessionProfile p : subSessionProfiles) {
                        if (p.getName() == null) {
                            errors.add("sub session profile name is null");
                            continue;
                        }
                        if (p.getEpl() != null && mp.getRawEventDefinition() == null) {
                            errors.add("Must specify EPL for sub sessinizer "  + p.getName());
                            continue;
                        }
                        if (p.getDefaultTtl() <= 0 || p.getDefaultTtl() > mp.getDefaultTtl()) {
                            errors.add("sub sessionizer " + p.getName() + " default ttl must be positive and less than main sessionizer default ttl");
                        }
                        if (subProfilerIds.contains(p.getName())) {
                            errors.add("duplicate sub session " + p.getName());
                        } else {
                            subProfilerIds.add(p.getName());
                        }
                    }
                }
            }
        }
        return errors;
    }
}
