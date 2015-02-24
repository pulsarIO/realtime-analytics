/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.collector.validation;

import java.util.List;
import java.util.Map;

public class DefaultValidator implements Validator {
    private List<String> mandatoryTags;


    public List<String> getMandatoryTags() {
        return mandatoryTags;
    }


    public void setMandatoryTags(List<String> mandatoryTags) {
        this.mandatoryTags = mandatoryTags;
    }


    @Override
    public ValidationResult validate(Map<String, Object> event, String eventType) {
        if (mandatoryTags != null) {
            for (int i = 0, t = mandatoryTags.size(); i < t; i++) {
                if (!event.containsKey(mandatoryTags.get(i))) {
                    ValidationResult r = new ValidationResult();
                    r.setSuccess(false);
                    r.setDetail(mandatoryTags.get(i) + " is mandatory");
                    return r;
                }
            }
        }
        return null;
    }

}
