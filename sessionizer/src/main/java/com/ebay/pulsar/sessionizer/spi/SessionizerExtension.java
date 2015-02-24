/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.spi;

import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.AnnotationPart;
import com.espertech.esper.client.soda.EPStatementObjectModel;

/**
 * Extension for sessionizer Esper engine.
 * 
 * @author xingwang
 */
public interface SessionizerExtension {
    /**
     * The annotation class.
     * 
     * @return
     */
    Class<?> getAnnotation();

    /**
     * The listener associated with the annotation.
     * 
     * @param context
     * @param annoation
     * @param model
     * @return
     */
    UpdateListener createUpdateListener(SessionizerContext context, AnnotationPart annoation, EPStatementObjectModel model);

}
