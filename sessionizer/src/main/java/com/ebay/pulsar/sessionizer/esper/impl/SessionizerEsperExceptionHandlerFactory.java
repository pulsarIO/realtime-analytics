/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Dual licensed under the Apache 2.0 license and the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.sessionizer.esper.impl;

import com.ebay.pulsar.sessionizer.impl.SessionizerErrorManager;
import com.espertech.esper.client.hook.ExceptionHandler;
import com.espertech.esper.client.hook.ExceptionHandlerContext;
import com.espertech.esper.client.hook.ExceptionHandlerFactory;
import com.espertech.esper.client.hook.ExceptionHandlerFactoryContext;

/**
 * Register Esper errors to unified error manager.
 * 
 * @author xingwang
 *
 */
public class SessionizerEsperExceptionHandlerFactory implements ExceptionHandlerFactory {
    private static class SessionizerExceptionHandler implements ExceptionHandler {

        @Override
        public void handle(ExceptionHandlerContext context) {
            if (errorManager != null) {
                errorManager.registerError(context.getThrowable(), SessionizerErrorManager.ErrorType.Esper);
                errorManager.registerError(context.getEngineURI(), SessionizerErrorManager.ErrorType.Esper, context.getEpl());
            }
        }

    }

    private static SessionizerErrorManager errorManager;

    public static void setErrorManager(SessionizerErrorManager errorManager) {
        SessionizerEsperExceptionHandlerFactory.errorManager = errorManager;
    }

    @Override
    public ExceptionHandler getHandler(ExceptionHandlerFactoryContext context) {
        return new SessionizerExceptionHandler();
    }

}
