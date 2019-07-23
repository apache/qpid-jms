/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp.builders;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;

/**
 * Specialized Builder that create a Connection that is intended to be immediately
 * closed.
 */
public class AmqpClosedConnectionBuilder extends AmqpConnectionBuilder {

    public AmqpClosedConnectionBuilder(AmqpProvider parent, JmsConnectionInfo resourceInfo) {
        super(parent, resourceInfo);
    }

    @Override
    protected AsyncResult createRequestIntercepter(final AsyncResult request) {
        return request;
    }

    @Override
    protected void afterOpened() {
        getEndpoint().close();
    }

    @Override
    protected void afterClosed(AmqpConnection resource, JmsConnectionInfo resourceInfo) {
        // If the resource closed and no error was given, we just closed it now to avoid
        // failing the request with a default error which creates log spam.
        if (!hasRemoteError()) {
            request.onSuccess();
        }
    }

    @Override
    protected boolean isClosePending() {
        return true;
    }
}
