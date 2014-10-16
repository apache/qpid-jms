/**
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
package org.apache.qpid.jms.provider.amqp;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;

/**
 * Subclass of the standard session object used solely by AmqpConnection to
 * aid in managing connection resources that require a persistent session.
 */
public class AmqpConnectionSession extends AmqpSession {

    /**
     * Create a new instance of a Connection owned Session object.
     *
     * @param connection
     *        the connection that owns this session.
     * @param info
     *        the <code>JmsSessionInfo</code> for the Session to create.
     */
    public AmqpConnectionSession(AmqpConnection connection, JmsSessionInfo info) {
        super(connection, info);
    }

    /**
     * Used to remove an existing durable topic subscription from the remote broker.
     *
     * @param subscriptionName
     *        the subscription name that is to be removed.
     * @param request
     *        the request that awaits the completion of this action.
     */
    public void unsubscribe(String subscriptionName, AsyncResult request) {
        request.onSuccess();
    }
}
