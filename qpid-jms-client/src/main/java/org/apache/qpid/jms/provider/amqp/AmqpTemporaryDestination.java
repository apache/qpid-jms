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
package org.apache.qpid.jms.provider.amqp;

import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.proton.engine.Sender;

/**
 * Manages a Temporary Destination linked to a given Connection.
 *
 * In order to create a temporary destination and keep it active for the life of the connection
 * we must create a sender with a dynamic target value.  Once the sender is open we can read
 * the actual name assigned by the broker from the target and that is the real temporary
 * destination that we will return.
 *
 * The open of the Sender instance will also allow us to catch any security errors from
 * the broker in the case where the user does not have authorization to access temporary
 * destinations.
 */
public class AmqpTemporaryDestination extends AmqpAbstractResource<JmsTemporaryDestination, Sender> {

    private final AmqpConnection connection;
    private final AmqpSession session;

    public AmqpTemporaryDestination(AmqpSession session, JmsTemporaryDestination destination, Sender endpoint) {
        super(destination, endpoint, session);

        this.session = session;
        this.connection = session.getConnection();
    }

    public AmqpConnection getConnection() {
        return connection;
    }

    public AmqpSession getSession() {
        return session;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getResourceInfo() + "}";
    }
}
