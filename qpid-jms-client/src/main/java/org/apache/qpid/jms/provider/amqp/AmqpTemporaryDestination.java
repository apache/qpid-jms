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

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AmqpTemporaryDestination extends AmqpAbstractResource<JmsDestination, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTemporaryDestination.class);

    private final AmqpConnection connection;
    private final AmqpSession session;

    public AmqpTemporaryDestination(AmqpSession session, JmsDestination destination) {
        super(destination);
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public void processStateChange() {
        // TODO - We might want to check on our producer to see if it becomes closed
        //        which might indicate that the broker purged the temporary destination.

        EndpointState remoteState = endpoint.getRemoteState();
        if (remoteState == EndpointState.ACTIVE) {
            LOG.trace("Temporary Destination: {} is now open", this.resource);
            opened();
        } else if (remoteState == EndpointState.CLOSED) {
            LOG.trace("Temporary Destination: {} is now closed", this.resource);
            closed();
        }
    }

    @Override
    public void opened() {

        // Once our producer is opened we can read the updated name from the target address.
        String oldDestinationName = resource.getName();
        String destinationName = this.endpoint.getRemoteTarget().getAddress();

        this.resource.setName(destinationName);

        LOG.trace("Updated temp destination to: {} from: {}", resource, oldDestinationName);

        super.opened();
    }

    @Override
    protected void doOpen() {

        String sourceAddress = resource.getName();
        if (resource.isQueue()) {
            sourceAddress = connection.getTempQueuePrefix() + sourceAddress;
        } else {
            // TODO - AMQ doesn't support temp topics so we make everything a temp queue for now
            sourceAddress = connection.getTempQueuePrefix() + sourceAddress;
        }
        Source source = new Source();
        source.setAddress(sourceAddress);
        Target target = new Target();
        target.setDynamic(true);
        target.setDurable(TerminusDurability.NONE);
        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        String senderName = sourceAddress;
        endpoint = session.getProtonSession().sender(senderName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        this.connection.addTemporaryDestination(this);

        super.doOpen();
    }

    @Override
    protected void doClose() {
        this.connection.removeTemporaryDestination(this);

        super.doClose();
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Sender getProtonSender() {
        return this.endpoint;
    }

    public JmsDestination getJmsDestination() {
        return this.resource;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + resource + "}";
    }
}
