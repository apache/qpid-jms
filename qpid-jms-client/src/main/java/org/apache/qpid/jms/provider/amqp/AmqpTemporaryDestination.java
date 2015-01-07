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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
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

    public static final Symbol DYNAMIC_NODE_LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    private static final String TEMP_QUEUE_CREATOR = "temp-queue-creator:";
    private static final String TEMP_TOPIC_CREATOR = "temp-topic-creator:";

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

        EndpointState remoteState = getEndpoint().getRemoteState();
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
        String destinationName = getEndpoint().getRemoteTarget().getAddress();

        this.resource.setName(destinationName);

        LOG.trace("Updated temp destination to: {} from: {}", resource, oldDestinationName);

        super.opened();
    }

    @Override
    protected void doOpen() {
        // Form a link name, use the local generated name with a prefix to aid debugging
        String localDestinationName = resource.getName();
        String senderLinkName = null;
        if (resource.isQueue()) {
            senderLinkName = TEMP_QUEUE_CREATOR + localDestinationName;
        } else {
            senderLinkName = TEMP_TOPIC_CREATOR + localDestinationName;
        }

        // Just use a bare Source, this is a producer which
        // wont send anything and the link name is unique.
        Source source = new Source();

        Target target = new Target();
        target.setDynamic(true);
        target.setDurable(TerminusDurability.NONE);
        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        // Set the dynamic node lifetime-policy
        Map<Symbol, Object> dynamicNodeProperties = new HashMap<Symbol, Object>();
        dynamicNodeProperties.put(DYNAMIC_NODE_LIFETIME_POLICY, DeleteOnClose.getInstance());
        target.setDynamicNodeProperties(dynamicNodeProperties);

        // Set the capability to indicate the node type being created
        if (resource.isQueue()) {
            target.setCapabilities(AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY);
        } else {
            target.setCapabilities(AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY);
        }

        Sender sender = session.getProtonSession().sender(senderLinkName);
        sender.setSource(source);
        sender.setTarget(target);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        setEndpoint(sender);

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
        return getEndpoint();
    }

    public JmsDestination getJmsDestination() {
        return this.resource;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + resource + "}";
    }
}
