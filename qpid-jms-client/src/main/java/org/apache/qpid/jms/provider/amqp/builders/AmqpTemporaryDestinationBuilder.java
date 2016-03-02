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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DYNAMIC_NODE_LIFETIME_POLICY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TEMP_QUEUE_CREATOR;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TEMP_TOPIC_CREATOR;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpTemporaryDestination;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource builder responsible for creating and opening an AmqpTemporaryDestination instance.
 */
public class AmqpTemporaryDestinationBuilder extends AmqpResourceBuilder<AmqpTemporaryDestination, AmqpSession, JmsTemporaryDestination, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTemporaryDestinationBuilder.class);

    public AmqpTemporaryDestinationBuilder(AmqpSession parent, JmsTemporaryDestination resourceInfo) {
        super(parent, resourceInfo);
    }

    @Override
    protected Sender createEndpoint(JmsTemporaryDestination resourceInfo) {
        // Form a link name, use the local generated name with a prefix to aid debugging
        String localDestinationName = resourceInfo.getName();
        String senderLinkName = null;
        if (resourceInfo.isQueue()) {
            senderLinkName = "qpid-jms:" + TEMP_QUEUE_CREATOR + localDestinationName;
        } else {
            senderLinkName = "qpid-jms:" + TEMP_TOPIC_CREATOR + localDestinationName;
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
        if (resourceInfo.isQueue()) {
            target.setCapabilities(AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY);
        } else {
            target.setCapabilities(AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY);
        }

        Sender sender = getParent().getEndpoint().sender(senderLinkName);
        sender.setSource(source);
        sender.setTarget(target);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        return sender;
    }

    @Override
    protected AmqpTemporaryDestination createResource(AmqpSession parent, JmsTemporaryDestination resourceInfo, Sender endpoint) {
        return new AmqpTemporaryDestination(getParent(), getResourceInfo(), endpoint);
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }

    @Override
    protected void afterOpened() {
        if (!isClosePending()) {
            // Once our sender is opened we can read the updated name from the target address.
            String oldDestinationName = resourceInfo.getName();
            String destinationName = getEndpoint().getRemoteTarget().getAddress();

            resourceInfo.setName(destinationName);

            LOG.trace("Updated temp destination to: {} from: {}", destinationName, oldDestinationName);
        }
    }
}
