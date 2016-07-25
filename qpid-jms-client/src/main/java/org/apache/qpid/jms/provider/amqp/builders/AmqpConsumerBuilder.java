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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.COPY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.JMS_NO_LOCAL_SYMBOL;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.JMS_SELECTOR_SYMBOL;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.MODIFIED_FAILED;

import java.util.HashMap;
import java.util.Map;

import javax.jms.InvalidDestinationException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.filters.AmqpJmsNoLocalType;
import org.apache.qpid.jms.provider.amqp.filters.AmqpJmsSelectorType;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Resource builder responsible for creating and opening an AmqpConsumer instance.
 */
public class AmqpConsumerBuilder extends AmqpResourceBuilder<AmqpConsumer, AmqpSession, JmsConsumerInfo, Receiver> {

    public AmqpConsumerBuilder(AmqpSession parent, JmsConsumerInfo consumerInfo) {
        super(parent, consumerInfo);
    }

    @Override
    protected Receiver createEndpoint(JmsConsumerInfo resourceInfo) {
        JmsDestination destination = resourceInfo.getDestination();
        String subscription = AmqpDestinationHelper.INSTANCE.getDestinationAddress(destination, getParent().getConnection());

        Source source = new Source();
        source.setAddress(subscription);
        Target target = new Target();

        configureSource(source);

        String receiverName = "qpid-jms:receiver:" + resourceInfo.getId() + ":" + subscription;
        if (resourceInfo.getSubscriptionName() != null && !resourceInfo.getSubscriptionName().isEmpty()) {
            // In the case of Durable Topic Subscriptions the client must use the same
            // receiver name which is derived from the subscription name property.
            receiverName = resourceInfo.getSubscriptionName();
        }

        Receiver receiver = getParent().getEndpoint().receiver(receiverName);
        receiver.setSource(source);
        receiver.setTarget(target);
        if (resourceInfo.isBrowser() || resourceInfo.isPresettle()) {
            receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        return receiver;
    }

    @Override
    protected AmqpConsumer createResource(AmqpSession parent, JmsConsumerInfo resourceInfo, Receiver endpoint) {
        return new AmqpConsumer(parent, resourceInfo, endpoint);
    }

    @Override
    protected Exception getOpenAbortException() {
        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source source = endpoint.getRemoteSource();
        if (source != null) {
            return super.getOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new InvalidDestinationException("Link creation was refused");
        }
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return endpoint.getRemoteSource() == null;
    }

    //----- Internal implementation ------------------------------------------//

    private void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<Symbol, DescribedType>();
        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                          Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };

        if (resourceInfo.getSubscriptionName() != null && !resourceInfo.getSubscriptionName().isEmpty()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

		if (this.parent.getConnection().getProvider().getDefaultExpiryPolicy() != null) {
			source.setExpiryPolicy(this.parent.getConnection().getProvider().getDefaultExpiryPolicy());
		}
		if (this.parent.getConnection().getProvider().getDefaultTimeout() != null) {
			source.setTimeout(this.parent.getConnection().getProvider().getDefaultTimeout());
		}

        if (resourceInfo.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        Symbol typeCapability =  AmqpDestinationHelper.INSTANCE.toTypeCapability(resourceInfo.getDestination());
        if(typeCapability != null) {
            source.setCapabilities(typeCapability);
        }

        source.setOutcomes(outcomes);
        source.setDefaultOutcome(MODIFIED_FAILED);

        if (resourceInfo.isNoLocal()) {
            filters.put(JMS_NO_LOCAL_SYMBOL, AmqpJmsNoLocalType.NO_LOCAL);
        }

        if (resourceInfo.getSelector() != null && !resourceInfo.getSelector().trim().equals("")) {
            filters.put(JMS_SELECTOR_SYMBOL, new AmqpJmsSelectorType(resourceInfo.getSelector()));
        }

        if (!filters.isEmpty()) {
            source.setFilter(filters);
        }
    }
}
