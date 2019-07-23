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
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SHARED_SUBS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSRuntimeException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpSubscriptionTracker;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.amqp.filters.AmqpJmsNoLocalType;
import org.apache.qpid.jms.provider.amqp.filters.AmqpJmsSelectorType;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.jms.provider.exceptions.ProviderUnsupportedOperationException;
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

    boolean validateSharedSubsLinkCapability;
    boolean sharedSubsNotSupported;

    public AmqpConsumerBuilder(AmqpSession parent, JmsConsumerInfo consumerInfo) {
        super(parent, consumerInfo);
    }

    @Override
    protected Receiver createEndpoint(JmsConsumerInfo resourceInfo) {
        JmsDestination destination = resourceInfo.getDestination();
        String address = AmqpDestinationHelper.getDestinationAddress(destination, getParent().getConnection());

        Source source = new Source();
        source.setAddress(address);
        Target target = new Target();

        configureSource(source);

        String receiverLinkName = null;
        String subscriptionName = resourceInfo.getSubscriptionName();
        if (subscriptionName != null && !subscriptionName.isEmpty()) {
            AmqpConnection connection = getParent().getConnection();

            if (resourceInfo.isShared() && !connection.getProperties().isSharedSubsSupported()) {
                validateSharedSubsLinkCapability = true;
            }

            AmqpSubscriptionTracker subTracker = connection.getSubTracker();

            // Validate subscriber type allowed given existing active subscriber types.
            if (resourceInfo.isShared() && resourceInfo.isDurable()) {
                if(subTracker.isActiveExclusiveDurableSub(subscriptionName)) {
                    // Don't allow shared sub if there is already an active exclusive durable sub
                    throw new JMSRuntimeException("A non-shared durable subscription is already active with name '" + subscriptionName + "'");
                }
            } else if (!resourceInfo.isShared() && resourceInfo.isDurable()) {
                if (subTracker.isActiveExclusiveDurableSub(subscriptionName)) {
                    // Exclusive durable sub is already active
                    throw new JMSRuntimeException("A non-shared durable subscription is already active with name '" + subscriptionName + "'");
                } else if (subTracker.isActiveSharedDurableSub(subscriptionName)) {
                    // Don't allow exclusive durable sub if there is already an active shared durable sub
                    throw new JMSRuntimeException("A shared durable subscription is already active with name '" + subscriptionName + "'");
                }
            }

            // Get the link name for the subscription. Throws if certain further validations fail.
            receiverLinkName = subTracker.reserveNextSubscriptionLinkName(subscriptionName, resourceInfo);
        }

        if (receiverLinkName == null) {
            receiverLinkName = "qpid-jms:receiver:" + resourceInfo.getId() + ":" + address;
        }

        Receiver receiver = getParent().getEndpoint().receiver(receiverLinkName);
        receiver.setSource(source);
        receiver.setTarget(target);
        if (resourceInfo.isBrowser() || resourceInfo.isPresettle()) {
            receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        if (validateSharedSubsLinkCapability) {
            receiver.setDesiredCapabilities(new Symbol[] { AmqpSupport.SHARED_SUBS });
        }

        return receiver;
    }

    @Override
    protected void afterOpened() {
        if (validateSharedSubsLinkCapability) {
            Symbol[] remoteOfferedCapabilities = endpoint.getRemoteOfferedCapabilities();

            boolean supported = false;
            if (remoteOfferedCapabilities != null) {
                List<Symbol> list = Arrays.asList(remoteOfferedCapabilities);
                if (list.contains(SHARED_SUBS)) {
                    supported = true;
                }
            }

            if (!supported) {
                sharedSubsNotSupported = true;

                if (resourceInfo.isDurable()) {
                    endpoint.detach();
                } else {
                    endpoint.close();
                }
            }
        }
    }

    @Override
    protected void afterClosed(AmqpConsumer resource, JmsConsumerInfo info) {
        // If the resource being built is closed during the creation process
        // then this is a failure, we need to ensure we don't track it.
        AmqpConnection connection = getParent().getConnection();
        AmqpSubscriptionTracker subTracker = connection.getSubTracker();
        subTracker.consumerRemoved(info);
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) {
        handleClosed(provider, null);
    }

    @Override
    protected AmqpConsumer createResource(AmqpSession parent, JmsConsumerInfo resourceInfo, Receiver endpoint) {
        return new AmqpConsumer(parent, resourceInfo, endpoint);
    }

    @Override
    protected ProviderException getDefaultOpenAbortException() {
        if (sharedSubsNotSupported) {
            return new ProviderUnsupportedOperationException("Remote peer does not support shared subscriptions");
        }

        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source source = endpoint.getRemoteSource();
        if (source != null) {
            return super.getDefaultOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new ProviderInvalidDestinationException("Link creation was refused");
        }
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return sharedSubsNotSupported || endpoint.getRemoteSource() == null;
    }

    //----- Internal implementation ------------------------------------------//

    private void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<Symbol, DescribedType>();
        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                          Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };

        if (resourceInfo.isDurable()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

        if (resourceInfo.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        // Capabilities
        LinkedList<Symbol> capabilities = new LinkedList<>();

        Symbol typeCapability =  AmqpDestinationHelper.toTypeCapability(resourceInfo.getDestination());
        if (typeCapability != null){
            capabilities.add(typeCapability);
        }

        if (resourceInfo.isShared()) {
            capabilities.add(AmqpSupport.SHARED);

            if(!resourceInfo.isExplicitClientID()) {
                capabilities.add(AmqpSupport.GLOBAL);
            }
        }

        if (!capabilities.isEmpty()) {
            Symbol[] capArray = capabilities.toArray(new Symbol[capabilities.size()]);
            source.setCapabilities(capArray);
        }

        //Outcomes
        source.setOutcomes(outcomes);
        source.setDefaultOutcome(MODIFIED_FAILED);

        // Filters
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
