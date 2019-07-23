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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;

import java.util.Arrays;
import java.util.List;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpAnonymousFallbackProducer;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpFixedProducer;
import org.apache.qpid.jms.provider.amqp.AmqpProducer;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource builder responsible for creating and opening an AmqpProducer instance.
 */
public class AmqpProducerBuilder extends AmqpResourceBuilder<AmqpProducer, AmqpSession, JmsProducerInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProducerBuilder.class);
    private boolean validateDelayedDeliveryLinkCapability;

    public AmqpProducerBuilder(AmqpSession parent, JmsProducerInfo resourceInfo) {
        super(parent, resourceInfo);
    }

    @Override
    public void buildResource(final AsyncResult request) {
        if (getResourceInfo().getDestination() == null && !getParent().getConnection().getProperties().isAnonymousRelaySupported()) {
            LOG.debug("Creating an AmqpAnonymousFallbackProducer");
            new AmqpAnonymousFallbackProducer(getParent(), getResourceInfo());
            request.onSuccess();
        } else {
            LOG.debug("Creating AmqpFixedProducer for: {}", getResourceInfo().getDestination());
            super.buildResource(request);
        }
    }

    @Override
    protected Sender createEndpoint(JmsProducerInfo resourceInfo) {
        JmsDestination destination = resourceInfo.getDestination();
        AmqpConnection connection = getParent().getConnection();

        String targetAddress = AmqpDestinationHelper.getDestinationAddress(destination, connection);

        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };
        String sourceAddress = resourceInfo.getId().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        source.setOutcomes(outcomes);
        // TODO: default outcome. Accepted normally, Rejected for transaction controller?

        Target target = new Target();
        target.setAddress(targetAddress);
        Symbol typeCapability =  AmqpDestinationHelper.toTypeCapability(destination);
        if (typeCapability != null) {
            target.setCapabilities(typeCapability);
        }

        String senderName = "qpid-jms:sender:" + sourceAddress + ":" + targetAddress;

        Sender sender = getParent().getEndpoint().sender(senderName);
        sender.setSource(source);
        sender.setTarget(target);
        if (resourceInfo.isPresettle()) {
            sender.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        if (!connection.getProperties().isDelayedDeliverySupported()) {
            validateDelayedDeliveryLinkCapability = true;
            sender.setDesiredCapabilities(new Symbol[] { AmqpSupport.DELAYED_DELIVERY });
        }

        return sender;
    }

    @Override
    protected AmqpProducer createResource(AmqpSession parent, JmsProducerInfo resourceInfo, Sender endpoint) {
        return new AmqpFixedProducer(getParent(), getResourceInfo(), endpoint);
    }

    @Override
    protected void afterOpened() {
        if (validateDelayedDeliveryLinkCapability) {
            Symbol[] remoteOfferedCapabilities = endpoint.getRemoteOfferedCapabilities();

            boolean supported = false;
            if (remoteOfferedCapabilities != null) {
                List<Symbol> list = Arrays.asList(remoteOfferedCapabilities);
                if (list.contains(DELAYED_DELIVERY)) {
                    supported = true;
                }
            }

            getResource().setDelayedDeliverySupported(supported);
        }
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }

    @Override
    protected ProviderException getDefaultOpenAbortException() {
        // Verify the attach response contained a non-null target
        org.apache.qpid.proton.amqp.transport.Target target = getEndpoint().getRemoteTarget();
        if (target != null) {
            return super.getDefaultOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new ProviderInvalidDestinationException("Link creation was refused");
        }
    }
}
