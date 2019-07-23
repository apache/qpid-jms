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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.NoOpAsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpResourceBuilder;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of the standard session object used solely by AmqpConnection to
 * aid in managing connection resources that require a persistent session.
 */
public class AmqpConnectionSession extends AmqpSession {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectionSession.class);

    private final Map<String, AsyncResult> pendingUnsubs = new HashMap<String, AsyncResult>();

    /**
     * Create a new instance of a Connection owned Session object.
     *
     * @param connection
     *        the connection that owns this session.
     * @param info
     *        the <code>JmsSessionInfo</code> for the Session to create.
     * @param session
     *        the Proton session instance that this resource wraps.
     */
    public AmqpConnectionSession(AmqpConnection connection, JmsSessionInfo info, Session session) {
        super(connection, info, session);
    }

    /**
     * Used to remove an existing durable topic subscription from the remote broker.
     *
     * @param subscriptionName
     *        the subscription name that is to be removed.
     * @param hasClientID
     *        whether the connection has a clientID set.
     * @param request
     *        the request that awaits the completion of this action.
     */
    public void unsubscribe(String subscriptionName, boolean hasClientID, AsyncResult request) {
        AmqpSubscriptionTracker subTracker = getConnection().getSubTracker();
        String linkName = subTracker.getFirstDurableSubscriptionLinkName(subscriptionName, hasClientID);

        DurableSubscriptionReattachBuilder builder = new DurableSubscriptionReattachBuilder(this, getResourceInfo(), linkName);
        DurableSubscriptionReattachRequest subscribeRequest = new DurableSubscriptionReattachRequest(subscriptionName, builder, request);
        pendingUnsubs.put(subscriptionName, subscribeRequest);

        LOG.debug("Attempting remove of subscription: {}", subscriptionName);
        builder.buildResource(subscribeRequest);
    }

    @Override
    public void addChildResource(AmqpResource resource) {
        // When a Connection Consumer is created the Connection is doing so
        // without a known session to associate it with, we link up the consumer
        // to this session by adding this session as the provider hint on the
        // consumer's parent session ID.
        if (resource instanceof AmqpConsumer) {
            AmqpConsumer consumer = (AmqpConsumer) resource;
            consumer.getConsumerId().getParentId().setProviderHint(this);
        }

        super.addChildResource(resource);
    }

    @Override
    public void handleResourceClosure(AmqpProvider provider, ProviderException cause) {
        List<AsyncResult> pending = new ArrayList<>(pendingUnsubs.values());
        for (AsyncResult unsubscribeRequest : pending) {
            unsubscribeRequest.onFailure(cause);
        }

        super.handleResourceClosure(provider, cause);
    }

    private static final class DurableSubscriptionReattach extends AmqpAbstractResource<JmsSessionInfo, Receiver> {

        public DurableSubscriptionReattach(JmsSessionInfo resource, Receiver receiver, AmqpResourceParent parent) {
            super(resource, receiver, parent);
        }

        @Override
        public void processRemoteClose(AmqpProvider provider) throws ProviderException {
            // For unsubscribe we care if the remote signaled an error on the close since
            // that would indicate that the unsubscribe did not succeed and we want to throw
            // that from the unsubscribe call.
            if (getEndpoint().getRemoteCondition().getCondition() != null) {
                closeResource(provider, AmqpSupport.convertToNonFatalException(provider, getEndpoint(), getEndpoint().getRemoteCondition()), true);
            } else {
                closeResource(provider, null, true);
            }
        }

        public String getLinkName() {
            return getEndpoint().getName();
        }
    }

    private final class DurableSubscriptionReattachBuilder extends AmqpResourceBuilder<DurableSubscriptionReattach, AmqpSession, JmsSessionInfo, Receiver> {

        private final String linkName;
        private final boolean hasClientID;

        public DurableSubscriptionReattachBuilder(AmqpSession parent, JmsSessionInfo resourceInfo, String linkName) {
            super(parent, resourceInfo);
            this.hasClientID = parent.getConnection().getResourceInfo().isExplicitClientID();
            this.linkName = linkName;
        }

        @Override
        protected Receiver createEndpoint(JmsSessionInfo resourceInfo) {
            Receiver receiver = getParent().getEndpoint().receiver(linkName);
            receiver.setTarget(new Target());
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

            if (!hasClientID) {
              // We are trying to unsubscribe a 'global' shared subs using a 'null source lookup', add link
              // desired capabilities as hints to the peer to consider this when trying to attach the link.
              receiver.setDesiredCapabilities(new Symbol[] { AmqpSupport.SHARED, AmqpSupport.GLOBAL });
            }

            return receiver;
        }

        @Override
        protected DurableSubscriptionReattach createResource(AmqpSession parent, JmsSessionInfo resourceInfo, Receiver endpoint) {
            return new DurableSubscriptionReattach(resourceInfo, endpoint, getProvider());
        }

        @Override
        protected boolean isClosePending() {
            // When no link terminus was created, the peer will now detach/close us otherwise
            // we need to validate the returned remote source prior to open completion.
            return endpoint.getRemoteSource() == null;
        }
    }

    private final class DurableSubscriptionReattachRequest extends WrappedAsyncResult {

        private final String subscriptionName;
        private final DurableSubscriptionReattachBuilder subscriberBuilder;

        public DurableSubscriptionReattachRequest(String subscriptionName, DurableSubscriptionReattachBuilder subscriberBuilder, AsyncResult originalRequest) {
            super(originalRequest);
            this.subscriptionName = subscriptionName;
            this.subscriberBuilder = subscriberBuilder;
        }

        @Override
        public void onSuccess() {
            DurableSubscriptionReattach subscriber = subscriberBuilder.getResource();
            LOG.trace("Reattached to subscription '{}' using link name '{}'", subscriptionName, subscriber.getLinkName());
            pendingUnsubs.remove(subscriptionName);
            if (subscriber.getEndpoint().getRemoteSource() != null) {
                subscriber.close(getWrappedRequest());
            } else {
                subscriber.close(NoOpAsyncResult.INSTANCE);
                getWrappedRequest().onFailure(
                    new ProviderInvalidDestinationException("Cannot remove a subscription that does not exist"));
            }
        }

        @Override
        public void onFailure(ProviderException cause) {
            DurableSubscriptionReattach subscriber = subscriberBuilder.getResource();
            LOG.trace("Failed to reattach to subscription '{}' using link name '{}'", subscriptionName, subscriber.getLinkName());
            pendingUnsubs.remove(subscriptionName);
            subscriber.closeResource(getProvider(), ProviderExceptionSupport.createNonFatalOrPassthrough(cause), false);
            super.onFailure(cause);
        }
    }
}
