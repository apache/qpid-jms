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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
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
        SubscriptionSourceRequestor requestor = new SubscriptionSourceRequestor(getJmsResource(), subscriptionName);
        SubscriptionSourceRequest sourceRequest = new SubscriptionSourceRequest(requestor, request);
        pendingUnsubs.put(subscriptionName, sourceRequest);

        LOG.debug("Attempting remove of subscription: {}", subscriptionName);
        requestor.open(sourceRequest);
    }

    private class SubscriptionSourceRequestor extends AmqpAbstractResource<JmsSessionInfo, Receiver> {

        private final String subscriptionName;

        public SubscriptionSourceRequestor(JmsSessionInfo resource, String subscriptionName) {
            super(resource);
            this.subscriptionName = subscriptionName;
        }

        @Override
        protected void doOpen() {
            endpoint = AmqpConnectionSession.this.getProtonSession().receiver(subscriptionName);
            endpoint.setTarget(new Target());
            endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        }

        @Override
        protected void doClose() {
        }

        public String getSubscriptionName() {
            return subscriptionName;
        }
    }

    private class SubscriptionSourceRequest extends WrappedAsyncResult {

        private final SubscriptionSourceRequestor requestor;

        public SubscriptionSourceRequest(SubscriptionSourceRequestor requestor, AsyncResult originalRequest) {
            super(originalRequest);
            this.requestor = requestor;
        }

        @Override
        public void onSuccess() {
            final Source returnedSource = (Source) requestor.getEndpoint().getRemoteSource();
            if (returnedSource == null) {
                LOG.trace("No Source returned for subscription: {}", requestor.getSubscriptionName());
                pendingUnsubs.remove(requestor.getSubscriptionName());
                super.onFailure(new IOException("Could not fetch remote subscription information"));
            } else {
                LOG.trace("Source returned for subscription: {} closing first stage", requestor.getSubscriptionName());
                requestor.close(new AsyncResult() {

                    @Override
                    public void onSuccess() {
                        RemoveDurabilityRequestor removeRequestor =
                            new RemoveDurabilityRequestor(getJmsResource(), requestor.getSubscriptionName(), returnedSource);
                        RemoveDurabilityRequest removeRequest = new RemoveDurabilityRequest(removeRequestor, getWrappedRequest());
                        pendingUnsubs.put(requestor.getSubscriptionName(), removeRequest);
                        LOG.trace("Second stage remove started for subscription: {}", requestor.getSubscriptionName());
                        removeRequestor.open(removeRequest);
                    }

                    @Override
                    public void onFailure(Throwable result) {
                        LOG.trace("Second stage remove failed for subscription: {}", requestor.getSubscriptionName());
                        pendingUnsubs.remove(requestor.getSubscriptionName());
                        getWrappedRequest().onFailure(result);
                    }

                    @Override
                    public boolean isComplete() {
                        return getWrappedRequest().isComplete();
                    }
                });
            }
        }

        @Override
        public void onFailure(Throwable result) {
            pendingUnsubs.remove(requestor.getSubscriptionName());
            super.onFailure(result);
        }
    }

    private class RemoveDurabilityRequestor extends AmqpAbstractResource<JmsSessionInfo, Receiver> {

        private final String subscriptionName;
        private final Source subscriptionSource;

        public RemoveDurabilityRequestor(JmsSessionInfo resource, String subscriptionName, Source subscriptionSource) {
            super(resource);
            this.subscriptionSource = subscriptionSource;
            this.subscriptionName = subscriptionName;
        }

        @Override
        protected void doOpen() {
            endpoint = AmqpConnectionSession.this.getProtonSession().receiver(subscriptionName);

            subscriptionSource.setDurable(TerminusDurability.NONE);
            subscriptionSource.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

            endpoint.setSource(subscriptionSource);
            endpoint.setTarget(new Target());
            endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        }

        @Override
        public void remotelyClosed() {
            if (isAwaitingOpen()) {
                openRequest.onSuccess();
            } else {
                AmqpConnectionSession.this.reportError(new IOException("Durable unsubscribe failed unexpectedly"));
            }
        }

        @Override
        protected void doClose() {
        }

        public String getSubscriptionName() {
            return subscriptionName;
        }
    }

    private class RemoveDurabilityRequest extends WrappedAsyncResult {

        private final RemoveDurabilityRequestor requestor;

        public RemoveDurabilityRequest(RemoveDurabilityRequestor requestor, AsyncResult originalRequest) {
            super(originalRequest);
            this.requestor = requestor;
        }

        @Override
        public void onSuccess() {
            LOG.trace("Second stage remove complete for subscription: {}", requestor.getSubscriptionName());
            pendingUnsubs.remove(requestor.getSubscriptionName());
            requestor.close(getWrappedRequest());
        }

        @Override
        public void onFailure(Throwable result) {
            LOG.trace("Second stage remove failed for subscription: {}", requestor.getSubscriptionName());
            pendingUnsubs.remove(requestor.getSubscriptionName());
            super.onFailure(result);
        }
    }
}
