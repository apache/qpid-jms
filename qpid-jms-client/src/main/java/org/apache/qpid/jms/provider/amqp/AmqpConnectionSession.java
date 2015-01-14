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

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.NoOpAsyncResult;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.proton.amqp.messaging.Target;
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
        DurableSubscriptionReattach subscriber = new DurableSubscriptionReattach(getJmsResource(), subscriptionName);
        DurableSubscriptionReattachRequest subscribeRequest = new DurableSubscriptionReattachRequest(subscriber, request);
        pendingUnsubs.put(subscriptionName, subscribeRequest);

        LOG.debug("Attempting remove of subscription: {}", subscriptionName);
        subscriber.open(subscribeRequest);
    }

    private class DurableSubscriptionReattach extends AmqpAbstractResource<JmsSessionInfo, Receiver> {
        private final String subscriptionName;

        public DurableSubscriptionReattach(JmsSessionInfo resource, String subscriptionName) {
            super(resource, AmqpConnectionSession.this.getProtonSession().receiver(subscriptionName));
            this.subscriptionName = subscriptionName;
        }

        @Override
        protected void doOpen() {
            Receiver receiver = getEndpoint();
            receiver.setTarget(new Target());
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            super.doOpen();
        }

        public String getSubscriptionName() {
            return subscriptionName;
        }
    }

    private class DurableSubscriptionReattachRequest extends WrappedAsyncResult {

        private final DurableSubscriptionReattach subscriber;

        public DurableSubscriptionReattachRequest(DurableSubscriptionReattach subscriber, AsyncResult originalRequest) {
            super(originalRequest);
            this.subscriber = subscriber;
        }

        @Override
        public void onSuccess() {
            LOG.trace("Reattached to subscription: {}", subscriber.getSubscriptionName());
            pendingUnsubs.remove(subscriber.getSubscriptionName());
            if (subscriber.getEndpoint().getRemoteSource() != null) {
                subscriber.close(getWrappedRequest());
            } else {
                subscriber.close(NoOpAsyncResult.INSTANCE);
                getWrappedRequest().onFailure(new InvalidDestinationException("Cannot remove a subscription that does not exist"));
            }
        }

        @Override
        public void onFailure(Throwable result) {
            LOG.trace("Failed to reattach to subscription: {}", subscriber.getSubscriptionName());
            pendingUnsubs.remove(subscriber.getSubscriptionName());
            subscriber.closed();
            super.onFailure(result);
        }
    }
}
