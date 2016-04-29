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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Producer object that is used to manage JMS MessageProducer semantics.
 *
 * This Producer is fixed to a given JmsDestination and can only produce messages to it.
 */
public class AmqpFixedProducer extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpFixedProducer.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator(true);
    private final Set<Delivery> sent = new LinkedHashSet<Delivery>();
    private final LinkedList<InFlightSend> blocked = new LinkedList<InFlightSend>();
    private byte[] encodeBuffer = new byte[1024 * 8];

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);
    }

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info, Sender sender) {
        super(session, info, sender);
    }

    @Override
    public void close(AsyncResult request) {
        // If any sends are held we need to wait for them to complete.
        if (!blocked.isEmpty()) {
            this.closeRequest = request;
            return;
        }

        super.close(request);
    }

    @Override
    public boolean send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        if (getEndpoint().getCredit() <= 0) {
            LOG.trace("Holding Message send until credit is available.");
            // Once a message goes into a held mode we no longer can send it async, so
            // we clear the async flag if set to avoid the sender never getting notified.
            envelope.setSendAsync(false);

            InFlightSend send = new InFlightSend(envelope, request);

            if (getSendTimeout() > JmsConnectionInfo.INFINITE) {
                send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(
                    send, getSendTimeout(), new JmsSendTimedOutException("Timed out waiting for credit to send Message", envelope.getMessage()));
            }

            blocked.addLast(send);
            return false;
        } else {
            doSend(envelope, request);
            return true;
        }
    }

    private void doSend(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        // If the transaction has failed due to remote termination etc then we just indicate
        // the send has succeeded until the a new transaction is started.
        if (session.isTransacted() && session.isTransactionFailed()) {
            request.onSuccess();
            return;
        }

        LOG.trace("Producer sending message: {}", envelope);

        JmsMessageFacade facade = envelope.getMessage().getFacade();
        boolean presettle = envelope.isPresettle() || isPresettle();
        Delivery delivery = null;

        if (presettle) {
            delivery = getEndpoint().delivery(EMPTY_BYTE_ARRAY, 0, 0);
        } else {
            byte[] tag = tagGenerator.getNextTag();
            delivery = getEndpoint().delivery(tag, 0, tag.length);
        }

        delivery.setContext(request);

        if (session.isTransacted()) {
            Binary amqpTxId = session.getTransactionContext().getAmqpTransactionId();
            TransactionalState state = new TransactionalState();
            state.setTxnId(amqpTxId);
            delivery.disposition(state);
        }

        AmqpJmsMessageFacade amqpMessageFacade = (AmqpJmsMessageFacade) facade;
        encodeAndSend(amqpMessageFacade.getAmqpMessage(), delivery);

        if (presettle) {
            delivery.settle();
        } else {
            sent.add(delivery);
            getEndpoint().advance();
        }

        if (envelope.isSendAsync() || presettle) {
            request.onSuccess();
        } else if (getSendTimeout() != JmsConnectionInfo.INFINITE) {
            InFlightSend send = new InFlightSend(envelope, request);

            send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(
                send, getSendTimeout(), new JmsSendTimedOutException("Timed out waiting for disposition of sent Message", envelope.getMessage()));

            // Update context so the incoming disposition can cancel any pending timeout
            delivery.setContext(send);
        }
    }

    private void encodeAndSend(Message message, Delivery delivery) throws IOException {

        int encodedSize;
        while (true) {
            try {
                encodedSize = message.encode(encodeBuffer, 0, encodeBuffer.length);
                break;
            } catch (java.nio.BufferOverflowException e) {
                encodeBuffer = new byte[encodeBuffer.length * 2];
            }
        }

        int sentSoFar = 0;

        while (true) {
            int sent = getEndpoint().send(encodeBuffer, sentSoFar, encodedSize - sentSoFar);
            if (sent > 0) {
                sentSoFar += sent;
                if ((encodedSize - sentSoFar) == 0) {
                    break;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws IOException {
        if (!blocked.isEmpty() && getEndpoint().getCredit() > 0) {
            while (getEndpoint().getCredit() > 0 && !blocked.isEmpty()) {
                LOG.trace("Dispatching previously held send");
                InFlightSend held = blocked.pop();
                try {
                    doSend(held.envelope, held);
                } catch (JMSException e) {
                    throw IOExceptionSupport.create(e);
                }
            }
        }

        // If a drain was requested, we just sent what we had so respond with drained
        if (getEndpoint().getDrain()) {
            getEndpoint().drained();
        }

        // Once the pending sends queue is drained we can propagate the close request.
        if (blocked.isEmpty() && isAwaitingClose() && !isClosed()) {
            super.close(closeRequest);
        }

        super.processFlowUpdates(provider);
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        List<Delivery> toRemove = new ArrayList<Delivery>();

        for (Delivery delivery : sent) {
            DeliveryState state = delivery.getRemoteState();
            if (state == null) {
                continue;
            }

            Outcome outcome = null;
            if (state instanceof TransactionalState) {
                LOG.trace("State of delivery is Transactional, retrieving outcome: {}", state);
                outcome = ((TransactionalState) state).getOutcome();
            } else if (state instanceof Outcome) {
                outcome = (Outcome) state;
            } else {
                LOG.warn("Message send updated with unsupported state: {}", state);
                outcome = null;
            }

            AsyncResult request = (AsyncResult) delivery.getContext();
            Exception deliveryError = null;

            if (outcome instanceof Accepted) {
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                if (request != null && !request.isComplete()) {
                    request.onSuccess();
                }
            } else if (outcome instanceof Rejected) {
                LOG.trace("Outcome of delivery was rejected: {}", delivery);
                ErrorCondition remoteError = ((Rejected) outcome).getError();
                if (remoteError == null) {
                    remoteError = getEndpoint().getRemoteCondition();
                }

                deliveryError = AmqpSupport.convertToException(getEndpoint(), remoteError);
            } else if (outcome instanceof Released) {
                LOG.trace("Outcome of delivery was released: {}", delivery);
                deliveryError = new JMSException("Delivery failed: released by receiver");
            } else if (outcome instanceof Modified) {
                LOG.trace("Outcome of delivery was modified: {}", delivery);
                deliveryError = new JMSException("Delivery failed: failure at remote");
            }

            if (deliveryError != null) {
                if (request != null && !request.isComplete()) {
                    request.onFailure(deliveryError);
                } else {
                    connection.getProvider().fireNonFatalProviderException(deliveryError);
                }
            }

            tagGenerator.returnTag(delivery.getTag());
            toRemove.add(delivery);
            delivery.settle();
        }

        sent.removeAll(toRemove);

        super.processDeliveryUpdates(provider);
    }

    public AmqpSession getSession() {
        return session;
    }

    @Override
    public boolean isAnonymous() {
        return getResourceInfo().getDestination() == null;
    }

    @Override
    public boolean isPresettle() {
        return getEndpoint().getSenderSettleMode() == SenderSettleMode.SETTLED;
    }

    public long getSendTimeout() {
        return getParent().getProvider().getSendTimeout();
    }

    @Override
    public String toString() {
        return "AmqpFixedProducer { " + getProducerId() + " }";
    }

    @Override
    public void remotelyClosed(AmqpProvider provider) {
        super.remotelyClosed(provider);

        Exception ex = null;
        if (!sent.isEmpty()) {
            // TODO: create/use a more specific/appropriate exception type?
            ex = new JMSException("Producer closed remotely before message transfer result was notified");
        }

        ex = AmqpSupport.convertToException(getEndpoint(), getEndpoint().getRemoteCondition(), ex);

        for (Delivery delivery : sent) {
            try {
                AsyncResult request = (AsyncResult) delivery.getContext();

                if (request != null && !request.isComplete()) {
                    request.onFailure(ex);
                }

                delivery.settle();
                tagGenerator.returnTag(delivery.getTag());
            } catch (Exception e) {
                LOG.debug("Caught exception when failing pending send during remote producer closure: {}", delivery, e);
            }
        }

        sent.clear();
    }

    //----- Class used to manage held sends ----------------------------------//

    private class InFlightSend implements AsyncResult {

        public final JmsOutboundMessageDispatch envelope;
        public final AsyncResult request;

        public ScheduledFuture<?> requestTimeout;

        public InFlightSend(JmsOutboundMessageDispatch envelope, AsyncResult request) {
            this.envelope = envelope;
            this.request = request;
        }

        @Override
        public void onFailure(Throwable cause) {
            if (requestTimeout != null) {
                requestTimeout.cancel(false);
                requestTimeout = null;
            }

            blocked.remove(this);

            request.onFailure(cause);
        }

        @Override
        public void onSuccess() {
            if (requestTimeout != null) {
                requestTimeout.cancel(false);
                requestTimeout = null;
            }

            blocked.remove(this);

            request.onSuccess();
        }

        @Override
        public boolean isComplete() {
            return request.isComplete();
        }
    }
}
