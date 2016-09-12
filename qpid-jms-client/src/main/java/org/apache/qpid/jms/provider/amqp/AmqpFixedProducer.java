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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import javax.jms.IllegalStateException;
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
    private final Map<Object, InFlightSend> sent = new LinkedHashMap<Object, InFlightSend>();
    private final Map<Object, InFlightSend> blocked = new LinkedHashMap<Object, InFlightSend>();
    private byte[] encodeBuffer = new byte[1024 * 8];

    private AsyncResult sendCompletionWatcher;

    private final AmqpConnection connection;

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info) {
        this(session, info, null);
    }

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info, Sender sender) {
        super(session, info, sender);

        connection = session.getConnection();
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
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        if (isClosed()) {
            request.onFailure(new IllegalStateException("The MessageProducer is closed"));
        }

        if (!connection.getProperties().isDelayedDeliverySupported() &&
            envelope.getMessage().getJMSDeliveryTime() != 0) {

            // Don't allow sends with delay if the remote said it can't handle them
            request.onFailure(new JMSException("Remote does not support delayed message delivery"));
        } else if (getEndpoint().getCredit() <= 0) {
            LOG.trace("Holding Message send until credit is available.");

            InFlightSend send = new InFlightSend(envelope, request);

            if (getSendTimeout() > JmsConnectionInfo.INFINITE) {
                send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
            }

            blocked.put(envelope.getMessageId(), send);
            getParent().getProvider().pumpToProtonTransport(request);
        } else {
            doSend(envelope, request);
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

        if (session.isTransacted()) {
            AmqpTransactionContext context = session.getTransactionContext();
            Binary amqpTxId = context.getAmqpTransactionId();
            TransactionalState state = new TransactionalState();
            state.setTxnId(amqpTxId);
            delivery.disposition(state);
            context.registerTxProducer(this);
        }

        AmqpJmsMessageFacade amqpMessageFacade = (AmqpJmsMessageFacade) facade;
        encodeAndSend(amqpMessageFacade.getAmqpMessage(), delivery);

        AmqpProvider provider = getParent().getProvider();

        InFlightSend send = null;
        if (request instanceof InFlightSend) {
            send = (InFlightSend) request;
        } else {
            send = new InFlightSend(envelope, request);

            if (!presettle && getSendTimeout() != JmsConnectionInfo.INFINITE) {
                send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
            }
        }

        if (presettle) {
            delivery.settle();
        } else {
            sent.put(envelope.getMessageId(), send);
            getEndpoint().advance();
        }

        send.setDelivery(delivery);
        delivery.setContext(send);

        // Put it on the wire and let it fail if the connection is broken, if it does
        // get written then continue on to determine when we should complete it.
        if (provider.pumpToProtonTransport(request)) {
            // For presettled messages we can just mark as successful and we are done, but
            // for any other message we still track it until the remote settles.  If the send
            // was tagged as asynchronous we must mark the original request as complete but
            // we still need to wait for the disposition before we can consider the send as
            // having been successful.
            if (presettle) {
                send.onSuccess();
            } else if (envelope.isSendAsync()) {
                send.getOriginalRequest().onSuccess();
            }
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
            Iterator<InFlightSend> blockedSends = blocked.values().iterator();
            while (getEndpoint().getCredit() > 0 && blockedSends.hasNext()) {
                LOG.trace("Dispatching previously held send");
                InFlightSend held = blockedSends.next();
                try {
                    doSend(held.getEnvelope(), held);
                } catch (JMSException e) {
                    throw IOExceptionSupport.create(e);
                } finally {
                    blockedSends.remove();
                }
            }
        }

        // If a drain was requested, we just sent what we had so respond with drained
        if (getEndpoint().getDrain()) {
            getEndpoint().drained();
        }

        super.processFlowUpdates(provider);
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws IOException {
        DeliveryState state = delivery.getRemoteState();
        if (state != null) {

            InFlightSend send = (InFlightSend) delivery.getContext();
            Exception deliveryError = null;
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

            if (outcome instanceof Accepted) {
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                send.onSuccess();
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
                send.onFailure(deliveryError);
            }
        }

        super.processDeliveryUpdates(provider, delivery);
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
    public void handleResourceClosure(AmqpProvider provider, Exception error) {
        if (error == null) {
            // TODO: create/use a more specific/appropriate exception type?
            error = new JMSException("Producer closed remotely before message transfer result was notified");
        }

        Collection<InFlightSend> inflightSends = new ArrayList<InFlightSend>(sent.values());
        for (InFlightSend send : inflightSends) {
            try {
                send.onFailure(error);
            } catch (Exception e) {
                LOG.debug("Caught exception when failing pending send during remote producer closure: {}", send, e);
            }
        }

        Collection<InFlightSend> blockedSends = new ArrayList<InFlightSend>(blocked.values());
        for (InFlightSend send : blockedSends) {
            try {
                send.onFailure(error);
            } catch (Exception e) {
                LOG.debug("Caught exception when failing blocked send during remote producer closure: {}", send, e);
            }
        }
    }

    @Override
    public void addSendCompletionWatcher(AsyncResult watcher) {
        // If none pending signal done already.
        // TODO - If we don't include blocked sends then update this.
        if (blocked.isEmpty() && sent.isEmpty()) {
            watcher.onSuccess();
        } else {
            this.sendCompletionWatcher = watcher;
        }
    }

    //----- Class used to manage held sends ----------------------------------//

    private class InFlightSend implements AsyncResult, AmqpExceptionBuilder {

        private final JmsOutboundMessageDispatch envelope;
        private final AsyncResult request;

        private Delivery delivery;
        private ScheduledFuture<?> requestTimeout;

        public InFlightSend(JmsOutboundMessageDispatch envelope, AsyncResult request) {
            this.envelope = envelope;
            this.request = request;
        }

        @Override
        public void onFailure(Throwable cause) {
            handleSendCompletion(false);

            if (request.isComplete()) {
                // Asynchronous sends can still be awaiting a completion in which case we
                // send to them otherwise send to the listener to be reported.
                if (envelope.isCompletionRequired()) {
                    getParent().getProvider().getProviderListener().onFailedMessageSend(envelope, cause);
                } else {
                    getParent().getProvider().fireNonFatalProviderException(IOExceptionSupport.create(cause));
                }
            } else {
                request.onFailure(cause);
            }
        }

        @Override
        public void onSuccess() {
            handleSendCompletion(true);

            if (!request.isComplete()) {
                request.onSuccess();
            }

            if (envelope.isCompletionRequired()) {
                getParent().getProvider().getProviderListener().onCompletedMessageSend(envelope);
            }
        }

        public void setRequestTimeout(ScheduledFuture<?> requestTimeout) {
            if (this.requestTimeout != null) {
                this.requestTimeout.cancel(false);
            }

            this.requestTimeout = requestTimeout;
        }

        public JmsOutboundMessageDispatch getEnvelope() {
            return envelope;
        }

        public AsyncResult getOriginalRequest() {
            return request;
        }

        public void setDelivery(Delivery delivery) {
            this.delivery = delivery;
        }

        public Delivery getDelivery() {
            return delivery;
        }

        @Override
        public boolean isComplete() {
            return request.isComplete();
        }

        private void handleSendCompletion(boolean successful) {
            setRequestTimeout(null);

            if (getDelivery() != null) {
                sent.remove(envelope.getMessageId());
                delivery.settle();
                tagGenerator.returnTag(delivery.getTag());
            } else {
                blocked.remove(envelope.getMessageId());
            }

            // TODO - Should this take blocked sends into consideration.
            // Signal the watcher that all pending sends have completed if one is registered
            // and both the in-flight sends and blocked sends have completed.
            if (sendCompletionWatcher != null && sent.isEmpty() && blocked.isEmpty()) {
                sendCompletionWatcher.onSuccess();
            }

            // Once the pending sends queue is drained and all in-flight sends have been
            // settled we can propagate the close request.
            if (isAwaitingClose() && !isClosed() && blocked.isEmpty() && sent.isEmpty()) {
                AmqpFixedProducer.super.close(closeRequest);
            }
        }

        @Override
        public Exception createException() {
            if (delivery == null) {
                return new JmsSendTimedOutException("Timed out waiting for credit to send Message", envelope.getMessage());
            } else {
                return new JmsSendTimedOutException("Timed out waiting for disposition of sent Message", envelope.getMessage());
            }
        }
    }
}
