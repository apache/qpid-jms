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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.message.AmqpReadableBuffer;
import org.apache.qpid.jms.provider.exceptions.ProviderDeliveryModifiedException;
import org.apache.qpid.jms.provider.exceptions.ProviderDeliveryReleasedException;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.provider.exceptions.ProviderSendTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderUnsupportedOperationException;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

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

    private final AmqpConnection connection;

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info, Sender sender) {
        super(session, info, sender);

        connection = session.getConnection();
        delayedDeliverySupported = connection.getProperties().isDelayedDeliverySupported();
    }

    @Override
    public void close(AsyncResult request) {
        // If any sends are held we need to wait for them to complete.
        if (!blocked.isEmpty() || !sent.isEmpty()) {
            this.closeRequest = request;
            return;
        }

        super.close(request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        if (isClosed()) {
            request.onFailure(new ProviderIllegalStateException("The MessageProducer is closed"));
        }

        if (!delayedDeliverySupported && envelope.getMessage().getFacade().isDeliveryTimeTransmitted()) {
            // Don't allow sends with delay if the remote has not said it can handle them
            request.onFailure(new ProviderUnsupportedOperationException("Remote does not support delayed message delivery"));
        } else if (getEndpoint().getCredit() <= 0) {
            LOG.trace("Holding Message send until credit is available.");

            InFlightSend send = new InFlightSend(envelope, request);

            if (getSendTimeout() > JmsConnectionInfo.INFINITE) {
                send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
            }

            blocked.put(envelope.getMessageId(), send);
            getParent().getProvider().pumpToProtonTransport(request);
        } else {
            // If the transaction has failed due to remote termination etc then we just indicate
            // the send has succeeded until the a new transaction is started.
            if (session.isTransacted() && session.isTransactionFailed()) {
                request.onSuccess();
                return;
            }

            doSend(envelope, new InFlightSend(envelope, request));
        }
    }

    private void doSend(JmsOutboundMessageDispatch envelope, InFlightSend send) throws ProviderException {
        LOG.trace("Producer sending message: {}", envelope);

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
            delivery.disposition(context.getTxnEnrolledState());
            context.registerTxProducer(this);
        }

        // Write the already encoded AMQP message into the Sender
        ByteBuf encoded = (ByteBuf) envelope.getPayload();
        getEndpoint().sendNoCopy(new AmqpReadableBuffer(encoded.duplicate()));

        AmqpProvider provider = getParent().getProvider();

        if (!presettle && getSendTimeout() != JmsConnectionInfo.INFINITE && send.requestTimeout == null) {
            send.requestTimeout = getParent().getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
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
        if (provider.pumpToProtonTransport(send, false)) {
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

            try {
                provider.getTransport().flush();
            } catch (Throwable ex) {
                throw ProviderExceptionSupport.createOrPassthroughFatal(ex);
            }
        }
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws ProviderException {
        if (!blocked.isEmpty() && getEndpoint().getCredit() > 0) {
            Iterator<InFlightSend> blockedSends = blocked.values().iterator();
            while (getEndpoint().getCredit() > 0 && blockedSends.hasNext()) {
                LOG.trace("Dispatching previously held send");
                InFlightSend held = blockedSends.next();
                try {
                    // If the transaction has failed due to remote termination etc then we just indicate
                    // the send has succeeded until the a new transaction is started.
                    if (session.isTransacted() && session.isTransactionFailed()) {
                        held.onSuccess();
                        return;
                    }

                    doSend(held.getEnvelope(), held);
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
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {
        DeliveryState state = delivery.getRemoteState();
        if (state != null) {
            InFlightSend send = (InFlightSend) delivery.getContext();

            if (state.getType() == DeliveryStateType.Accepted) {
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                send.onSuccess();
            } else {
                applyDeliveryStateUpdate(send, delivery, state);
            }
        }

        super.processDeliveryUpdates(provider, delivery);
    }

    private void applyDeliveryStateUpdate(InFlightSend send, Delivery delivery, DeliveryState state) {
        ProviderException deliveryError = null;
        if (state == null) {
            return;
        }

        switch (state.getType()) {
            case Transactional:
                LOG.trace("State of delivery is Transactional, retrieving outcome: {}", state);
                applyDeliveryStateUpdate(send, delivery, (DeliveryState) ((TransactionalState) state).getOutcome());
                break;
            case Accepted:
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                send.onSuccess();
                break;
            case Rejected:
                LOG.trace("Outcome of delivery was rejected: {}", delivery);
                ErrorCondition remoteError = ((Rejected) state).getError();
                if (remoteError == null) {
                    remoteError = getEndpoint().getRemoteCondition();
                }

                deliveryError = AmqpSupport.convertToNonFatalException(getParent().getProvider(), getEndpoint(), remoteError);
                break;
            case Released:
                LOG.trace("Outcome of delivery was released: {}", delivery);
                deliveryError = new ProviderDeliveryReleasedException("Delivery failed: released by receiver");
                break;
            case Modified:
                LOG.trace("Outcome of delivery was modified: {}", delivery);
                Modified modified = (Modified) state;
                deliveryError = new ProviderDeliveryModifiedException("Delivery failed: failure at remote", modified);
                break;
            default:
                LOG.warn("Message send updated with unsupported state: {}", state);
        }

        if (deliveryError != null) {
            send.onFailure(deliveryError);
        }
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
    public void handleResourceClosure(AmqpProvider provider, ProviderException error) {
        if (error == null) {
            // TODO: create/use a more specific/appropriate exception type?
            error = new ProviderException("Producer closed remotely before message transfer result was notified");
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
        public void onFailure(ProviderException cause) {
            handleSendCompletion(false);

            if (request.isComplete()) {
                // Asynchronous sends can still be awaiting a completion in which case we
                // send to them otherwise send to the listener to be reported.
                if (envelope.isCompletionRequired()) {
                    getParent().getProvider().getProviderListener().onFailedMessageSend(envelope, ProviderExceptionSupport.createNonFatalOrPassthrough(cause));
                } else {
                    getParent().getProvider().fireNonFatalProviderException(ProviderExceptionSupport.createNonFatalOrPassthrough(cause));
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

            // Put the message back to usable state following send complete
            envelope.getMessage().onSendComplete();

            // Once the pending sends queue is drained and all in-flight sends have been
            // settled we can propagate the close request.
            if (isAwaitingClose() && !isClosed() && blocked.isEmpty() && sent.isEmpty()) {
                AmqpFixedProducer.super.close(closeRequest);
            }
        }

        @Override
        public ProviderException createException() {
            if (delivery == null) {
                return new ProviderSendTimedOutException("Timed out waiting for credit to send Message", envelope.getMessage());
            } else {
                return new ProviderSendTimedOutException("Timed out waiting for disposition of sent Message", envelope.getMessage());
            }
        }
    }
}
