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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.MODIFIED_FAILED;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.MODIFIED_FAILED_UNDELIVERABLE;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.REJECTED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageBuilder;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer extends AmqpAbstractResource<JmsConsumerInfo, Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    private static final int INITIAL_BUFFER_CAPACITY = 1024 * 128;

    protected final AmqpSession session;
    protected final Map<JmsInboundMessageDispatch, Delivery> delivered = new LinkedHashMap<JmsInboundMessageDispatch, Delivery>();
    protected boolean presettle;
    protected AsyncResult stopRequest;
    protected AsyncResult pullRequest;
    protected final ByteBuf incomingBuffer = Unpooled.buffer(INITIAL_BUFFER_CAPACITY);
    protected final AtomicLong incomingSequence = new AtomicLong(0);

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info, Receiver receiver) {
        super(info, receiver, session);

        this.session = session;
    }

    /**
     * Starts the consumer by setting the link credit to the given prefetch value.
     *
     * @param request
     *      The request that awaits completion of the consumer start.
     */
    public void start(AsyncResult request) {
        sendFlowIfNeeded();
        request.onSuccess();
    }

    /**
     * Stops the consumer, using all link credit and waiting for in-flight messages to arrive.
     *
     * @param request
     *      The request that awaits completion of the consumer stop.
     */
    public void stop(AsyncResult request) {
        Receiver receiver = getEndpoint();
        if (receiver.getRemoteCredit() <= 0) {
            if (receiver.getQueued() == 0) {
                // We have no remote credit and all the deliveries have been processed.
                request.onSuccess();
            } else {
                // There are still deliveries to process, wait for them to be.
                stopRequest = request;
            }
        } else {
            // TODO: We don't actually want the additional messages that could be sent while
            // draining. We could explicitly reduce credit first, or possibly use 'echo' instead
            // of drain if it was supported. We would first need to understand what happens
            // if we reduce credit below the number of messages already in-flight before
            // the peer sees the update.
            stopRequest = request;
            receiver.drain(0);

            if (getDrainTimeout() > 0) {
                // If the remote doesn't respond we will close the consumer and break any
                // blocked receive or stop calls that are waiting.
                final ScheduledFuture<?> future = getSession().schedule(new Runnable() {
                    @Override
                    public void run() {
                        LOG.trace("Consumer {} drain request timed out", getConsumerId());
                        Exception cause = new JmsOperationTimedOutException("Remote did not respond to a drain request in time");
                        locallyClosed(session.getProvider(), cause);
                        stopRequest.onFailure(cause);
                        session.getProvider().pumpToProtonTransport(stopRequest);
                    }
                }, getDrainTimeout());

                stopRequest = new ScheduledRequest(future, stopRequest);
            }
        }
    }

    private void stopOnSchedule(long timeout, final AsyncResult request) {
        LOG.trace("Consumer {} scheduling stop", getConsumerId());
        // We need to drain the credit if no message(s) arrive to use it.
        final ScheduledFuture<?> future = getSession().schedule(new Runnable() {
            @Override
            public void run() {
                LOG.trace("Consumer {} running scheduled stop", getConsumerId());
                if (getEndpoint().getRemoteCredit() != 0) {
                    stop(request);
                    session.getProvider().pumpToProtonTransport(request);
                }
            }
        }, timeout);

        stopRequest = new ScheduledRequest(future, request);
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws IOException {
        // Check if we tried to stop and have now run out of credit, and
        // processed all locally queued messages
        if (stopRequest != null) {
            Receiver receiver = getEndpoint();
            if (receiver.getRemoteCredit() <= 0 && receiver.getQueued() == 0) {
                stopRequest.onSuccess();
                stopRequest = null;
            }
        }

        if (pullRequest != null) {
            Receiver receiver = getEndpoint();
            if (receiver.getRemoteCredit() <= 0 && receiver.getQueued() == 0) {
                pullRequest.onSuccess();
                pullRequest = null;
            }
        }

        LOG.trace("Consumer {} flow updated, remote credit = {}", getConsumerId(), getEndpoint().getRemoteCredit());

        super.processFlowUpdates(provider);
    }

    /**
     * Called to acknowledge all messages that have been marked as delivered but
     * have not yet been marked consumed.  Usually this is called as part of an
     * client acknowledge session operation.
     *
     * Only messages that have already been acknowledged as delivered by the JMS
     * framework will be in the delivered Map.  This means that the link credit
     * would already have been given for these so we just need to settle them.
     *
     * @param ackType the type of acknowledgement to perform
     */
    public void acknowledge(ACK_TYPE ackType) {
        LOG.trace("Session Acknowledge for consumer {} with ack type {}", getResourceInfo().getId(), ackType);
        for (Delivery delivery : delivered.values()) {
            switch (ackType) {
                case ACCEPTED:
                    delivery.disposition(Accepted.getInstance());
                    break;
                case RELEASED:
                    delivery.disposition(Released.getInstance());
                    break;
                case REJECTED:
                    delivery.disposition(REJECTED);
                    break;
                case MODIFIED_FAILED:
                    delivery.disposition(MODIFIED_FAILED);
                    break;
                case MODIFIED_FAILED_UNDELIVERABLE:
                    delivery.disposition(MODIFIED_FAILED_UNDELIVERABLE);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid acknowledgement type specified: " + ackType);
            }

            delivery.settle();
        }

        delivered.clear();
    }

    /**
     * Called to acknowledge a given delivery.  Depending on the Ack Mode that
     * the consumer was created with this method can acknowledge more than just
     * the target delivery.
     *
     * @param envelope
     *        the delivery that is to be acknowledged.
     * @param ackType
     *        the type of acknowledgment to perform.
     *
     * @throws JMSException if an error occurs accessing the Message properties.
     */
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        Delivery delivery = null;

        if (envelope.getProviderHint() instanceof Delivery) {
            delivery = (Delivery) envelope.getProviderHint();
        } else {
            delivery = delivered.get(envelope);
            if (delivery == null) {
                LOG.warn("Received Ack for unknown message: {}", envelope);
                return;
            }
        }

        if (ackType.equals(ACK_TYPE.DELIVERED)) {
            LOG.debug("Delivered Ack of message: {}", envelope);
            if (!delivery.isSettled()) {
                delivered.put(envelope, delivery);
                delivery.setDefaultDeliveryState(MODIFIED_FAILED);
            }
            sendFlowIfNeeded();
        } else if (ackType.equals(ACK_TYPE.ACCEPTED)) {
            // A Consumer may not always send a DELIVERED ack so we need to
            // check to ensure we don't add too much credit to the link.
            if (delivery.isSettled() || delivered.remove(envelope) == null) {
                sendFlowIfNeeded();
            }
            LOG.debug("Accepted Ack of message: {}", envelope);
            if (!delivery.isSettled()) {
                if (session.isTransacted() && !getResourceInfo().isBrowser()) {

                    if (session.isTransactionFailed()) {
                        LOG.trace("Skipping ack of message {} in failed transaction.", envelope);
                        return;
                    }

                    Binary txnId = session.getTransactionContext().getAmqpTransactionId();
                    if (txnId != null) {
                        TransactionalState txState = new TransactionalState();
                        txState.setOutcome(Accepted.getInstance());
                        txState.setTxnId(txnId);
                        delivery.disposition(txState);
                        delivery.settle();
                        session.getTransactionContext().registerTxConsumer(this);
                    }
                } else {
                    delivery.disposition(Accepted.getInstance());
                    delivery.settle();
                }
            }
        } else if (ackType.equals(ACK_TYPE.MODIFIED_FAILED_UNDELIVERABLE)) {
            deliveryFailedUndeliverable(delivery);
        } else if (ackType.equals(ACK_TYPE.EXPIRED)) {
            deliveryFailedUndeliverable(delivery);
        } else if (ackType.equals(ACK_TYPE.RELEASED)) {
            delivery.disposition(Released.getInstance());
            delivery.settle();
        } else {
            LOG.warn("Unsupported Ack Type for message: {}", envelope);
        }
    }

    /**
     * We only send more credits as the credit window dwindles to a certain point and
     * then we open the window back up to full prefetch size.  If this is a pull consumer
     * or we are stopping then we never send credit here.
     */
    private void sendFlowIfNeeded() {
        if (getResourceInfo().getPrefetchSize() == 0 || isStopping()) {
            // TODO: isStopping isn't effective when this method is called following
            // processing the last of any messages received while stopping, since that
            // happens just after we stopped. That may be ok in some situations however, and
            // if will only happen if prefetchSize != 0.
            return;
        }

        int currentCredit = getEndpoint().getCredit();
        if (currentCredit <= getResourceInfo().getPrefetchSize() * 0.3) {
            int newCredit = getResourceInfo().getPrefetchSize() - currentCredit;
            LOG.trace("Consumer {} granting additional credit: {}", getConsumerId(), newCredit);
            getEndpoint().flow(newCredit);
        }
    }

    /**
     * Recovers all previously delivered but not acknowledged messages.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        LOG.debug("Session Recover for consumer: {}", getResourceInfo().getId());
        Collection<JmsInboundMessageDispatch> values = delivered.keySet();
        ArrayList<JmsInboundMessageDispatch> envelopes = new ArrayList<JmsInboundMessageDispatch>(values);
        ListIterator<JmsInboundMessageDispatch> reverseIterator = envelopes.listIterator(values.size());

        while (reverseIterator.hasPrevious()) {
            JmsInboundMessageDispatch envelope = reverseIterator.previous();
            envelope.getMessage().getFacade().setRedeliveryCount(
                envelope.getMessage().getFacade().getRedeliveryCount() + 1);
            envelope.setEnqueueFirst(true);
            deliver(envelope);
        }

        delivered.clear();
    }

    /**
     * Request a remote peer send a Message to this client.
     *
     *   {@literal timeout < 0} then it should remain open until a message is received.
     *   {@literal timeout = 0} then it returns a message or null if none available
     *   {@literal timeout > 0} then it should remain open for timeout amount of time.
     *
     * The timeout value when positive is given in milliseconds.
     *
     * @param timeout
     *        the amount of time to tell the remote peer to keep this pull request valid.
     * @param request
     *        the asynchronous request object waiting to be notified of the pull having completed.
     */
    public void pull(final long timeout, final AsyncResult request) {
        LOG.trace("Pull on consumer {} with timeout = {}", getConsumerId(), timeout);
        if (timeout < 0) {
            // Wait until message arrives. Just give credit if needed.
            if (getEndpoint().getCredit() == 0) {
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().flow(1);
            }

            // Await the message arrival
            pullRequest = request;
        } else if (timeout == 0) {
            // If we have no credit then we need to issue some so that we can
            // try to fulfill the request, then drain down what is there to
            // ensure we consume what is available and remove all credit.
            if (getEndpoint().getCredit() == 0){
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().flow(1);
            }

            // Drain immediately and wait for the message(s) to arrive,
            // or a flow indicating removal of the remaining credit.
            stop(request);
        } else if (timeout > 0) {
            // If we have no credit then we need to issue some so that we can
            // try to fulfill the request, then drain down what is there to
            // ensure we consume what is available and remove all credit.
            if (getEndpoint().getCredit() == 0) {
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().flow(1);
            }

            // Wait for the timeout for the message(s) to arrive, then drain if required
            // and wait for remaining message(s) to arrive or a flow indicating
            // removal of the remaining credit.
            stopOnSchedule(timeout, request);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        Delivery incoming = null;
        do {
            incoming = getEndpoint().current();
            if (incoming != null) {
                if (incoming.isReadable() && !incoming.isPartial()) {
                    LOG.trace("{} has incoming Message(s).", this);
                    try {
                        if (processDelivery(incoming)) {
                            // We processed a message, signal completion
                            // of a message pull request if there is one.
                            if (pullRequest != null) {
                                pullRequest.onSuccess();
                                pullRequest = null;
                            }
                        }
                    } catch (Exception e) {
                        throw IOExceptionSupport.create(e);
                    }
                } else {
                    LOG.trace("{} has a partial incoming Message(s), deferring.", this);
                    incoming = null;
                }
            } else {
                // We have exhausted the locally queued messages on this link.
                // Check if we tried to stop and have now run out of credit.
                if (getEndpoint().getRemoteCredit() <= 0) {
                    if (stopRequest != null) {
                        stopRequest.onSuccess();
                        stopRequest = null;
                    }
                }
            }
        } while (incoming != null);

        super.processDeliveryUpdates(provider);
    }

    private boolean processDelivery(Delivery incoming) throws Exception {
        incoming.setDefaultDeliveryState(Released.getInstance());
        JmsMessage message = null;
        try {
            message = AmqpJmsMessageBuilder.createJmsMessage(this, unwrapIncomingMessage(incoming));
        } catch (Exception e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the message.
            //        In the future once the JMS mapping is complete we should be
            //        able to convert everything to some message even if its just
            //        a bytes messages as a fall back.
            deliveryFailedUndeliverable(incoming);
            return false;
        }

        getEndpoint().advance();

        // Let the message do any final processing before sending it onto a consumer.
        // We could defer this to a later stage such as the JmsConnection or even in
        // the JmsMessageConsumer dispatch method if we needed to.
        message.onDispatch();

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(getNextIncomingSequenceNumber());
        envelope.setMessage(message);
        envelope.setConsumerId(getResourceInfo().getId());
        // Store link to delivery in the hint for use in acknowledge requests.
        envelope.setProviderHint(incoming);
        envelope.setMessageId(message.getFacade().getProviderMessageIdObject());

        // Store reference to envelope in delivery context for recovery
        incoming.setContext(envelope);

        deliver(envelope);

        return true;
    }

    protected long getNextIncomingSequenceNumber() {
        return incomingSequence.incrementAndGet();
    }

    @Override
    protected void closeOrDetachEndpoint() {
        if (getResourceInfo().isDurable()) {
            getEndpoint().detach();
        } else {
            getEndpoint().close();
        }
    }

    public AmqpConnection getConnection() {
        return session.getConnection();
    }

    public AmqpSession getSession() {
        return session;
    }

    public JmsConsumerId getConsumerId() {
        return this.getResourceInfo().getId();
    }

    public JmsDestination getDestination() {
        return this.getResourceInfo().getDestination();
    }

    public boolean isPresettle() {
        return presettle || getResourceInfo().isBrowser();
    }

    public boolean isStopping() {
        return stopRequest != null;
    }

    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    public int getDrainTimeout() {
        return session.getProvider().getDrainTimeout();
    }

    @Override
    public String toString() {
        return "AmqpConsumer { " + getResourceInfo().getId() + " }";
    }

    protected void deliveryFailedUndeliverable(Delivery incoming) {
        incoming.disposition(MODIFIED_FAILED_UNDELIVERABLE);
        incoming.settle();
        // TODO: this flows credit, which we might not want, e.g if
        // a drain was issued to stop the link.
        sendFlowIfNeeded();
    }

    protected void deliver(JmsInboundMessageDispatch envelope) throws Exception {
        ProviderListener listener = session.getProvider().getProviderListener();
        if (listener != null) {
            LOG.debug("Dispatching received message: {}", envelope);
            listener.onInboundMessage(envelope);
        } else {
            LOG.error("Provider listener is not set, message will be dropped: {}", envelope);
        }
    }

    protected ByteBuf unwrapIncomingMessage(Delivery incoming) {
        int count;

        while ((count = getEndpoint().recv(incomingBuffer.array(), incomingBuffer.writerIndex(), incomingBuffer.writableBytes())) > 0) {
            incomingBuffer.writerIndex(incomingBuffer.writerIndex() + count);
            if (!incomingBuffer.isWritable()) {
                incomingBuffer.capacity((int) (incomingBuffer.capacity() * 1.5));
            }
        }

        try {
            return incomingBuffer.duplicate();
        } finally {
            incomingBuffer.clear();
        }
    }

    public void preCommit() {
    }

    public void preRollback() {
    }

    public void postCommit() {
    }

    public void postRollback() {
    }

    //----- Inner classes used in message pull operations --------------------//

    protected static final class ScheduledRequest implements AsyncResult {

        private final ScheduledFuture<?> sheduledTask;
        private final AsyncResult origRequest;

        public ScheduledRequest(ScheduledFuture<?> completionTask, AsyncResult origRequest) {
            this.sheduledTask = completionTask;
            this.origRequest = origRequest;
        }

        @Override
        public void onFailure(Throwable cause) {
            sheduledTask.cancel(false);
            origRequest.onFailure(cause);
        }

        @Override
        public void onSuccess() {
            boolean cancelled = sheduledTask.cancel(false);
            if (cancelled) {
                // Signal completion. Otherwise wait for the scheduled task to do it.
                origRequest.onSuccess();
            }
        }

        @Override
        public boolean isComplete() {
            return origRequest.isComplete();
        }
    }
}
