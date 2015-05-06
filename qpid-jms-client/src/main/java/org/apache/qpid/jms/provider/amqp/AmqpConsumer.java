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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageBuilder;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
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
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer extends AmqpAbstractResource<JmsConsumerInfo, Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    protected static final Symbol COPY = Symbol.getSymbol("copy");
    protected static final Symbol JMS_NO_LOCAL_SYMBOL = Symbol.valueOf("no-local");
    protected static final Symbol JMS_SELECTOR_SYMBOL = Symbol.valueOf("jms-selector");

    private static final int INITIAL_BUFFER_CAPACITY = 1024 * 128;

    protected final AmqpSession session;
    protected final Map<JmsInboundMessageDispatch, Delivery> delivered = new LinkedHashMap<JmsInboundMessageDispatch, Delivery>();
    protected boolean presettle;

    private final ByteBuf incomingBuffer = Unpooled.buffer(INITIAL_BUFFER_CAPACITY);

    private final AtomicLong _incomingSequence = new AtomicLong(0);

    private AsyncResult stopRequest;

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info) {
        super(info);
        this.session = session;

        // Add a shortcut back to this Consumer for quicker lookups
        this.resource.getConsumerId().setProviderHint(this);
    }

    /**
     * Starts the consumer by setting the link credit to the given prefetch value.
     */
    public void start(AsyncResult request) {
        getEndpoint().flow(resource.getPrefetchSize());
        request.onSuccess();
    }

    /**
     * Stops the consumer, using all link credit and waiting for in-flight messages to arrive.
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
            //TODO: We dont actually want the additional messages that could be sent while
            // draining. We could explicitly reduce credit first, or possibly use 'echo' instead
            // of drain if it was supported. We would first need to understand what happens
            // if we reduce credit below the number of messages already in-flight before
            // the peer sees the update.
            stopRequest = request;
            receiver.drain(0);
        }
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

        super.processFlowUpdates(provider);
    }

    @Override
    protected void doOpen() {
        JmsDestination destination  = resource.getDestination();
        String subscription = AmqpDestinationHelper.INSTANCE.getDestinationAddress(destination, session.getConnection());

        Source source = new Source();
        source.setAddress(subscription);
        Target target = new Target();

        configureSource(source);

        String receiverName = "qpid-jms:receiver:" + getConsumerId() + ":" + subscription;
        if (resource.getSubscriptionName() != null && !resource.getSubscriptionName().isEmpty()) {
            // In the case of Durable Topic Subscriptions the client must use the same
            // receiver name which is derived from the subscription name property.
            receiverName = resource.getSubscriptionName();
        }

        Receiver receiver = session.getProtonSession().receiver(receiverName);
        receiver.setSource(source);
        receiver.setTarget(target);
        if (isPresettle()) {
            receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        setEndpoint(receiver);

        super.doOpen();
    }

    @Override
    protected void doOpenCompletion() {
        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source s = getEndpoint().getRemoteSource();
        if (s != null) {
            super.doOpenCompletion();
        } else {
            // No link terminus was created, the peer will now detach/close us.
        }
    }

    @Override
    protected Exception getOpenAbortException() {
        // Verify the attach response contained a non-null Source
        org.apache.qpid.proton.amqp.transport.Source s = getEndpoint().getRemoteSource();
        if (s != null) {
            return super.getOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new InvalidDestinationException("Link creation was refused");
        }
    }

    @Override
    public void opened() {
        this.session.addResource(this);
        super.opened();
    }

    @Override
    public void closed() {
        this.session.removeResource(this);
        super.closed();
    }

    protected void configureSource(Source source) {
        Map<Symbol, DescribedType> filters = new HashMap<Symbol, DescribedType>();
        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                          Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };

        if (resource.getSubscriptionName() != null && !resource.getSubscriptionName().isEmpty()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

        Symbol typeCapability =  AmqpDestinationHelper.INSTANCE.toTypeCapability(resource.getDestination());
        if(typeCapability != null) {
            source.setCapabilities(typeCapability);
        }

        source.setOutcomes(outcomes);

        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(false);

        source.setDefaultOutcome(modified);

        if (resource.isNoLocal()) {
            filters.put(JMS_NO_LOCAL_SYMBOL, AmqpJmsNoLocalType.NO_LOCAL);
        }

        if (resource.getSelector() != null && !resource.getSelector().trim().equals("")) {
            filters.put(JMS_SELECTOR_SYMBOL, new AmqpJmsSelectorType(resource.getSelector()));
        }

        if (!filters.isEmpty()) {
            source.setFilter(filters);
        }
    }

    /**
     * Called to acknowledge all messages that have been marked as delivered but
     * have not yet been marked consumed.  Usually this is called as part of an
     * client acknowledge session operation.
     *
     * Only messages that have already been acknowledged as delivered by the JMS
     * framework will be in the delivered Map.  This means that the link credit
     * would already have been given for these so we just need to settle them.
     */
    public void acknowledge() {
        LOG.trace("Session Acknowledge for consumer: {}", resource.getConsumerId());
        for (Delivery delivery : delivered.values()) {
            delivery.disposition(Accepted.getInstance());
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
            if (!isPresettle()) {
                delivered.put(envelope, delivery);
            }
            sendFlowIfNeeded();
        } else if (ackType.equals(ACK_TYPE.CONSUMED)) {
            // A Consumer may not always send a DELIVERED ack so we need to
            // check to ensure we don't add too much credit to the link.
            if (isPresettle() || delivered.remove(envelope) == null) {
                sendFlowIfNeeded();
            }
            LOG.debug("Consumed Ack of message: {}", envelope);
            if (!delivery.isSettled()) {
                if (session.isTransacted() && !isBrowser()) {
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
        } else if (ackType.equals(ACK_TYPE.REDELIVERED)) {
            //TODO: remove ack type?
        } else if (ackType.equals(ACK_TYPE.POISONED)) {
            deliveryFailed(delivery, false);
        } else if (ackType.equals(ACK_TYPE.RELEASED)) {
            delivery.disposition(Released.getInstance());
            delivery.settle();
        } else {
            LOG.warn("Unsupported Ack Type for message: {}", envelope);
        }
    }

    /**
     * We only send more credits as the credit window dwindles to a certain point and
     * then we open the window back up to full prefetch size.
     */
    private void sendFlowIfNeeded() {
        if (resource.getPrefetchSize() == 0) {
            return;
        }

        int currentCredit = getEndpoint().getCredit();
        if (currentCredit <= resource.getPrefetchSize() * 0.2) {
            getEndpoint().flow(resource.getPrefetchSize() - currentCredit);
        }
    }

    /**
     * Recovers all previously delivered but not acknowledged messages.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        LOG.debug("Session Recover for consumer: {}", resource.getConsumerId());
        Collection<JmsInboundMessageDispatch> values = delivered.keySet();
        ArrayList<JmsInboundMessageDispatch> envelopes = new ArrayList<JmsInboundMessageDispatch>(values);
        ListIterator<JmsInboundMessageDispatch> reverseIterator = envelopes.listIterator(values.size());

        while (reverseIterator.hasPrevious()) {
            JmsInboundMessageDispatch envelope = reverseIterator.previous();
            // TODO: apply connection redelivery policy to those messages that are past max redelivery.
            envelope.getMessage().getFacade().setRedeliveryCount(
                envelope.getMessage().getFacade().getRedeliveryCount() + 1);
            envelope.setEnqueueFirst(true);
            deliver(envelope);
        }

        delivered.clear();
    }

    /**
     * For a consumer whose prefetch value is set to zero this method will attempt to solicite
     * a new message dispatch from the broker.
     *
     * @param timeout
     */
    public void pull(long timeout) {
        if (resource.getPrefetchSize() == 0 && getEndpoint().getCredit() == 0) {
            // expand the credit window by one.
            getEndpoint().flow(1);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        Delivery incoming = null;
        do {
            incoming = getEndpoint().current();
            if (incoming != null) {
                if(incoming.isReadable() && !incoming.isPartial()) {
                    LOG.trace("{} has incoming Message(s).", this);
                    try {
                        processDelivery(incoming);
                    } catch (Exception e) {
                        throw IOExceptionSupport.create(e);
                    }
                    getEndpoint().advance();
                } else {
                    LOG.trace("{} has a partial incoming Message(s), deferring.", this);
                    incoming = null;
                }
            } else {
                // We have exhausted the locally queued messages on this link.
                // Check if we tried to stop and have now run out of credit.
                if(stopRequest != null) {
                    if(getEndpoint().getRemoteCredit() <= 0)
                    {
                        stopRequest.onSuccess();
                        stopRequest = null;
                    }
                }
            }
        } while (incoming != null);

        super.processDeliveryUpdates(provider);
    }

    private void processDelivery(Delivery incoming) throws Exception {
        JmsMessage message = null;
        try {
            message = AmqpJmsMessageBuilder.createJmsMessage(this, decodeIncomingMessage(incoming));
        } catch (Exception e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the message.
            //        In the future once the JMS mapping is complete we should be
            //        able to convert everything to some message even if its just
            //        a bytes messages as a fall back.
            deliveryFailed(incoming, true);
            return;
        }

        // Let the message do any final processing before sending it onto a consumer.
        // We could defer this to a later stage such as the JmsConnection or even in
        // the JmsMessageConsumer dispatch method if we needed to.
        message.onDispatch();

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(getNextIncomingSequenceNumber());
        envelope.setMessage(message);
        envelope.setConsumerId(resource.getConsumerId());
        // Store link to delivery in the hint for use in acknowledge requests.
        envelope.setProviderHint(incoming);
        envelope.setMessageId(message.getFacade().getProviderMessageIdObject());

        // Store reference to envelope in delivery context for recovery
        incoming.setContext(envelope);

        deliver(envelope);
    }

    protected long getNextIncomingSequenceNumber() {
        return _incomingSequence.incrementAndGet();
    }

    @Override
    protected void doClose() {
        if (resource.isDurable()) {
            getEndpoint().detach();
        } else {
            getEndpoint().close();
        }
    }

    public AmqpConnection getConnection() {
        return this.session.getConnection();
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public JmsConsumerId getConsumerId() {
        return this.resource.getConsumerId();
    }

    public JmsDestination getDestination() {
        return this.resource.getDestination();
    }

    public Receiver getProtonReceiver() {
        return this.getEndpoint();
    }

    public boolean isBrowser() {
        return false;
    }

    public boolean isPresettle() {
        return presettle;
    }

    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    @Override
    public String toString() {
        return "AmqpConsumer { " + this.resource.getConsumerId() + " }";
    }

    protected void deliveryFailed(Delivery incoming, boolean expandCredit) {
        Modified disposition = new Modified();
        disposition.setUndeliverableHere(true);
        disposition.setDeliveryFailed(true);
        incoming.disposition(disposition);
        incoming.settle();
        if (expandCredit) {
            getEndpoint().flow(1);
        }
    }

    protected void deliver(JmsInboundMessageDispatch envelope) throws Exception {
        ProviderListener listener = session.getProvider().getProviderListener();
        if (listener != null) {
            if (envelope.getMessage() != null) {
                LOG.debug("Dispatching received message: {}", envelope);
            } else {
                LOG.debug("Dispatching end of browse to: {}", envelope.getConsumerId());
            }
            listener.onInboundMessage(envelope);
        } else {
            LOG.error("Provider listener is not set, message will be dropped: {}", envelope);
        }
    }

    // TODO - Find more efficient ways to produce the Message instance.
    protected Message decodeIncomingMessage(Delivery incoming) {
        int count;

        while ((count = getEndpoint().recv(incomingBuffer.array(), incomingBuffer.writerIndex(), incomingBuffer.writableBytes())) > 0) {
            incomingBuffer.writerIndex(incomingBuffer.writerIndex() + count);
            if (!incomingBuffer.isWritable()) {
                incomingBuffer.capacity((int) (incomingBuffer.capacity() * 1.5));
            }
        }

        try {
            Message protonMessage = Message.Factory.create();
            protonMessage.decode(incomingBuffer.array(), 0, incomingBuffer.readableBytes());
            return protonMessage;
        } finally {
            incomingBuffer.clear();
        }
    }

    public void preCommit() {
    }

    public void preRollback() {
    }

    /**
     * @throws Exception if an error occurs while performing this action.
     */
    public void postCommit() throws Exception {
    }

    /**
     * @throws Exception if an error occurs while performing this action.
     */
    public void postRollback() throws Exception {
    }
}
