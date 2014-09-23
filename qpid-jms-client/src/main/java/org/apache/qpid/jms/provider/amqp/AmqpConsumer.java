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
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsMessageId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.InboundTransformer;
import org.apache.qpid.proton.jms.JMSMappingInboundTransformer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer extends AbstractAmqpResource<JmsConsumerInfo, Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    protected static final Symbol COPY = Symbol.getSymbol("copy");
    protected static final Symbol JMS_NO_LOCAL_SYMBOL = Symbol.valueOf("no-local");
    protected static final Symbol JMS_SELECTOR_SYMBOL = Symbol.valueOf("jms-selector");

    protected final AmqpSession session;
    protected final InboundTransformer inboundTransformer =
        new JMSMappingInboundTransformer(AmqpJMSVendor.INSTANCE);;
    protected final Map<JmsMessageId, Delivery> delivered = new LinkedHashMap<JmsMessageId, Delivery>();
    protected boolean presettle;

    private final ByteArrayOutputStream streamBuffer = new ByteArrayOutputStream();
    private final byte incomingBuffer[] = new byte[1024 * 64];

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info) {
        super(info);
        this.session = session;

        // Add a shortcut back to this Consumer for quicker lookups
        this.info.getConsumerId().setProviderHint(this);
    }

    /**
     * Starts the consumer by setting the link credit to the given prefetch value.
     */
    public void start(AsyncResult request) {
        this.endpoint.flow(info.getPrefetchSize());
        request.onSuccess();
    }

    @Override
    protected void doOpen() {
        JmsDestination destination  = info.getDestination();
        String subscription = session.getQualifiedName(destination);

        Source source = new Source();
        source.setAddress(subscription);
        Target target = new Target();

        configureSource(source);

        String receiverName = getConsumerId() + ":" + subscription;
        if (info.getSubscriptionName() != null && !info.getSubscriptionName().isEmpty()) {
            // In the case of Durable Topic Subscriptions the client must use the same
            // receiver name which is derived from the subscription name property.
            receiverName = info.getSubscriptionName();
        }

        endpoint = session.getProtonSession().receiver(receiverName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        if (isPresettle()) {
            endpoint.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);
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

        if (info.getSubscriptionName() != null && !info.getSubscriptionName().isEmpty()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
            source.setDistributionMode(COPY);
        } else {
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }

        if (info.isNoLocal()) {
            filters.put(JMS_NO_LOCAL_SYMBOL, AmqpJmsNoLocalType.NO_LOCAL);
        }

        if (info.getSelector() != null && !info.getSelector().trim().equals("")) {
            filters.put(JMS_SELECTOR_SYMBOL, new AmqpJmsSelectorType(info.getSelector()));
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
        LOG.trace("Session Acknowledge for consumer: {}", info.getConsumerId());
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
        JmsMessageId messageId = envelope.getMessage().getFacade().getMessageId();
        Delivery delivery = null;

        if (messageId.getProviderHint() instanceof Delivery) {
            delivery = (Delivery) messageId.getProviderHint();
        } else {
            delivery = delivered.get(messageId);
            if (delivery == null) {
                LOG.warn("Received Ack for unknown message: {}", messageId);
                return;
            }
        }

        if (ackType.equals(ACK_TYPE.DELIVERED)) {
            LOG.debug("Delivered Ack of message: {}", messageId);
            if (session.isTransacted()) {
                Binary txnId = session.getTransactionContext().getAmqpTransactionId();
                if (txnId != null) {
                    TransactionalState txState = new TransactionalState();
                    txState.setOutcome(Accepted.getInstance());
                    txState.setTxnId(txnId);
                    delivery.disposition(txState);
                    session.getTransactionContext().registerTxConsumer(this);
                }
            }
            if (!isPresettle()) {
                delivered.put(messageId, delivery);
            }
            sendFlowIfNeeded();
        } else if (ackType.equals(ACK_TYPE.CONSUMED)) {
            // A Consumer may not always send a delivered ACK so we need to check to
            // ensure we don't add to much credit to the link.
            if (isPresettle() || delivered.remove(messageId) == null) {
                sendFlowIfNeeded();
            }
            LOG.debug("Consumed Ack of message: {}", messageId);
            if (!delivery.isSettled()) {
                delivery.disposition(Accepted.getInstance());
                delivery.settle();
            }
        } else if (ackType.equals(ACK_TYPE.REDELIVERED)) {
            Modified disposition = new Modified();
            disposition.setUndeliverableHere(false);
            disposition.setDeliveryFailed(true);
            delivery.disposition(disposition);
            delivery.settle();
        } else if (ackType.equals(ACK_TYPE.POISONED)) {
            deliveryFailed(delivery, false);
        } else {
            LOG.warn("Unsupporeted Ack Type for message: {}", messageId);
        }
    }

    /**
     * We only send more credits as the credit window dwindles to a certain point and
     * then we open the window back up to full prefetch size.
     */
    private void sendFlowIfNeeded() {
        if (info.getPrefetchSize() == 0) {
            return;
        }

        int currentCredit = endpoint.getCredit();
        if (currentCredit <= info.getPrefetchSize() * 0.2) {
            endpoint.flow(info.getPrefetchSize() - currentCredit);
        }
    }

    /**
     * Recovers all previously delivered but not acknowledged messages.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        LOG.debug("Session Recover for consumer: {}", info.getConsumerId());
        for (Delivery delivery : delivered.values()) {
            // TODO - increment redelivery counter and apply connection redelivery policy
            //        to those messages that are past max redlivery.
            JmsInboundMessageDispatch envelope = (JmsInboundMessageDispatch) delivery.getContext();
            envelope.onMessageRedelivered();
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
        if (info.getPrefetchSize() == 0 && endpoint.getCredit() == 0) {
            // expand the credit window by one.
            endpoint.flow(1);
        }
    }

    @Override
    public void processDeliveryUpdates() throws IOException {
        Delivery incoming = null;
        do {
            incoming = endpoint.current();
            if (incoming != null && incoming.isReadable() && !incoming.isPartial()) {
                LOG.trace("{} has incoming Message(s).", this);
                try {
                    processDelivery(incoming);
                } catch (Exception e) {
                    throw IOExceptionSupport.create(e);
                }
                endpoint.advance();
            } else {
                LOG.trace("{} has a partial incoming Message(s), deferring.", this);
                incoming = null;
            }
        } while (incoming != null);
    }

    private void processDelivery(Delivery incoming) throws Exception {
        EncodedMessage encoded = readIncomingMessage(incoming);
        JmsMessage message = null;
        try {
            message = (JmsMessage) inboundTransformer.transform(encoded);
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

        try {
            message.setJMSDestination(info.getDestination());
        } catch (JMSException e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the destination.
            deliveryFailed(incoming, true);
            return;
        }

        // Store link to delivery in the hint for use in acknowledge requests.
        message.getFacade().getMessageId().setProviderHint(incoming);

        // We need to signal to the create message that it's being dispatched and for now
        // the transformer creates the message in write mode, onSend will reset it to read
        // mode and the consumer will see it as a normal received message.
        message.onSend();

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch();
        envelope.setMessage(message);
        envelope.setConsumerId(info.getConsumerId());
        envelope.setProviderHint(incoming);

        // Store reference to envelope in delivery context for recovery
        incoming.setContext(envelope);

        deliver(envelope);
    }

    @Override
    protected void doClose() {
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public JmsConsumerId getConsumerId() {
        return this.info.getConsumerId();
    }

    public Receiver getProtonReceiver() {
        return this.endpoint;
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
        return "AmqpConsumer { " + this.info.getConsumerId() + " }";
    }

    protected void deliveryFailed(Delivery incoming, boolean expandCredit) {
        Modified disposition = new Modified();
        disposition.setUndeliverableHere(true);
        disposition.setDeliveryFailed(true);
        incoming.disposition(disposition);
        incoming.settle();
        if (expandCredit) {
            endpoint.flow(1);
        }
    }

    protected void deliver(JmsInboundMessageDispatch envelope) throws Exception {
        ProviderListener listener = session.getProvider().getProviderListener();
        if (listener != null) {
            if (envelope.getMessage() != null) {
                LOG.debug("Dispatching received message: {}", envelope.getMessage().getFacade().getMessageId());
            } else {
                LOG.debug("Dispatching end of browse to: {}", envelope.getConsumerId());
            }
            listener.onMessage(envelope);
        } else {
            LOG.error("Provider listener is not set, message will be dropped.");
        }
    }

    protected EncodedMessage readIncomingMessage(Delivery incoming) {
        Buffer buffer;
        int count;

        while ((count = endpoint.recv(incomingBuffer, 0, incomingBuffer.length)) > 0) {
            streamBuffer.write(incomingBuffer, 0, count);
        }

        buffer = streamBuffer.toBuffer();

        try {
            return new EncodedMessage(incoming.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
        } finally {
            streamBuffer.reset();
        }
    }

    public void preCommit() {
    }

    public void preRollback() {
    }

    /**
     * Ensures that all delivered messages are marked as settled locally before the TX state
     * is cleared and the next TX started.
     *
     * @throws Exception if an error occurs while performing this action.
     */
    public void postCommit() throws Exception {
        for (Delivery delivery : delivered.values()) {
            delivery.settle();
        }
        this.delivered.clear();
    }

    /**
     * Redeliver Acknowledge all previously delivered messages and clear state to prepare for
     * the next TX to start.
     *
     * @throws Exception if an error occurs while performing this action.
     */
    public void postRollback() throws Exception {
        for (Delivery delivery : delivered.values()) {
            JmsInboundMessageDispatch envelope = (JmsInboundMessageDispatch) delivery.getContext();
            acknowledge(envelope, ACK_TYPE.REDELIVERED);
        }
        this.delivered.clear();
    }
}
