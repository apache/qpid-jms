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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.jms.AutoOutboundTransformer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.OutboundTransformer;
import org.apache.qpid.proton.message.Message;
import org.fusesource.hawtbuf.Buffer;
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
    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();
    private final LinkedList<PendingSend> pendingSends = new LinkedList<PendingSend>();
    private byte[] encodeBuffer = new byte[1024 * 8];

    private final OutboundTransformer outboundTransformer = new AutoOutboundTransformer(AmqpJMSVendor.INSTANCE);
    private final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";
    private boolean presettle = false;

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);
    }

    @Override
    public void close(AsyncResult request) {
        // If any sends are held we need to wait for them to complete.
        if (!pendingSends.isEmpty()) {
            this.closeRequest = request;
            return;
        }

        super.close(request);
    }

    @Override
    public boolean send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {

        // TODO - Handle the case where remote has no credit which means we can't send to it.
        //        We need to hold the send until remote credit becomes available but we should
        //        also have a send timeout option and filter timed out sends.
        if (endpoint.getCredit() <= 0) {
            LOG.trace("Holding Message send until credit is available.");
            // Once a message goes into a held mode we no longer can send it async, so
            // we clear the async flag if set to avoid the sender never getting notified.
            envelope.setSendAsync(false);
            this.pendingSends.addLast(new PendingSend(envelope, request));
            return false;
        } else {
            doSend(envelope, request);
            return true;
        }
    }

    private void doSend(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        JmsMessageFacade facade = envelope.getMessage().getFacade();

        LOG.trace("Producer sending message: {}", envelope.getMessage().getFacade().getMessageId());

        byte[] tag = tagGenerator.getNextTag();
        Delivery delivery = null;

        if (presettle) {
            delivery = endpoint.delivery(EMPTY_BYTE_ARRAY, 0, 0);
        } else {
            delivery = endpoint.delivery(tag, 0, tag.length);
        }

        delivery.setContext(request);

        if (session.isTransacted()) {
            Binary amqpTxId = session.getTransactionContext().getAmqpTransactionId();
            TransactionalState state = new TransactionalState();
            state.setTxnId(amqpTxId);
            delivery.disposition(state);
        }

        JmsMessage message = envelope.getMessage();
        message.setReadOnlyBody(true);

        // TODO: why do we need this?
        // Possibly because AMQP spec "2.7.5 Transfer" says message format MUST be set on at least
        // the first Transfer frame of a message.  That is on the encoded Transfer frames though and
        // this property isn't, but rather within the application-properties map.  We should probably
        // ensure this elsewhere (appears Proton does so itself in TransportImpl#processTransportWorkSender)
        if (!message.getProperties().containsKey(MESSAGE_FORMAT_KEY)) {
            message.setProperty(MESSAGE_FORMAT_KEY, 0);
        }

        if (facade instanceof AmqpJmsMessageFacade) {
            AmqpJmsMessageFacade amqpMessage = (AmqpJmsMessageFacade) facade;
            encodeAndSend(amqpMessage.getAmqpMessage(), delivery);
        } else {
            encodeAndSend(envelope.getMessage(), delivery);
        }

        if (presettle) {
            delivery.settle();
        } else {
            pending.add(delivery);
            endpoint.advance();
        }

        if (envelope.isSendAsync() || presettle) {
            request.onSuccess();
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

        Buffer sendBuffer = new Buffer(encodeBuffer, 0, encodedSize);

        while (true) {
            int sent = endpoint.send(sendBuffer.data, sendBuffer.offset, sendBuffer.length);
            if (sent > 0) {
                sendBuffer.moveHead(sent);
                if (sendBuffer.length == 0) {
                    break;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }

    private void encodeAndSend(JmsMessage message, Delivery delivery) throws IOException {

        Buffer sendBuffer = null;
        EncodedMessage amqp = null;

        try {
            amqp = outboundTransformer.transform(message);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }

        if (amqp != null && amqp.getLength() > 0) {
            sendBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
        }

        while (true) {
            int sent = endpoint.send(sendBuffer.data, sendBuffer.offset, sendBuffer.length);
            if (sent > 0) {
                sendBuffer.moveHead(sent);
                if (sendBuffer.length == 0) {
                    break;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }

    @Override
    public void processFlowUpdates() throws IOException {
        if (!pendingSends.isEmpty() && endpoint.getCredit() > 0) {
            while (endpoint.getCredit() > 0 && !pendingSends.isEmpty()) {
                LOG.trace("Dispatching previously held send");
                PendingSend held = pendingSends.pop();
                try {
                    doSend(held.envelope, held.request);
                } catch (JMSException e) {
                    throw IOExceptionSupport.create(e);
                }
            }
        }

        // Once the pending sends queue is drained we can propagate the close request.
        if (pendingSends.isEmpty() && isAwaitingClose()) {
            super.close(closeRequest);
        }
    }

    @Override
    public void processDeliveryUpdates() {
        List<Delivery> toRemove = new ArrayList<Delivery>();

        for (Delivery delivery : pending) {
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
                continue;
            }

            AsyncResult request = (AsyncResult) delivery.getContext();

            if (outcome instanceof Accepted) {
                toRemove.add(delivery);
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                tagGenerator.returnTag(delivery.getTag());
                if (request != null && !request.isComplete()) {
                    request.onSuccess();
                }
            } else if (outcome instanceof Rejected) {
                Exception remoteError = getRemoteError();
                toRemove.add(delivery);
                LOG.trace("Outcome of delivery was rejected: {}", delivery);
                tagGenerator.returnTag(delivery.getTag());
                if (request != null && !request.isComplete()) {
                    request.onFailure(remoteError);
                } else {
                    connection.getProvider().fireProviderException(remoteError);
                }
            } else {
                LOG.warn("Message send updated with unsupported outcome: {}", outcome);
            }
        }

        pending.removeAll(toRemove);
    }

    @Override
    protected void doOpen() {
        JmsDestination destination = info.getDestination();

        String destnationName = session.getQualifiedName(destination);
        String sourceAddress = getProducerId().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        Target target = new Target();
        target.setAddress(destnationName);

        String senderName = sourceAddress + ":" + destnationName != null ? destnationName : "Anonymous";
        endpoint = session.getProtonSession().sender(senderName);
        endpoint.setSource(source);
        endpoint.setTarget(target);
        if (presettle) {
            endpoint.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            endpoint.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        endpoint.setReceiverSettleMode(ReceiverSettleMode.FIRST);
    }

    @Override
    protected void doClose() {
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public Sender getProtonSender() {
        return this.endpoint;
    }

    @Override
    public boolean isAnonymous() {
        return false;
    }

    @Override
    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    @Override
    public boolean isPresettle() {
        return this.presettle;
    }

    @Override
    public String toString() {
        return "AmqpFixedProducer { " + getProducerId() + " }";
    }

    private class PendingSend {

        public JmsOutboundMessageDispatch envelope;
        public AsyncResult request;

        public PendingSend(JmsOutboundMessageDispatch envelope, AsyncResult request) {
            this.envelope = envelope;
            this.request = request;
        }
    }
}
