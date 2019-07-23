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
package org.apache.qpid.jms;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.qpid.jms.exceptions.JmsConnectionFailedException;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderSynchronization;

/**
 * Implementation of a JMS MessageProducer
 */
public class JmsMessageProducer implements AutoCloseable, MessageProducer {

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsProducerInfo producerInfo;
    protected final boolean anonymousProducer;
    protected long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected boolean disableMessageId;
    protected boolean disableTimestamp;
    protected final AtomicLong messageSequence = new AtomicLong();
    protected final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    protected JmsMessageProducer(JmsProducerId producerId, JmsSession session, JmsDestination destination) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.anonymousProducer = destination == null;

        JmsMessageIDBuilder messageIDBuilder =
            session.getMessageIDPolicy().getMessageIDBuilder(session, destination).initialize(producerId.toString());

        this.producerInfo = new JmsProducerInfo(producerId, messageIDBuilder);
        this.producerInfo.setDestination(destination);
        this.producerInfo.setPresettle(session.getPresettlePolicy().isProducerPresttled(session, destination));

        session.getConnection().createResource(producerInfo, new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
                session.add(JmsMessageProducer.this);
            }

            @Override
            public void onPendingFailure(ProviderException cause) {
            }
        });
    }

    @Override
    public void close() throws JMSException {
        if (!closed.get()) {
            doClose();
        }
    }

    /**
     * Called to initiate shutdown of Producer resources and request that the remote
     * peer remove the registered producer.
     *
     * @throws JMSException if an internal error occurs during the close operation.
     */
    protected void doClose() throws JMSException {
        session.checkIsCompletionThread();
        shutdown();
        try {
            connection.destroyResource(producerInfo);
        } catch (JmsConnectionFailedException jmsEx) {
        }
    }

    /**
     * Called to release all producer resources without requiring a destroy request
     * to be sent to the remote peer.  This is most commonly needed when the parent
     * Session is closing.
     *
     * @throws JMSException if an internal error occurs during the shutdown operation.
     */
    protected void shutdown() throws JMSException {
        shutdown(null);
    }

    protected void shutdown(Throwable cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            producerInfo.setState(ResourceState.CLOSED);
            failureCause.set(cause);
            session.remove(this);
        }
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkClosed();
        return deliveryDelay;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return deliveryMode;
    }

    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return producerInfo.getDestination();
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return disableMessageId;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return disableTimestamp;
    }

    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return priority;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return timeToLive;
    }

    @Override
    public void send(Message message) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        sendMessage(producerInfo.getDestination(), message, deliveryMode, priority, timeToLive, null);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, null);
    }

    @Override
    public void send(Message message, CompletionListener listener) throws JMSException {
        send(message, deliveryMode, priority, timeToLive, listener);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        if (listener == null) {
            throw new IllegalArgumentException("JmsCompletetionListener cannot be null");
        }

        sendMessage(producerInfo.getDestination(), message, deliveryMode, priority, timeToLive, listener);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener listener) throws JMSException {
        send(destination, message, this.deliveryMode, this.priority, this.timeToLive, listener);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        checkClosed();

        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        if (listener == null) {
            throw new IllegalArgumentException("JmsCompletetionListener cannot be null");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive, listener);
    }

    private void checkDestinationNotInvalid(Destination destination) throws InvalidDestinationException {
        if (destination == null) {
            throw new InvalidDestinationException("Destination must not be null");
        }
    }

    private void sendMessage(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        this.session.send(this, destination, message, deliveryMode, priority, timeToLive, disableMessageId, disableTimestamp, deliveryDelay, listener);
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkClosed();
        this.deliveryDelay = deliveryDelay;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        switch (deliveryMode) {
            case DeliveryMode.PERSISTENT:
            case DeliveryMode.NON_PERSISTENT:
                this.deliveryMode = deliveryMode;
                break;
            default:
                throw new JMSException(String.format("Invalid DeliveryMode specified: %d", deliveryMode));
        }
    }

    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkClosed();
        this.disableMessageId = value;
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkClosed();
        this.disableTimestamp = value;
    }

    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        checkClosed();

        if (defaultPriority < 0 || defaultPriority > 9) {
            throw new JMSException(String.format("Priority value given {%d} is out of range (0..9)", defaultPriority));
        }

        this.priority = defaultPriority;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkClosed();
        this.timeToLive = timeToLive;
    }

    /**
     * @return the producer's assigned JmsProducerId.
     */
    protected JmsProducerId getProducerId() {
        return this.producerInfo.getId();
    }

    /**
     * @return the next logical sequence for a Message sent from this Producer.
     */
    protected long getNextMessageSequence() {
        return messageSequence.incrementAndGet();
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException jmsEx = null;

            if (getFailureCause() == null) {
                jmsEx = new IllegalStateException("The MessageProducer is closed");
            } else {
                jmsEx = new IllegalStateException("The MessageProducer was closed due to an unrecoverable error.");
                jmsEx.initCause(getFailureCause());
            }

            throw jmsEx;
        }
    }

    protected boolean isPresettled() {
        return producerInfo.isPresettle();
    }

    protected boolean isAnonymous() {
        return anonymousProducer;
    }

    protected JmsMessageIDBuilder getMessageIDBuilder() {
        return producerInfo.getMessageIDBuilder();
    }

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        if (failureCause.get() == null) {
            return session.getFailureCause();
        }

        return failureCause.get();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Connection interruption handlers.
    ////////////////////////////////////////////////////////////////////////////

    protected void onConnectionInterrupted() {
    }

    protected void onConnectionRecovery(Provider provider) throws Exception {
        if (!producerInfo.isClosed()) {
            ProviderFuture request = provider.newProviderFuture();
            try {
                provider.create(producerInfo, request);
                request.sync();
            } catch (ProviderException poe) {
                if (connection.isCloseLinksThatFailOnReconnect()) {
                    session.producerClosed(producerInfo, poe);
                } else {
                    throw poe;
                }
            }
        }
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {
    }

    protected void onConnectionRestored() {
    }
}
