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

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;

/**
 * Implementation of a JMS MessageProducer
 */
public class JmsMessageProducer implements AutoCloseable, MessageProducer {

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsProducerInfo producerInfo;
    protected final boolean anonymousProducer;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected boolean disableMessageId;
    protected boolean disableTimestamp;
    protected final AtomicLong messageSequence = new AtomicLong();
    protected final AtomicReference<Exception> failureCause = new AtomicReference<>();

    protected JmsMessageProducer(JmsProducerId producerId, JmsSession session, JmsDestination destination) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.anonymousProducer = destination == null;

        JmsMessageIDBuilder messageIDBuilder =
            session.getMessageIDPolicy().getMessageIDBuilder(session, destination);

        this.producerInfo = new JmsProducerInfo(producerId, messageIDBuilder);
        this.producerInfo.setDestination(destination);
        this.producerInfo.setPresettle(session.getPresettlePolicy().isProducerPresttled(session, destination));

        session.getConnection().createResource(producerInfo);
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
        shutdown();
        this.connection.destroyResource(producerInfo);
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

    protected void shutdown(Exception cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            failureCause.set(cause);
            session.remove(this);
        }
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return this.deliveryMode;
    }

    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return this.producerInfo.getDestination();
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return this.disableMessageId;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return this.disableTimestamp;
    }

    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return this.priority;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return this.timeToLive;
    }

    @Override
    public void send(Message message) throws JMSException {
        send(message, this.deliveryMode, this.priority, this.timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        sendMessage(producerInfo.getDestination(), message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, this.deliveryMode, this.priority, this.timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        sendMessage(destination, message, deliveryMode, priority, timeToLive);
    }

    private void sendMessage(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (destination == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        }

        this.session.send(this, destination, message, deliveryMode, priority, timeToLive, disableMessageId, disableTimestamp);
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        this.deliveryMode = deliveryMode;
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
        return this.messageSequence.incrementAndGet();
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

    void setFailureCause(Exception failureCause) {
        this.failureCause.set(failureCause);
    }

    Exception getFailureCause() {
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
        ProviderFuture request = new ProviderFuture();
        provider.create(producerInfo, request);
        request.sync();
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {
    }

    protected void onConnectionRestored() {
    }
}
