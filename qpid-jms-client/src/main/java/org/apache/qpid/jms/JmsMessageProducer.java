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
package org.apache.qpid.jms;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;

/**
 * Implementation of a JMS MessageProducer
 */
public class JmsMessageProducer implements MessageProducer {

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsProducerInfo producerInfo;
    protected final boolean flexibleDestination;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected boolean disableMessageId;
    protected boolean disableTimestamp;
    protected final AtomicLong messageSequence = new AtomicLong();
    protected Exception failureCause;

    protected JmsMessageProducer(JmsProducerId producerId, JmsSession session, JmsDestination destination) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.flexibleDestination = destination == null;
        this.producerInfo = new JmsProducerInfo(producerId);
        this.producerInfo.setDestination(destination);

        session.getConnection().createResource(producerInfo);
    }

    /**
     * Close the producer
     *
     * @throws JMSException
     *
     * @see javax.jms.MessageProducer#close()
     */
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
     * @throws JMSException
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
     * @throws JMSException
     */
    protected void shutdown() throws JMSException {
        shutdown(null);
    }

    protected void shutdown(Exception cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            failureCause = cause;
            session.remove(this);
        }
    }

    /**
     * @return the delivery mode
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDeliveryMode()
     */
    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return this.deliveryMode;
    }

    /**
     * @return the destination
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDestination()
     */
    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return this.producerInfo.getDestination();
    }

    /**
     * @return true if disableIds is set
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDisableMessageID()
     */
    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return this.disableMessageId;
    }

    /**
     * @return true if disable timestamp is set
     * @throws JMSException
     * @see javax.jms.MessageProducer#getDisableMessageTimestamp()
     */
    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return this.disableTimestamp;
    }

    /**
     * @return the priority
     * @throws JMSException
     * @see javax.jms.MessageProducer#getPriority()
     */
    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return this.priority;
    }

    /**
     * @return timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#getTimeToLive()
     */
    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return this.timeToLive;
    }

    /**
     * @param message
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Message)
     */
    @Override
    public void send(Message message) throws JMSException {
        send(message, this.deliveryMode, this.priority, this.timeToLive);
    }

    /**
     * @param message
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (flexibleDestination) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        sendMessage(producerInfo.getDestination(), message, deliveryMode, priority, timeToLive);
    }

    /**
     * @param destination
     * @param message
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     *      javax.jms.Message)
     */
    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, this.deliveryMode, this.priority, this.timeToLive);
    }

    /**
     * @param destination
     * @param message
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     *      javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (!flexibleDestination) {
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

    /**
     * @param deliveryMode
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDeliveryMode(int)
     */
    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        this.deliveryMode = deliveryMode;
    }

    /**
     * @param value
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDisableMessageID(boolean)
     */
    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkClosed();
        this.disableMessageId = value;
    }

    /**
     * @param value
     * @throws JMSException
     * @see javax.jms.MessageProducer#setDisableMessageTimestamp(boolean)
     */
    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkClosed();
        this.disableTimestamp = value;
    }

    /**
     * @param defaultPriority
     * @throws JMSException
     * @see javax.jms.MessageProducer#setPriority(int)
     */
    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        checkClosed();
        this.priority = defaultPriority;
    }

    /**
     * @param timeToLive
     * @throws JMSException
     * @see javax.jms.MessageProducer#setTimeToLive(long)
     */
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

            if (failureCause == null) {
                jmsEx = new IllegalStateException("The MessageProducer is closed");
            } else {
                jmsEx = new IllegalStateException("The MessageProducer was closed due to an unrecoverable error.");
                jmsEx.initCause(failureCause);
            }

            throw jmsEx;
        }
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
