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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.util.FifoMessageQueue;
import org.apache.qpid.jms.util.MessageQueue;
import org.apache.qpid.jms.util.PriorityMessageQueue;

/**
 * implementation of a JMS Message Consumer
 */
public class JmsMessageConsumer implements MessageConsumer, JmsMessageAvailableConsumer, JmsMessageDispatcher {

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsConsumerInfo consumerInfo;
    protected final int acknowledgementMode;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected boolean started;
    protected MessageListener messageListener;
    protected JmsMessageAvailableListener availableListener;
    protected final MessageQueue messageQueue;
    protected final Lock lock = new ReentrantLock();
    protected final AtomicBoolean suspendedConnection = new AtomicBoolean();
    protected final AtomicBoolean delivered = new AtomicBoolean();

    /**
     * Create a non-durable MessageConsumer
     *
     * @param consumerId
     * @param session
     * @param destination
     * @param selector
     * @param noLocal
     * @throws JMSException
     */
    protected JmsMessageConsumer(JmsConsumerId consumerId, JmsSession session, JmsDestination destination,
        String selector, boolean noLocal) throws JMSException {
        this(consumerId, session, destination, null, selector, noLocal);
    }

    /**
     * Create a MessageConsumer which could be durable.
     *
     * @param consumerId
     * @param session
     * @param destination
     * @param name
     * @param selector
     * @param noLocal
     * @throws JMSException
     */
    protected JmsMessageConsumer(JmsConsumerId consumerId, JmsSession session, JmsDestination destination,
                                 String name, String selector, boolean noLocal) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.acknowledgementMode = session.acknowledgementMode();

        if (connection.isMessagePrioritySupported()) {
            this.messageQueue = new PriorityMessageQueue();
        } else {
            this.messageQueue = new FifoMessageQueue();
        }

        JmsPrefetchPolicy policy = this.connection.getPrefetchPolicy();

        this.consumerInfo = new JmsConsumerInfo(consumerId);
        this.consumerInfo.setClientId(connection.getClientID());
        this.consumerInfo.setSelector(selector);
        this.consumerInfo.setSubscriptionName(name);
        this.consumerInfo.setDestination(destination);
        this.consumerInfo.setAcknowledgementMode(acknowledgementMode);
        this.consumerInfo.setNoLocal(noLocal);
        this.consumerInfo.setBrowser(isBrowser());
        this.consumerInfo.setPrefetchSize(getConfiguredPrefetch(destination, policy));

        try {
            this.consumerInfo = session.getConnection().createResource(consumerInfo);
        } catch (JMSException ex) {
            throw ex;
        }
    }

    public void init() throws JMSException {
        session.add(this);
        try {
            session.getConnection().startResource(consumerInfo);
        } catch (JMSException ex) {
            session.remove(this);
            throw ex;
        }
    }

    /**
     * @throws JMSException
     * @see javax.jms.MessageConsumer#close()
     */
    @Override
    public void close() throws JMSException {
        if (!closed.get()) {
            if (delivered.get() && session.getTransactionContext().isInTransaction()) {
                session.getTransactionContext().addSynchronization(new JmsTxSynchronization() {
                    @Override
                    public void afterCommit() throws Exception {
                        doClose();
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        doClose();
                    }
                });
            } else {
                doClose();
            }
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
        this.connection.destroyResource(consumerInfo);
    }

    /**
     * Called to release all producer resources without requiring a destroy request
     * to be sent to the remote peer.  This is most commonly needed when the parent
     * Session is closing.
     *
     * @throws JMSException
     */
    protected void shutdown() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            this.session.remove(this);
        }
    }

    /**
     * @return a Message or null if closed during the operation
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receive()
     */
    @Override
    public Message receive() throws JMSException {
        checkClosed();
        checkMessageListener();
        sendPullCommand(0);

        try {
            return copy(ack(this.messageQueue.dequeue(-1)));
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @param timeout
     * @return a Message or null
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receive(long)
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        checkMessageListener();
        sendPullCommand(timeout);

        if (timeout > 0) {
            try {
                return copy(ack(this.messageQueue.dequeue(timeout)));
            } catch (InterruptedException e) {
                throw JmsExceptionSupport.create(e);
            }
        }

        return null;
    }

    /**
     * @return a Message or null
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receiveNoWait()
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        checkMessageListener();
        sendPullCommand(-1);

        return copy(ack(this.messageQueue.dequeueNoWait()));
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The MessageConsumer is closed");
        }
    }

    JmsMessage copy(final JmsInboundMessageDispatch envelope) throws JMSException {
        if (envelope == null || envelope.getMessage() == null) {
            return null;
        }
        return envelope.getMessage().copy();
    }

    JmsInboundMessageDispatch ack(final JmsInboundMessageDispatch envelope) throws JMSException {
        if (envelope != null && envelope.getMessage() != null) {
            JmsMessage message = envelope.getMessage();
            if (message.getAcknowledgeCallback() != null || session.isTransacted()) {
                // Message has been received by the app.. expand the credit
                // window so that we receive more messages.
                session.acknowledge(envelope, ACK_TYPE.DELIVERED);
            } else {
                doAck(envelope);
            }
            // Tags that we have delivered and can't close if in a TX Session.
            delivered.set(true);
        }
        return envelope;
    }

    private void doAck(final JmsInboundMessageDispatch envelope) throws JMSException {
        checkClosed();
        try {
            session.acknowledge(envelope, ACK_TYPE.CONSUMED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
    }

    /**
     * Called from the session when a new Message has been dispatched to this Consumer
     * from the connection.
     *
     * @param facade
     *        the newly arrived message.
     */
    @Override
    public void onMessage(final JmsInboundMessageDispatch envelope) {
        lock.lock();
        try {
            if (acknowledgementMode == Session.CLIENT_ACKNOWLEDGE) {
                envelope.getMessage().setAcknowledgeCallback(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (session.isClosed()) {
                            throw new javax.jms.IllegalStateException("Session closed.");
                        }
                        session.acknowledge();
                        return null;
                    }
                });
            }
            this.messageQueue.enqueue(envelope);
        } finally {
            lock.unlock();
        }

        if (this.messageListener != null && this.started) {
            session.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    JmsInboundMessageDispatch envelope;
                    while (session.isStarted() && (envelope = messageQueue.dequeueNoWait()) != null) {
                        try {
                            messageListener.onMessage(copy(ack(envelope)));
                        } catch (Exception e) {
                            session.getConnection().onException(e);
                        }
                    }
                }
            });
        } else {
            if (availableListener != null) {
                availableListener.onMessageAvailable(this);
            }
        }
    }

    public void start() {
        lock.lock();
        try {
            this.started = true;
            this.messageQueue.start();
            drainMessageQueueToListener();
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        lock.lock();
        try {
            this.started = false;
            this.messageQueue.stop();
        } finally {
            lock.unlock();
        }
    }

    void drainMessageQueueToListener() {
        MessageListener listener = this.messageListener;
        if (listener != null) {
            if (!this.messageQueue.isEmpty()) {
                List<JmsInboundMessageDispatch> drain = this.messageQueue.removeAll();
                for (JmsInboundMessageDispatch envelope : drain) {
                    try {
                        listener.onMessage(copy(ack(envelope)));
                    } catch (Exception e) {
                        session.getConnection().onException(e);
                    }
                }
                drain.clear();
            }
        }
    }

    /**
     * @return the id
     */
    public JmsConsumerId getConsumerId() {
        return this.consumerInfo.getConsumerId();
    }

    /**
     * @return the Destination
     */
    public JmsDestination getDestination() {
        return this.consumerInfo.getDestination();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener;
    }

    /**
     * @param listener
     * @throws JMSException
     * @see javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
     */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        if (consumerInfo.getPrefetchSize() == 0) {
            throw new JMSException("Illegal prefetch size of zero. This setting is not supported" +
                                   "for asynchronous consumers please set a value of at least 1");
        }
        this.messageListener = listener;
        drainMessageQueueToListener();
    }

    /**
     * @return the Message Selector
     * @throws JMSException
     * @see javax.jms.MessageConsumer#getMessageSelector()
     */
    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return this.consumerInfo.getSelector();
    }

    /**
     * Gets the configured prefetch size for this consumer.
     * @return the prefetch size configuration for this consumer.
     */
    public int getPrefetchSize() {
        return this.consumerInfo.getPrefetchSize();
    }

    protected void checkMessageListener() throws JMSException {
        session.checkMessageListener();
    }

    boolean hasMessageListener() {
        return this.messageListener != null;
    }

    boolean isUsingDestination(JmsDestination destination) {
        return this.consumerInfo.getDestination().equals(destination);
    }

    protected int getMessageQueueSize() {
        return this.messageQueue.size();
    }

    public boolean getNoLocal() throws IllegalStateException {
        return this.consumerInfo.isNoLocal();
    }

    public boolean isDurableSubscription() {
        return false;
    }

    public boolean isBrowser() {
        return false;
    }

    @Override
    public void setAvailableListener(JmsMessageAvailableListener availableListener) {
        this.availableListener = availableListener;
    }

    @Override
    public JmsMessageAvailableListener getAvailableListener() {
        return availableListener;
    }

    protected void onConnectionInterrupted() {
        messageQueue.clear();
    }

    protected void onConnectionRecovery(Provider provider) throws Exception {
        ProviderFuture request = new ProviderFuture();
        provider.create(consumerInfo, request);
        request.sync();
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {
        ProviderFuture request = new ProviderFuture();
        provider.start(consumerInfo, request);
        request.sync();
    }

    protected void onConnectionRestored() {
    }

    /**
     * Triggers a pull request from the connected Provider.  An attempt is made to set
     * a timeout on the pull request however some providers will not honor this value
     * and the pull will remain active until a message is dispatched.
     *
     * The timeout value can be one of:
     *
     *  < 0 to indicate that the request should expire immediately if no message.
     *  = 0 to indicate that the request should never time out.
     *  > 1 to indicate that the request should expire after the given time in milliseconds.
     *
     * @param timeout
     *        The amount of time the pull request should remain valid.
     */
    protected void sendPullCommand(long timeout) throws JMSException {
        if (messageQueue.isEmpty() && (getPrefetchSize() == 0 || isBrowser())) {
            connection.pull(getConsumerId(), timeout);
        }
    }

    private int getConfiguredPrefetch(JmsDestination destination, JmsPrefetchPolicy policy) {
        int prefetch = 0;
        if (destination.isTopic()) {
            if (isDurableSubscription()) {
                prefetch = policy.getDurableTopicPrefetch();
            } else {
                prefetch = policy.getTopicPrefetch();
            }
        } else {
            if (isBrowser()) {
                prefetch = policy.getQueueBrowserPrefetch();
            } else {
                prefetch = policy.getQueuePrefetch();
            }
        }

        return prefetch;
    }
}
