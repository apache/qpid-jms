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
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.util.FifoMessageQueue;
import org.apache.qpid.jms.util.MessageQueue;
import org.apache.qpid.jms.util.PriorityMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * implementation of a JMS Message Consumer
 */
public class JmsMessageConsumer implements MessageConsumer, JmsMessageAvailableConsumer, JmsMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumer.class);

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
    protected Exception failureCause;

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

        if(destination.isTemporary()) {
            connection.checkConsumeFromTemporaryDestination((JmsTemporaryDestination) destination);
        }

        if (connection.isLocalMessagePriority()) {
            this.messageQueue = new PriorityMessageQueue();
        } else {
            this.messageQueue = new FifoMessageQueue();
        }

        JmsPrefetchPolicy policy = connection.getPrefetchPolicy();
        JmsRedeliveryPolicy redeliveryPolicy = connection.getRedeliveryPolicy().copy();

        consumerInfo = new JmsConsumerInfo(consumerId);
        consumerInfo.setClientId(connection.getClientID());
        consumerInfo.setSelector(selector);
        consumerInfo.setSubscriptionName(name);
        consumerInfo.setDestination(destination);
        consumerInfo.setAcknowledgementMode(acknowledgementMode);
        consumerInfo.setNoLocal(noLocal);
        consumerInfo.setBrowser(isBrowser());
        consumerInfo.setPrefetchSize(getConfiguredPrefetch(destination, policy));
        consumerInfo.setRedeliveryPolicy(redeliveryPolicy);
        consumerInfo.setLocalMessageExpiry(connection.isLocalMessageExpiry());

        session.getConnection().createResource(consumerInfo);
    }

    public void init() throws JMSException {
        session.add(this);
        startConsumerResource();
    }

    private void startConsumerResource() throws JMSException {
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
            session.getTransactionContext().addSynchronization(new JmsTxSynchronization() {

                @Override
                public boolean validate(JmsTransactionContext context) throws Exception {
                    if (!context.isInTransaction() || !delivered.get() || isBrowser()) {
                        doClose();
                        return false;
                    }

                    return true;
                }

                @Override
                public void afterCommit() throws Exception {
                    doClose();
                }

                @Override
                public void afterRollback() throws Exception {
                    doClose();
                }
            });
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
        shutdown(null);
    }

    protected void shutdown(Exception cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            failureCause = cause;
            session.remove(this);
            stop(true);
        }
    }

    /**
     * @return a Message or null if closed during the operation
     * @throws JMSException
     * @see javax.jms.MessageConsumer#receive()
     */
    @Override
    public Message receive() throws JMSException {
        return receive(0);
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

        // Configure for infinite wait when timeout is zero (JMS Spec)
        if (timeout == 0) {
            timeout = -1;
        }

        return copy(ackFromReceive(dequeue(timeout)));
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
        return copy(ackFromReceive(dequeue(0)));
    }

    /**
     * Used to get an enqueued message from the unconsumedMessages list. The
     * amount of time this method blocks is based on the timeout value.
     *
     *   timeout < 0 then it blocks until a message is received.
     *   timeout = 0 then it returns a message or null if none available
     *   timeout > 0 then it blocks up to timeout amount of time.
     *
     * This method may consume messages that are expired or exceed a configured
     * delivery count value but will continue to wait for the configured timeout.
     *
     * @throws JMSException
     * @return null if we timeout or if the consumer is closed.
     */
    private JmsInboundMessageDispatch dequeue(long timeout) throws JMSException {

        try {
            long deadline = 0;
            if (timeout > 0) {
                deadline = System.currentTimeMillis() + timeout;
            }

            sendPullCommand(timeout);

            while (true) {
                JmsInboundMessageDispatch envelope = null;
                if (isPullConsumer()) {
                    envelope = messageQueue.dequeue(-1);
                } else {
                    envelope = messageQueue.dequeue(timeout);
                }

                if (envelope == null) {

                    if (failureCause != null && !messageQueue.isClosed() && timeout == 0) {
                        LOG.debug("{} receive failed: {}", getConsumerId(), failureCause.getMessage());
                        throw JmsExceptionSupport.create(failureCause);
                    }

                    if (timeout == 0 || messageQueue.isClosed()) {
                        return null;
                    } else if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }

                    sendPullCommand(timeout);
                } else if (envelope.getMessage() == null) {
                    LOG.trace("{} no message was available for this consumer: {}", getConsumerId());
                    return null;
                } else if (consumeExpiredMessage(envelope)) {
                    LOG.trace("{} filtered expired message: {}", getConsumerId(), envelope);
                    doAckExpired(envelope);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                    sendPullCommand(timeout);
                } else if (redeliveryExceeded(envelope)) {
                    LOG.debug("{} filtered message with excessive redelivery count: {}", getConsumerId(), envelope);
                    doAckUndeliverable(envelope);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                    sendPullCommand(timeout);
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(getConsumerId() + " received message: " + envelope);
                    }
                    return envelope;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JmsExceptionSupport.create(e);
        }
    }

    private boolean consumeExpiredMessage(JmsInboundMessageDispatch dispatch) {
        if (!isBrowser() && consumerInfo.isLocalMessageExpiry() && dispatch.getMessage().isExpired()) {
            return true;
        }

        return false;
    }

    protected boolean redeliveryExceeded(JmsInboundMessageDispatch envelope) {
        LOG.trace("checking envelope with {} redeliveries", envelope.getRedeliveryCount());

        JmsRedeliveryPolicy redeliveryPolicy = consumerInfo.getRedeliveryPolicy();
        return redeliveryPolicy != null &&
               redeliveryPolicy.getMaxRedeliveries() != JmsRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES &&
               redeliveryPolicy.getMaxRedeliveries() < envelope.getRedeliveryCount();
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException jmsEx = null;

            if (failureCause == null) {
                jmsEx = new IllegalStateException("The MessageConsumer is closed");
            } else {
                jmsEx = new IllegalStateException("The MessageConsumer was closed due to an unrecoverable error.");
                jmsEx.initCause(failureCause);
            }

            throw jmsEx;
        }
    }

    JmsMessage copy(final JmsInboundMessageDispatch envelope) throws JMSException {
        if (envelope == null || envelope.getMessage() == null) {
            return null;
        }
        return envelope.getMessage().copy();
    }

    JmsInboundMessageDispatch ackFromReceive(final JmsInboundMessageDispatch envelope) throws JMSException {
        if (envelope != null && envelope.getMessage() != null) {
            JmsMessage message = envelope.getMessage();
            if (message.getAcknowledgeCallback() != null) {
                // Message has been received by the app.. expand the credit
                // window so that we receive more messages.
                doAckDelivered(envelope);
            } else {
                doAckConsumed(envelope);
            }

            // Tags that we have delivered and can't close if in a TX Session.
            delivered.set(true);
        }
        return envelope;
    }

    private JmsInboundMessageDispatch doAckConsumed(final JmsInboundMessageDispatch envelope) throws JMSException {
        checkClosed();
        try {
            session.acknowledge(envelope, ACK_TYPE.CONSUMED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
        return envelope;
    }

    private JmsInboundMessageDispatch doAckDelivered(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            session.acknowledge(envelope, ACK_TYPE.DELIVERED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
        return envelope;
    }

    private void doAckExpired(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            session.acknowledge(envelope, ACK_TYPE.EXPIRED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
    }

    private void doAckUndeliverable(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            session.acknowledge(envelope, ACK_TYPE.POISONED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
    }

    private void doAckReleased(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            session.acknowledge(envelope, ACK_TYPE.RELEASED);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
    }

    /**
     * Called from the session when a new Message has been dispatched to this Consumer
     * from the connection.
     *
     * @param envelope
     *        the newly arrived message.
     */
    @Override
    public void onInboundMessage(final JmsInboundMessageDispatch envelope) {
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
                        envelope.getMessage().setAcknowledgeCallback(null);
                        return null;
                    }
                });
            }

            if (envelope.isEnqueueFirst()) {
                this.messageQueue.enqueueFirst(envelope);
            } else {
                this.messageQueue.enqueue(envelope);
            }

            if (this.messageListener != null && this.started) {
                session.getExecutor().execute(new MessageDeliverTask());
            } else {
                if (availableListener != null) {
                    session.getExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            if (session.isStarted()) {
                                availableListener.onMessageAvailable(JmsMessageConsumer.this);
                            }
                        }
                    });
                }
            }
        } finally {
            lock.unlock();
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
        stop(false);
    }

    private void stop(boolean closeMessageQueue) {
        lock.lock();
        try {
            this.started = false;
            if (closeMessageQueue) {
                this.messageQueue.close();
            } else {
                this.messageQueue.stop();
            }
        } finally {
            lock.unlock();
        }
    }

    void suspendForRollback() throws JMSException {
        stop();

        session.getConnection().stopResource(consumerInfo);
    }

    void resumeAfterRollback() throws JMSException {
        if (!this.messageQueue.isEmpty()) {
            List<JmsInboundMessageDispatch> drain = this.messageQueue.removeAll();
            for (JmsInboundMessageDispatch envelope : drain) {
                doAckReleased(envelope);
            }
            drain.clear();
        }

        start();

        startConsumerResource();
    }

    void drainMessageQueueToListener() {
        if (this.messageListener != null && this.started) {
            session.getExecutor().execute(new MessageDeliverTask());
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

    protected boolean isNoLocal() {
        return this.consumerInfo.isNoLocal();
    }

    public boolean isDurableSubscription() {
        return false;
    }

    public boolean isBrowser() {
        return false;
    }

    public boolean isPullConsumer() {
        return getPrefetchSize() == 0;
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

        // For a pull consumer we want to stop the Queue waking an blocked receives and
        // preventing a new one from blocking until it has reissued a new pull.
        if (isPullConsumer()) {
            lock.lock();
            try {
                messageQueue.stop();
            } finally {
                lock.unlock();
            }
        }
    }

    protected void onConnectionRestored() {
    }

    /**
     * Triggers a pull request from the connected Provider with the given timeout value.
     * <p>
     * The timeout value can be one of:
     * <br>
     * {@literal < 0} to indicate that the request should never time out.<br>
     * {@literal = 0} to indicate that the request should expire immediately if no message.<br>
     * {@literal > 0} to indicate that the request should expire after the given time in milliseconds.
     *
     * @param timeout
     *        The amount of time the pull request should remain valid.
     */
    protected void sendPullCommand(long timeout) throws JMSException {
        if (!messageQueue.isClosed() && messageQueue.isEmpty() && isPullConsumer()) {
            connection.pull(getConsumerId(), timeout);

            // Once a new pull has gone out check to see if the queue was stopped due to failover
            // and restart it so that the receive calls can once again start blocking on dequeue.
            lock.lock();
            try {
                if (started) {
                    messageQueue.start();
                }
            } finally {
                lock.unlock();
            }
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

    private final class MessageDeliverTask implements Runnable {
        @Override
        public void run() {
            JmsInboundMessageDispatch envelope;
            while (session.isStarted() && (envelope = messageQueue.dequeueNoWait()) != null) {
                try {
                    JmsMessage copy = null;

                    if (consumeExpiredMessage(envelope)) {
                        LOG.trace("{} filtered expired message: {}", getConsumerId(), envelope);
                        doAckExpired(envelope);
                    } else if (redeliveryExceeded(envelope)) {
                        LOG.trace("{} filtered message with excessive redelivery count: {}", getConsumerId(), envelope);
                        doAckUndeliverable(envelope);
                    } else {
                        boolean deliveryFailed = false;
                        boolean autoAckOrDupsOk = acknowledgementMode == Session.AUTO_ACKNOWLEDGE ||
                                                  acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE;
                        if (autoAckOrDupsOk) {
                            copy = copy(doAckDelivered(envelope));
                        } else {
                            copy = copy(ackFromReceive(envelope));
                        }
                        session.clearSessionRecovered();

                        try {
                            messageListener.onMessage(copy);
                        } catch (RuntimeException rte) {
                            deliveryFailed = true;
                        }

                        if (autoAckOrDupsOk && !session.isSessionRecovered()) {
                            if (!deliveryFailed) {
                                doAckConsumed(envelope);
                            } else {
                                doAckReleased(envelope);
                            }
                        }
                    }
                } catch (Exception e) {
                    // TODO - We need to handle exception of on message with some other
                    //        ack such as rejected and consider adding a redlivery policy
                    //        to control when we might just poison the message with an ack
                    //        of modified set to not deliverable here.
                    session.getConnection().onException(e);
                }
            }
        }
    }
}
