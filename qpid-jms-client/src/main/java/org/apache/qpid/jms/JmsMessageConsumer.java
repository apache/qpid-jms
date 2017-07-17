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

import static org.apache.qpid.jms.message.JmsMessageSupport.lookupAckTypeForDisposition;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.qpid.jms.exceptions.JmsConnectionFailedException;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.policy.JmsDeserializationPolicy;
import org.apache.qpid.jms.policy.JmsPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsRedeliveryPolicy;
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
public class JmsMessageConsumer implements AutoCloseable, MessageConsumer, JmsMessageAvailableConsumer, JmsMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumer.class);

    protected final JmsSession session;
    protected final JmsConnection connection;
    protected JmsConsumerInfo consumerInfo;
    protected final int acknowledgementMode;
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected volatile MessageListener messageListener;
    protected volatile JmsMessageAvailableListener availableListener;
    protected final MessageQueue messageQueue;
    protected final Lock lock = new ReentrantLock();
    protected final Lock dispatchLock = new ReentrantLock();
    protected final AtomicBoolean suspendedConnection = new AtomicBoolean();
    protected final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    protected final MessageDeliverTask deliveryTask = new MessageDeliverTask();

    protected JmsMessageConsumer(JmsConsumerId consumerId, JmsSession session, JmsDestination destination,
                                 String selector, boolean noLocal) throws JMSException {
        this(consumerId, session, destination, null, selector, noLocal);
    }

    protected JmsMessageConsumer(JmsConsumerId consumerId, JmsSession session, JmsDestination destination,
                                 String name, String selector, boolean noLocal) throws JMSException {
        this.session = session;
        this.connection = session.getConnection();
        this.acknowledgementMode = isBrowser() ? Session.AUTO_ACKNOWLEDGE : session.acknowledgementMode();

        if (destination.isTemporary()) {
            connection.checkConsumeFromTemporaryDestination((JmsTemporaryDestination) destination);
        }

        if (connection.isLocalMessagePriority()) {
            this.messageQueue = new PriorityMessageQueue();
        } else {
            this.messageQueue = new FifoMessageQueue();
        }

        JmsPrefetchPolicy prefetchPolicy = session.getPrefetchPolicy();
        JmsRedeliveryPolicy redeliveryPolicy = session.getRedeliveryPolicy().copy();
        JmsDeserializationPolicy deserializationPolicy = session.getDeserializationPolicy().copy();

        consumerInfo = new JmsConsumerInfo(consumerId, messageQueue);
        consumerInfo.setExplicitClientID(connection.isExplicitClientID());
        consumerInfo.setSelector(selector);
        consumerInfo.setDurable(isDurableSubscription());
        consumerInfo.setSubscriptionName(name);
        consumerInfo.setShared(isSharedSubscription());
        consumerInfo.setDestination(destination);
        consumerInfo.setAcknowledgementMode(acknowledgementMode);
        consumerInfo.setNoLocal(noLocal);
        consumerInfo.setBrowser(isBrowser());
        consumerInfo.setPrefetchSize(
            prefetchPolicy.getConfiguredPrefetch(session, destination, isDurableSubscription(), isBrowser()));
        consumerInfo.setRedeliveryPolicy(redeliveryPolicy);
        consumerInfo.setLocalMessageExpiry(connection.isLocalMessageExpiry());
        consumerInfo.setPresettle(session.getPresettlePolicy().isConsumerPresttled(session, destination));
        consumerInfo.setDeserializationPolicy(deserializationPolicy);

        session.add(this);
        try {
            session.getConnection().createResource(consumerInfo);
        } catch (JMSException jmse) {
            session.remove(this);
            throw jmse;
        }
    }

    public void init() throws JMSException {
        if (!isPullConsumer()){
            startConsumerResource();
        }
    }

    private void startConsumerResource() throws JMSException {
        try {
            session.getConnection().startResource(consumerInfo);
        } catch (JMSException ex) {
            session.remove(this);
            throw ex;
        }
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
     * @throws JMSException if an error occurs during the consumer close operation.
     */
    protected void doClose() throws JMSException {
        shutdown();
        try {
            this.connection.destroyResource(consumerInfo);
        } catch (JmsConnectionFailedException jmsex) {
        }
    }

    /**
     * Called to release all producer resources without requiring a destroy request
     * to be sent to the remote peer.  This is most commonly needed when the parent
     * Session is closing.
     *
     * @throws JMSException if an error occurs during shutdown.
     */
    protected void shutdown() throws JMSException {
        shutdown(null);
    }

    protected void shutdown(Throwable cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            consumerInfo.setState(ResourceState.CLOSED);
            setFailureCause(cause);
            session.remove(this);
            stop(true);
        }
    }

    @Override
    public Message receive() throws JMSException {
        return receive(0);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        checkMessageListener();

        // Configure for infinite wait when timeout is zero (JMS Spec)
        if (timeout == 0) {
            timeout = -1;
        }

        return copy(ackFromReceive(dequeue(timeout, connection.isReceiveLocalOnly())));
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        checkMessageListener();

        return copy(ackFromReceive(dequeue(0, connection.isReceiveNoWaitLocalOnly())));
    }

    /**
     * Reads the next available message for this consumer and returns the body of that message
     * if the type requested matches that of the message.  The amount of time this method blocks
     * is based on the timeout value.
     *
     *   {@literal timeout < 0} then it blocks until a message is received.
     *   {@literal timeout = 0} then it returns the body immediately or null if none available.
     *   {@literal timeout > 0} then it blocks up to timeout amount of time.
     *
     * @param desired
     *      The type to assign the body of the message to for return.
     * @param timeout
     *      The time to wait for an incoming message before this method returns null.
     *
     * @return the assigned body of the next available message or null if the consumer is closed
     *         or the specified timeout elapses.
     *
     * @throws MessageFormatException if the message body cannot be assigned to the requested type.
     * @throws JMSException if an error occurs while receiving the next message.
     */
    public <T> T receiveBody(Class<T> desired, long timeout) throws JMSException {
        checkClosed();
        checkMessageListener();

        T messageBody = null;
        JmsInboundMessageDispatch envelope = null;

        try {
            envelope = dequeue(timeout, connection.isReceiveLocalOnly());
            if (envelope != null) {
                messageBody = envelope.getMessage().getBody(desired);
            }
        } catch (MessageFormatException mfe) {
            // Should behave as if receiveBody never happened in these modes.
            if (acknowledgementMode == Session.AUTO_ACKNOWLEDGE ||
                acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE) {

                envelope.setEnqueueFirst(true);
                onInboundMessage(envelope);
                envelope = null;
            }

            throw mfe;
        } finally {
            if (envelope != null) {
                ackFromReceive(envelope);
            }
        }

        return messageBody;
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
     * @param localCheckOnly
     *          if false, try pulling a message if a >= 0 timeout expires with no message arriving
     *
     * @return null if we timeout or if the consumer is closed concurrently.
     *
     * @throws JMSException if an error occurs during the dequeue.
     */
    private JmsInboundMessageDispatch dequeue(long timeout, boolean localCheckOnly) throws JMSException {
        boolean pullConsumer = isPullConsumer();
        boolean pullForced = pullConsumer;

        try {
            long deadline = 0;
            if (timeout > 0) {
                deadline = System.currentTimeMillis() + timeout;
            }

            performPullIfRequired(timeout, false);

            while (true) {
                JmsInboundMessageDispatch envelope = null;
                if (pullForced || pullConsumer) {
                    // Any waiting was done by the pull request, try immediate retrieval from the queue.
                    envelope = messageQueue.dequeue(0);
                } else {
                    envelope = messageQueue.dequeue(timeout);
                }

                if (getFailureCause() != null) {
                    LOG.debug("{} receive failed: {}", getConsumerId(), getFailureCause().getMessage());
                    throw JmsExceptionSupport.create(getFailureCause());
                }

                if (envelope == null) {
                    if ((timeout == 0 && (pullForced || localCheckOnly)) || pullConsumer || messageQueue.isClosed()) {
                        return null;
                    } else if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }

                    if (timeout >= 0 && !localCheckOnly) {
                        // We don't do this for receive with no timeout since it
                        // is redundant: zero-prefetch consumers already pull, and
                        // the rest block indefinitely on the local messageQueue.
                        pullForced = true;
                        if (performPullIfRequired(timeout, true)) {
                            startConsumerResource();
                            // We refresh credit if it is a prefetching consumer, since the
                            // pull drained it. Processing acks can open the credit window, but
                            // not in all cases, and if we didn't get a message it would stay
                            // closed until future pulls were performed.
                        }
                    }
                } else if (consumeExpiredMessage(envelope)) {
                    LOG.trace("{} filtered expired message: {}", getConsumerId(), envelope);
                    doAckExpired(envelope);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                    performPullIfRequired(timeout, false);
                } else if (redeliveryExceeded(envelope)) {
                    LOG.debug("{} filtered message with excessive redelivery count: {}", getConsumerId(), envelope);
                    applyRedeliveryPolicyOutcome(envelope);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                    performPullIfRequired(timeout, false);
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
               redeliveryPolicy.getMaxRedeliveries(getDestination()) >= 0 &&
               redeliveryPolicy.getMaxRedeliveries(getDestination()) < envelope.getRedeliveryCount();
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException jmsEx = null;

            if (getFailureCause() == null) {
                jmsEx = new IllegalStateException("The MessageConsumer is closed");
            } else {
                jmsEx = new IllegalStateException("The MessageConsumer was closed due to an unrecoverable error.");
                jmsEx.initCause(getFailureCause());
            }

            throw jmsEx;
        }
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
        }
        return envelope;
    }

    private JmsInboundMessageDispatch doAckConsumed(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            session.acknowledge(envelope, ACK_TYPE.ACCEPTED);
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
            session.acknowledge(envelope, ACK_TYPE.MODIFIED_FAILED_UNDELIVERABLE);
        } catch (JMSException ex) {
            session.onException(ex);
            throw ex;
        }
    }

    private void applyRedeliveryPolicyOutcome(final JmsInboundMessageDispatch envelope) throws JMSException {
        try {
            JmsRedeliveryPolicy redeliveryPolicy = consumerInfo.getRedeliveryPolicy();
            session.acknowledge(envelope, lookupAckTypeForDisposition(redeliveryPolicy.getOutcome(getDestination())));
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
                envelope.getMessage().setAcknowledgeCallback(new JmsAcknowledgeCallback(session));
            }

            if (envelope.isEnqueueFirst()) {
                this.messageQueue.enqueueFirst(envelope);
            } else {
                this.messageQueue.enqueue(envelope);
            }

            if (session.isStarted() && messageQueue.isRunning()) {
                if (messageListener != null) {
                    session.getDispatcherExecutor().execute(deliveryTask);
                } else if (availableListener != null) {
                    session.getDispatcherExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            if (messageQueue.isRunning()) {
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
            if (!messageQueue.isRunning()) {
                this.messageQueue.start();
                drainMessageQueueToListener();
            }
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        stop(false);
    }

    private void stop(boolean closeMessageQueue) {
        dispatchLock.lock();
        lock.lock();
        try {
            if (closeMessageQueue) {
                this.messageQueue.close();
            } else {
                this.messageQueue.stop();
            }
        } finally {
            lock.unlock();
            dispatchLock.unlock();
        }
    }

    void suspendForRollback() throws JMSException {
        stop();

        try {
            session.getConnection().stopResource(consumerInfo);
        } finally {
            if (session.getTransactionContext().isActiveInThisContext(getConsumerId())) {
                messageQueue.clear();
            }
        }
    }

    void resumeAfterRollback() throws JMSException {
        start();
        startConsumerResource();
    }

    /**
     * @return the id
     */
    public JmsConsumerId getConsumerId() {
        return this.consumerInfo.getId();
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

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();

        dispatchLock.lock();
        try {
            messageListener = listener;
            consumerInfo.setListener(listener != null);

            if (listener != null) {
                if (isPullConsumer()){
                    startConsumerResource();
                }
                drainMessageQueueToListener();
            }
        } finally {
            dispatchLock.unlock();
        }
    }

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

    public boolean isSharedSubscription() {
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
        if (consumerInfo.isOpen()) {
            ProviderFuture request = new ProviderFuture();
            provider.create(consumerInfo, request);
            request.sync();
        }
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {
        if (consumerInfo.isOpen()) {
            ProviderFuture request = new ProviderFuture();
            provider.start(consumerInfo, request);
            request.sync();
        }
    }

    protected void onConnectionRestored() {
    }

    /**
     * Triggers a pull request from the connected Provider with the given timeout value
     * if the consumer is a pull consumer or requested to be treated as one, and the
     * local queue is still running, and is currently empty.
     * <p>
     * The timeout value can be one of:
     * <br>
     * {@literal < 0} to indicate that the request should never time out.<br>
     * {@literal = 0} to indicate that the request should expire immediately if no message.<br>
     * {@literal > 0} to indicate that the request should expire after the given time in milliseconds.
     *
     * @param timeout
     *        The amount of time the pull request should remain valid.
     * @param treatAsPullConsumer
     *        Treat the consumer as if it were a pull consumer, even if it isn't.
     * @return true if a pull was performed, false if it was not.
     */
    protected boolean performPullIfRequired(long timeout, boolean treatAsPullConsumer) throws JMSException {
        if ((isPullConsumer() || treatAsPullConsumer) && messageQueue.isRunning() && messageQueue.isEmpty()) {
            connection.pull(getConsumerId(), timeout);
            return true;
        }

        return false;
    }

    private void drainMessageQueueToListener() {
        if (messageListener != null && session.isStarted() && messageQueue.isRunning()) {
            session.getDispatcherExecutor().execute(new BoundedMessageDeliverTask(messageQueue.size()));
        }
    }

    private boolean deliverNextPending() {
        if (session.isStarted() && messageQueue.isRunning() && messageListener != null) {
            dispatchLock.lock();
            try {
                JmsInboundMessageDispatch envelope = messageQueue.dequeueNoWait();
                if (envelope == null) {
                    return false;
                }

                JmsMessage copy = null;

                if (consumeExpiredMessage(envelope)) {
                    LOG.trace("{} filtered expired message: {}", getConsumerId(), envelope);
                    doAckExpired(envelope);
                } else if (redeliveryExceeded(envelope)) {
                    LOG.trace("{} filtered message with excessive redelivery count: {}", getConsumerId(), envelope);
                    applyRedeliveryPolicyOutcome(envelope);
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
                // TODO - There are two cases where we can get an error here, one being
                //        and error returned from the attempted ACK that was sent and the
                //        other being an error while attempting to copy the incoming message.
                //        We need to decide how to respond to these.
                session.getConnection().onException(e);
            } finally {
                dispatchLock.unlock();

                if (isPullConsumer()) {
                    try {
                        startConsumerResource();
                    } catch (JMSException e) {
                        LOG.error("Exception during credit replenishment for consumer listener {}", getConsumerId(), e);
                    }
                }
            }
        }

        return !messageQueue.isEmpty();
    }

    private final class BoundedMessageDeliverTask implements Runnable {

        private final int deliveryCount;

        public BoundedMessageDeliverTask(int deliveryCount) {
            this.deliveryCount = deliveryCount;
        }

        @Override
        public void run() {
            int current = 0;

            while (session.isStarted() && messageQueue.isRunning() && current++ < deliveryCount) {
                if (!deliverNextPending()) {
                    return;  // Another task already drained the queue.
                }
            }
        }
    }

    private final class MessageDeliverTask implements Runnable {

        @Override
        public void run() {
            deliverNextPending();
        }
    }
}
