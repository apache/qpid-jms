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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.exceptions.JmsConnectionFailedException;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageTransformation;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.policy.JmsDeserializationPolicy;
import org.apache.qpid.jms.policy.JmsMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsPresettlePolicy;
import org.apache.qpid.jms.policy.JmsRedeliveryPolicy;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.apache.qpid.jms.selector.SelectorParser;
import org.apache.qpid.jms.selector.filter.FilterException;
import org.apache.qpid.jms.util.NoOpExecutor;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS Session implementation
 */
public class JmsSession implements AutoCloseable, Session, QueueSession, TopicSession, JmsMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JmsSession.class);

    private static final int INDIVIDUAL_ACKNOWLEDGE = 101;
    private static final int ARTEMIS_PRE_ACKNOWLEDGE = 100;
    private static final int NO_ACKNOWLEDGE = 257;

    private final JmsConnection connection;
    private final int acknowledgementMode;
    private final Map<JmsProducerId, JmsMessageProducer> producers = new ConcurrentHashMap<JmsProducerId, JmsMessageProducer>();
    private final Map<JmsConsumerId, JmsMessageConsumer> consumers = new ConcurrentHashMap<JmsConsumerId, JmsMessageConsumer>();
    private MessageListener messageListener;

    private final java.util.Queue<Consumer<JmsSession>> sessionQueue = new ArrayDeque<>();

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final JmsSessionInfo sessionInfo;
    private final ReentrantLock sendLock = new ReentrantLock();
    private volatile ThreadPoolExecutor deliveryExecutor;
    private volatile ThreadPoolExecutor completionExcecutor;
    private volatile ProviderFuture asyncSendsCompletion;
    private AtomicReference<Thread> deliveryThread = new AtomicReference<Thread>();
    private boolean deliveryThreadCheckEnabled = true;
    private AtomicReference<Thread> completionThread = new AtomicReference<Thread>();

    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong producerIdGenerator = new AtomicLong();
    private JmsTransactionContext transactionContext;
    private boolean sessionRecovered;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    private final Deque<SendCompletion> asyncSendQueue = new ConcurrentLinkedDeque<SendCompletion>();

    protected JmsSession(JmsConnection connection, JmsSessionId sessionId, int acknowledgementMode) throws JMSException {
        this.connection = connection;
        this.acknowledgementMode = acknowledgementMode;

        if (acknowledgementMode == SESSION_TRANSACTED) {
            setTransactionContext(new JmsLocalTransactionContext(this));
        } else {
            setTransactionContext(new JmsNoTxTransactionContext());
        }

        sessionInfo = new JmsSessionInfo(sessionId);
        sessionInfo.setAcknowledgementMode(acknowledgementMode);
        sessionInfo.setSendAcksAsync(connection.isForceAsyncAcks());
        sessionInfo.setMessageIDPolicy(connection.getMessageIDPolicy().copy());
        sessionInfo.setPrefetchPolicy(connection.getPrefetchPolicy().copy());
        sessionInfo.setPresettlePolicy(connection.getPresettlePolicy().copy());
        sessionInfo.setRedeliveryPolicy(connection.getRedeliveryPolicy().copy());
        sessionInfo.setDeserializationPolicy(connection.getDeserializationPolicy());

        connection.createResource(sessionInfo, new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
                connection.addSession(sessionInfo, JmsSession.this);
            }

            @Override
            public void onPendingFailure(ProviderException cause) {
            }
        });

        // We always keep an open TX if transacted so start now.
        try {
            getTransactionContext().begin();
        } catch (Exception e) {
            // failed, close the AMQP session before we throw
            try {
                connection.destroyResource(sessionInfo, new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        connection.removeSession(sessionInfo);
                    }

                    @Override
                    public void onPendingFailure(ProviderException cause) {
                        connection.removeSession(sessionInfo);
                    }
                });
            } catch (Exception ex) {
                // Ignore, throw original error
            }

            throw e;
        }
    }

    int acknowledgementMode() {
        return acknowledgementMode;
    }

    //////////////////////////////////////////////////////////////////////////
    // Session methods
    //////////////////////////////////////////////////////////////////////////

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkClosed();
        return acknowledgementMode;
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return isTransacted();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        if (listener != null) {
            checkClosed();
        }

        this.messageListener = listener;
    }

    @Override
    public void recover() throws JMSException {
        checkClosed();
        if (getTransacted()) {
            throw new javax.jms.IllegalStateException("Cannot call recover() on a transacted session");
        }

        boolean wasStarted = isStarted();
        stop();

        connection.recover(getSessionId());
        sessionRecovered = true;

        if (wasStarted) {
            start();
        }
    }

    @Override
    public void commit() throws JMSException {
        checkClosed();
        checkIsCompletionThread();

        if (!getTransacted()) {
            throw new javax.jms.IllegalStateException("Not a transacted session");
        }

        transactionContext.commit();
    }

    @Override
    public void rollback() throws JMSException {
        checkClosed();
        checkIsCompletionThread();

        if (!getTransacted()) {
            throw new javax.jms.IllegalStateException("Not a transacted session");
        }

        // Stop processing any new messages that arrive
        try {
            for (JmsMessageConsumer c : consumers.values()) {
                c.suspendForRollback();
            }
        } finally {
            transactionContext.rollback();
        }

        // Currently some consumers won't get suspended and some won't restart
        // after a failed rollback.
        for (JmsMessageConsumer c : consumers.values()) {
            c.resumeAfterRollback();
        }
    }

    @Override
    public void close() throws JMSException {
        checkIsDeliveryThread();
        checkIsCompletionThread();

        if (!closed.get()) {
            doClose();
        }
    }

    /**
     * Shutdown the Session and release all resources.  Once completed the Session can
     * request that the Provider destroy the Session and it's child resources.
     *
     * @throws JMSException if an internal error occurs during the close operation.
     */
    protected void doClose() throws JMSException {
        boolean interrupted = Thread.interrupted();
        shutdown();
        try {
            connection.destroyResource(sessionInfo);
        } catch (JmsConnectionFailedException jmsex) {
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method should terminate all Session resources and prepare for disposal of the
     * Session.  It is called either from the Session close method or from the Connection
     * when a close request is made and the Connection wants to cleanup all Session resources.
     *
     * This method should not attempt to send any requests to the Provider as the resources
     * that were associated with this session are either cleaned up by another method in the
     * session or are already gone due to remote close or some other error.
     *
     * @throws JMSException if an error occurs while shutting down the session resources.
     */
    protected void shutdown() throws JMSException {
        shutdown(null);
    }

    protected boolean shutdown(Throwable cause) throws JMSException {
        boolean listenerPresent = false;

        if (closed.compareAndSet(false, true)) {
            JMSException shutdownError = null;

            try {
                sessionInfo.setState(ResourceState.CLOSED);
                setFailureCause(cause);

                try {
                    stop();

                    for (JmsMessageConsumer consumer : new ArrayList<JmsMessageConsumer>(this.consumers.values())) {
                        if (consumer.hasMessageListener()) {
                            listenerPresent = true;
                        }

                        consumer.shutdown(cause);
                    }

                    for (JmsMessageProducer producer : new ArrayList<JmsMessageProducer>(this.producers.values())) {
                        producer.shutdown(cause);
                    }

                } catch (JMSException jmsEx) {
                    shutdownError = jmsEx;
                }

                boolean inDoubt = transactionContext.isInDoubt();
                try {
                    transactionContext.shutdown();
                } catch (JMSException jmsEx) {
                    if (!inDoubt) {
                        LOG.warn("Rollback of active transaction failed due to error: ", jmsEx);
                        shutdownError = shutdownError == null ? jmsEx : shutdownError;
                    } else {
                        LOG.trace("Rollback of indoubt transaction failed due to error: ", jmsEx);
                    }
                }

                try {
                    if (getSessionMode() == Session.CLIENT_ACKNOWLEDGE) {
                        acknowledge(ACK_TYPE.SESSION_SHUTDOWN);
                    }
                } catch (Exception e) {
                    LOG.trace("Exception during session shutdown cleanup acknowledgement", e);
                }

                // Ensure that no asynchronous completion sends remain blocked after close but wait
                // using the close timeout for the asynchronous sends to complete normally.
                final ExecutorService completionExecutor = getCompletionExecutor();
                try {
                    synchronized (sessionInfo) {
                        // Producers are now quiesced and we can await completion of asynchronous sends
                        // that are still pending a result or timeout once we've done a quick check to
                        // see if any are actually pending or have completed already.
                        asyncSendsCompletion = connection.newProviderFuture();

                        if (asyncSendsCompletion != null) {
                            completionExecutor.execute(() -> {
                                if (asyncSendQueue.isEmpty()) {
                                    asyncSendsCompletion.onSuccess();
                                }
                            });
                        }
                    }

                    try {
                        if (asyncSendsCompletion != null) {
                            asyncSendsCompletion.sync(connection.getCloseTimeout(), TimeUnit.MILLISECONDS);
                        }
                    } catch (Exception ex) {
                        LOG.trace("Exception during wait for asynchronous sends to complete", ex);
                    } finally {
                        if (cause == null) {
                            cause = new JMSException("Session closed remotely before message transfer result was notified");
                        }

                        // as a last task we want to fail any stragglers in the asynchronous send queue and then
                        // shutdown the queue to prevent any more submissions while the cleanup goes on.
                        completionExecutor.execute(new FailOrCompleteAsyncCompletionsTask(JmsExceptionSupport.create(cause)));
                    }
                } finally {
                    completionExecutor.shutdown();

                    try {
                        completionExecutor.awaitTermination(connection.getCloseTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOG.trace("Session close awaiting send completions was interrupted");
                    }
                }

                if (shutdownError != null) {
                    throw shutdownError;
                }
            } catch (Throwable e) {
                if (shutdownError != null) {
                    throw shutdownError;
                } else {
                    throw JmsExceptionSupport.create(e);
                }
            } finally {
                connection.removeSession(sessionInfo);
            }
        }

        return listenerPresent;
    }

    //----- Events fired when resource remotely closed due to some error -----//

    void sessionClosed(Throwable cause) {
        try {
            boolean listenerPresent = shutdown(cause);
            if (listenerPresent) {
                connection.onAsyncException(JmsExceptionSupport.create(cause));
            }
        } catch (Throwable error) {
            LOG.trace("Ignoring exception thrown during cleanup of closed session", error);
        }
    }

    JmsMessageConsumer consumerClosed(JmsConsumerInfo resource, Throwable cause) {
        LOG.info("A JMS MessageConsumer has been closed: {}", resource);

        JmsMessageConsumer consumer = consumers.get(resource.getId());
        try {
            if (consumer != null) {
                if (consumer.hasMessageListener()) {
                    connection.onAsyncException(JmsExceptionSupport.create(cause));
                }

                consumer.shutdown(cause);
            }
        } catch (Throwable error) {
            LOG.trace("Ignoring exception thrown during cleanup of closed consumer", error);
        }

        return consumer;
    }

    JmsMessageProducer producerClosed(JmsProducerInfo resource, Throwable cause) {
        LOG.info("A JMS MessageProducer has been closed: {}", resource);

        JmsMessageProducer producer = producers.get(resource.getId());

        try {
            if (producer != null) {
                getCompletionExecutor().execute(new FailOrCompleteAsyncCompletionsTask(
                    producer.getProducerId(), JmsExceptionSupport.create(cause)));
                producer.shutdown(cause);
            }
        } catch (Throwable error) {
            LOG.trace("Ignoring exception thrown during cleanup of closed producer", error);
        }

        return producer;
    }

    //////////////////////////////////////////////////////////////////////////
    // Consumer creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(destination);
        messageSelector = checkSelector(messageSelector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsMessageConsumer result = new JmsMessageConsumer(getNextConsumerId(), this, dest, messageSelector, noLocal);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        checkClosed();
        checkDestination(queue);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, queue);
        JmsQueueReceiver result = new JmsQueueReceiver(getNextConsumerId(), this, dest, null);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(queue);
        messageSelector = checkSelector(messageSelector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, queue);
        JmsQueueReceiver result = new JmsQueueReceiver(getNextConsumerId(), this, dest, messageSelector);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.Session#createBrowser(javax.jms.Queue)
     */
    @Override
    public QueueBrowser createBrowser(Queue destination) throws JMSException {
        return createBrowser(destination, null);
    }

    /**
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueBrowser createBrowser(Queue destination, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(destination);
        messageSelector = checkSelector(messageSelector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsQueueBrowser result = new JmsQueueBrowser(this, dest, messageSelector);
        return result;
    }

    /**
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    /**
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        messageSelector = checkSelector(messageSelector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicSubscriber result = new JmsTopicSubscriber(getNextConsumerId(), this, dest, noLocal, messageSelector);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    /**
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        checkClientIDWasSetExplicitly();
        messageSelector = checkSelector(messageSelector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicSubscriber result = new JmsDurableTopicSubscriber(getNextConsumerId(), this, dest, name, noLocal, messageSelector);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.Session#createDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    /**
     * @see javax.jms.Session#createDurableConsumer(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        return createDurableSubscriber(topic, name, messageSelector, noLocal);
    }

    protected void checkClientIDWasSetExplicitly() throws IllegalStateException {
        if (!connection.isExplicitClientID()) {
            throw new IllegalStateException("You must specify a unique clientID for the Connection to use a DurableSubscriber");
        }
    }

    /**
     * @see javax.jms.Session#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
        connection.unsubscribe(name);
    }

    /**
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        return createSharedConsumer(topic, name, null);
    }

    /**
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        checkDestination(topic);
        selector = checkSelector(selector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsMessageConsumer result = new JmsSharedMessageConsumer(getNextConsumerId(), this, dest, name, selector);
        result.init();
        return result;
    }

    /**
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        return createSharedDurableConsumer(topic, name, null);
    }

    /**
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        checkDestination(topic);
        selector = checkSelector(selector, connection.isValidateSelector());
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsMessageConsumer result = new JmsSharedDurableMessageConsumer(getNextConsumerId(), this, dest, name, selector);
        result.init();
        return result;
    }

    //////////////////////////////////////////////////////////////////////////
    // Producer creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @see javax.jms.Session#createProducer(javax.jms.Destination)
     */
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsMessageProducer result = new JmsMessageProducer(getNextProducerId(), this, dest);
        return result;
    }

    /**
     * @see javax.jms.QueueSession#createSender(javax.jms.Queue)
     */
    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        checkClosed();
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, queue);
        JmsQueueSender result = new JmsQueueSender(getNextProducerId(), this, dest);
        return result;
    }

    /**
     * @see javax.jms.TopicSession#createPublisher(javax.jms.Topic)
     */
    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        checkClosed();
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicPublisher result = new JmsTopicPublisher(getNextProducerId(), this, dest);
        return result;
    }

    //////////////////////////////////////////////////////////////////////////
    // Message creation
    //////////////////////////////////////////////////////////////////////////

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createBytesMessage());
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createMapMessage());
    }

    @Override
    public Message createMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createMessage());
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createObjectMessage(null));
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createObjectMessage(object));
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createStreamMessage());
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createTextMessage(null));
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        return init(connection.getMessageFactory().createTextMessage(text));
    }

    //////////////////////////////////////////////////////////////////////////
    // Destination creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @see javax.jms.Session#createQueue(java.lang.String)
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return new JmsQueue(queueName);
    }

    /**
     * @see javax.jms.Session#createTopic(java.lang.String)
     */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        checkClosed();
        return new JmsTopic(topicName);
    }

    /**
     * @see javax.jms.Session#createTemporaryQueue()
     */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkClosed();
        return connection.createTemporaryQueue();
    }

    /**
     * @see javax.jms.Session#createTemporaryTopic()
     */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkClosed();
        return connection.createTemporaryTopic();
    }

    //----- Session dispatch support -----------------------------------------//

    @Override
    public void run() {
        try {
            checkClosed();
        } catch (IllegalStateException ex) {
            throw new RuntimeException(ex);
        }

        Consumer<JmsSession> dispatcher = null;
        while ((dispatcher = sessionQueue.poll()) != null) {
            dispatcher.accept(this);
        }
    }

    //----- Session Implementation methods -----------------------------------//

    protected void add(JmsMessageConsumer consumer) {
        consumers.put(consumer.getConsumerId(), consumer);
    }

    protected void remove(JmsMessageConsumer consumer) {
        consumers.remove(consumer.getConsumerId());
    }

    protected JmsMessageConsumer lookup(JmsConsumerId consumerId) {
        return consumers.get(consumerId);
    }

    protected void add(JmsMessageProducer producer) {
        producers.put(producer.getProducerId(), producer);
    }

    protected void remove(JmsMessageProducer producer) {
        producers.remove(producer.getProducerId());
    }

    protected JmsMessageProducer lookup(JmsProducerId producerId) {
        return producers.get(producerId);
    }

    protected void onException(Exception ex) {
        connection.onException(ex);
    }

    protected void send(JmsMessageProducer producer, Destination dest, Message msg, int deliveryMode, int priority, long timeToLive, boolean disableMsgId, boolean disableTimestamp, long deliveryDelay, CompletionListener listener) throws JMSException {
        if (dest == null) {
            throw new InvalidDestinationException("Destination must not be null");
        }

        if (msg == null) {
            throw new MessageFormatException("Message must not be null");
        }

        JmsDestination destination = JmsMessageTransformation.transformDestination(connection, dest);

        if (destination.isTemporary() && ((JmsTemporaryDestination) destination).isDeleted()) {
            throw new IllegalStateException("Temporary destination has been deleted");
        }

        send(producer, destination, msg, deliveryMode, priority, timeToLive, disableMsgId, disableTimestamp, deliveryDelay, listener);
    }

    private void send(JmsMessageProducer producer, JmsDestination destination, Message original, int deliveryMode, int priority, long timeToLive, boolean disableMsgId, boolean disableTimestamp, long deliveryDelay, CompletionListener listener) throws JMSException {
        JmsMessage outbound = null;
        sendLock.lock();

        try {
            checkClosed();

            original.setJMSDeliveryMode(deliveryMode);
            original.setJMSPriority(priority);
            original.setJMSRedelivered(false);
            original.setJMSDestination(destination);

            long timeStamp = System.currentTimeMillis();
            boolean hasTTL = timeToLive > Message.DEFAULT_TIME_TO_LIVE;
            boolean hasDelay = deliveryDelay > Message.DEFAULT_DELIVERY_DELAY;

            boolean isJmsMessage = original instanceof JmsMessage;

            if (!disableTimestamp) {
                original.setJMSTimestamp(timeStamp);
            } else {
                original.setJMSTimestamp(0);
            }

            if (hasTTL) {
                original.setJMSExpiration(timeStamp + timeToLive);
            } else {
                original.setJMSExpiration(0);
            }

            long messageSequence = producer.getNextMessageSequence();
            Object messageId = null;
            if (!disableMsgId) {
                messageId = producer.getMessageIDBuilder().createMessageID(producer.getProducerId().toString(), messageSequence);
            }

            if (isJmsMessage) {
                outbound = (JmsMessage) original;
            } else {
                // Transform and assign the Destination as one of our own destination objects.
                outbound = JmsMessageTransformation.transformMessage(connection, original);
                outbound.setJMSDestination(destination);
            }

            // Set the delivery time. Purposefully avoided doing this earlier so
            // that we use the 'outbound' JmsMessage object reference when
            // updating our own message instances, avoids using the interface
            // in case the JMS 1.1 Message API is actually being used due to
            // being on the classpath too.
            long deliveryTime = timeStamp;
            if (hasDelay) {
                deliveryTime = timeStamp + deliveryDelay;
            }

            outbound.getFacade().setDeliveryTime(deliveryTime, hasDelay);
            if (!isJmsMessage) {
                // If the original was a foreign message, we still need to update it too.
                setForeignMessageDeliveryTime(original, deliveryTime);
            }

            // Set the message ID
            outbound.getFacade().setProviderMessageIdObject(messageId);
            if (!isJmsMessage) {
                // If the original was a foreign message, we still need to update it
                // with the properly encoded Message ID String, get it from the one
                // we transformed from now that it is set.
                original.setJMSMessageID(outbound.getJMSMessageID());
            }

            // If configured set the User ID using the value we have encoded and cached,
            // otherwise clear to prevent caller from spoofing the user ID value.
            if (connection.isPopulateJMSXUserID()) {
                outbound.getFacade().setUserIdBytes(connection.getEncodedUsername());
            } else {
                outbound.getFacade().setUserId(null);
            }

            boolean sync = connection.isForceSyncSend() ||
                           (!connection.isForceAsyncSend() && deliveryMode == DeliveryMode.PERSISTENT && !getTransacted());

            outbound.onSend(timeToLive);

            JmsOutboundMessageDispatch envelope = new JmsOutboundMessageDispatch();
            envelope.setMessage(outbound);
            envelope.setPayload(outbound.getFacade().encodeMessage());
            envelope.setProducerId(producer.getProducerId());
            envelope.setDestination(destination);
            envelope.setSendAsync(listener == null ? !sync : true);
            envelope.setDispatchId(messageSequence);
            envelope.setCompletionRequired(listener != null);

            if (producer.isAnonymous()) {
                envelope.setPresettle(getPresettlePolicy().isProducerPresttled(this, destination));
            } else {
                envelope.setPresettle(producer.isPresettled());
            }

            if (envelope.isSendAsync() && !envelope.isCompletionRequired() && !envelope.isPresettle()) {
                envelope.setMessage(outbound.copy());
                outbound.onSendComplete();
            }

            if (envelope.isCompletionRequired()) {
                transactionContext.send(connection, envelope, new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        // Provider accepted the send request so new we place the marker in
                        // the queue so that it can be completed asynchronously.
                        asyncSendQueue.addLast(new SendCompletion(envelope, listener));
                    }

                    @Override
                    public void onPendingFailure(ProviderException cause) {
                        // Provider has rejected the send request so we will throw the
                        // exception that is to follow so no completion will be needed.
                    }
                });
            } else {
                transactionContext.send(connection, envelope, null);
            }
        } catch (JMSException jmsEx) {
            // Ensure that on failure case the message is returned to usable state for another send attempt.
            if (outbound != null) {
                outbound.onSendComplete();
            }
            throw jmsEx;
        } finally {
            sendLock.unlock();
        }
    }

    private void setForeignMessageDeliveryTime(Message foreignMessage, long deliveryTime) throws JMSException {
        // Verify if the setJMSDeliveryTime method exists, i.e the foreign provider isn't only JMS 1.1.
        Method deliveryTimeMethod = null;
        try {
            Class<?> clazz = foreignMessage.getClass();
            Method method = clazz.getMethod("setJMSDeliveryTime", new Class[] { long.class });
            if (!Modifier.isAbstract(method.getModifiers())) {
                deliveryTimeMethod = method;
            }
        } catch (NoSuchMethodException e) {
            // Assume its a JMS 1.1 Message, we will no-op.
        }

        if (deliveryTimeMethod != null) {
            // Method exists, isn't abstract, so use it.
            foreignMessage.setJMSDeliveryTime(deliveryTime);
        }
    }

    JmsInboundMessageDispatch acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        transactionContext.acknowledge(connection, envelope, ackType);
        return envelope;
    }

    /**
     * Acknowledge all previously delivered messages in this Session as consumed.  This
     * method is usually only called when the Session is in the CLIENT_ACKNOWLEDGE mode.
     *
     * @param ackType
     *      The type of acknowledgement being done.
     *
     * @throws JMSException if an error occurs while the acknowledge is processed.
     */
    void acknowledge(ACK_TYPE ackType) throws JMSException {
        if (isTransacted()) {
            throw new IllegalStateException("Session acknowledge called inside a transacted Session");
        }

        this.connection.acknowledge(sessionInfo.getId(), ackType);
    }

    /**
     * Acknowledge a specific delivered message in this Session as consumed.  This
     * method is usually only called when the Session is in the individual ack mode.
     *
     * @param ackType
     *      The type of acknowledgement being done.
     * @param envelope
     *      The message envelope.
     *
     * @throws JMSException if an error occurs while the acknowledge is processed.
     */
    void acknowledgeIndividual(ACK_TYPE ackType, JmsInboundMessageDispatch envelope) throws JMSException {
        if (isTransacted()) {
            throw new IllegalStateException("Message acknowledge called inside a transacted Session");
        }

        this.connection.acknowledge(envelope, ackType);
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Checks whether the session uses transactions.
     *
     * @return true if the session uses transactions.
     */
    public boolean isTransacted() {
        return acknowledgementMode == Session.SESSION_TRANSACTED;
    }

    /**
     * Checks whether the session used client acknowledgment.
     *
     * @return true if the session uses client acknowledgment.
     */
    public boolean isClientAcknowledge() {
        return acknowledgementMode == Session.CLIENT_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used auto acknowledgment.
     *
     * @return true if the session uses client acknowledgment.
     */
    public boolean isAutoAcknowledge() {
        return acknowledgementMode == Session.AUTO_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used dup ok acknowledgment.
     *
     * @return true if the session uses client acknowledgment.
     */
    public boolean isDupsOkAcknowledge() {
        return acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session uses presettlement for all consumers.
     *
     * @return true if the session is using a presettlement for consumers.
     */
    public boolean isNoAcknowledge() {
        return acknowledgementMode == NO_ACKNOWLEDGE ||
               acknowledgementMode == ARTEMIS_PRE_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used individual acknowledgment mode.
     *
     * @return true if the session uses individual acknowledgment.
     */
    public boolean isIndividualAcknowledge() {
        return acknowledgementMode == INDIVIDUAL_ACKNOWLEDGE;
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException jmsEx = null;

            if (failureCause.get() == null) {
                jmsEx = new IllegalStateException("The Session is closed");
            } else {
                jmsEx = new IllegalStateException("The Session was closed due to an unrecoverable error.");
                jmsEx.initCause(failureCause.get());
            }

            throw jmsEx;
        }
    }

    static String checkSelector(String selector, boolean validate) throws InvalidSelectorException {
        if (selector != null) {
            if (selector.trim().length() == 0) {
                return null;
            }

            if (validate) {
                try {
                    SelectorParser.parse(selector);
                } catch (FilterException e) {
                    throw new InvalidSelectorException(e.getMessage());
                }
            }
        }

        return selector;
    }

    public static void checkDestination(Destination dest) throws InvalidDestinationException {
        if (dest == null) {
            throw new InvalidDestinationException("Destination cannot be null");
        }
    }

    protected void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            for (JmsMessageConsumer consumer : consumers.values()) {
                consumer.start();
            }
        }
    }

    protected void stop() throws JMSException {
        started.set(false);

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.stop();
        }

        synchronized (sessionInfo) {
            if (deliveryExecutor != null) {
                deliveryExecutor.shutdown();
                deliveryExecutor = null;
            }
        }
    }

    protected boolean isStarted() {
        return started.get();
    }

    public JmsConnection getConnection() {
        return connection;
    }

    Executor getDispatcherExecutor() {
        ThreadPoolExecutor exec = deliveryExecutor;
        if (exec == null) {
            synchronized (sessionInfo) {
                if (deliveryExecutor == null) {
                    if (!closed.get()) {
                        deliveryExecutor = exec = createExecutor("delivery dispatcher", deliveryThread);
                    } else {
                        return NoOpExecutor.INSTANCE;
                    }
                } else {
                    exec = deliveryExecutor;
                }
            }
        }

        return exec;
    }

    private ExecutorService getCompletionExecutor() {
        ThreadPoolExecutor exec = completionExcecutor;
        if (exec == null) {
            synchronized (sessionInfo) {
                exec = completionExcecutor;
                if (exec == null) {
                    exec = createExecutor("completion dispatcher", completionThread);

                    // Ensure work thread is fully up before allowing other threads
                    // to attempt to execute on this instance.
                    Future<?> starter = exec.submit(() -> {});
                    try {
                        starter.get();
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.trace("Completion Executor starter task failed: {}", e.getMessage());
                    }

                    completionExcecutor = exec;
                }
            }
        }

        return exec;
    }

    private ThreadPoolExecutor createExecutor(final String threadNameSuffix, AtomicReference<Thread> threadTracker) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            new QpidJMSThreadFactory("JmsSession ["+ sessionInfo.getId() + "] " + threadNameSuffix, true, threadTracker));

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                // Completely ignore the task if the session has closed.
                if (!closed.get()) {
                    LOG.trace("Task {} rejected from executor: {}", r, e);
                    super.rejectedExecution(r, e);
                }
            }
        });

        return executor;
    }

    protected JmsSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    protected JmsSessionId getSessionId() {
        return sessionInfo.getId();
    }

    protected int getSessionMode() {
        return acknowledgementMode;
    }

    protected JmsConsumerId getNextConsumerId() {
        return new JmsConsumerId(sessionInfo.getId(), consumerIdGenerator.incrementAndGet());
    }

    protected JmsProducerId getNextProducerId() {
        return new JmsProducerId(sessionInfo.getId(), producerIdGenerator.incrementAndGet());
    }

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        return failureCause.get();
    }

    private <T extends JmsMessage> T init(T message) {
        message.setConnection(connection);
        message.setValidatePropertyNames(connection.isValidatePropertyNames());
        return message;
    }

    boolean isDestinationInUse(JmsDestination destination) {
        for (JmsMessageConsumer consumer : consumers.values()) {
            if (consumer.isUsingDestination(destination)) {
                return true;
            }
        }
        return false;
    }

    void checkMessageListener() throws JMSException {
        if (messageListener != null) {
            throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
        }
        for (JmsMessageConsumer consumer : consumers.values()) {
            if (consumer.hasMessageListener()) {
                throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
            }
        }
    }

    void setDeliveryThreadCheckEnabled(boolean enabled) {
        deliveryThreadCheckEnabled = enabled;
    }

    void checkIsDeliveryThread() throws JMSException {
        if (deliveryThreadCheckEnabled && Thread.currentThread().equals(deliveryThread.get())) {
            throw new IllegalStateException("Illegal invocation from MessageListener callback");
        }
    }

    void checkIsCompletionThread() throws JMSException {
        if (Thread.currentThread().equals(completionThread.get())) {
            throw new IllegalStateException("Illegal invocation from CompletionListener callback");
        }
    }

    public JmsMessageIDPolicy getMessageIDPolicy() {
        return sessionInfo.getMessageIDPolicy();
    }

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return sessionInfo.getPrefetchPolicy();
    }

    public JmsPresettlePolicy getPresettlePolicy() {
        return sessionInfo.getPresettlePolicy();
    }

    public JmsRedeliveryPolicy getRedeliveryPolicy() {
        return sessionInfo.getRedeliveryPolicy();
    }

    public JmsDeserializationPolicy getDeserializationPolicy() {
        return sessionInfo.getDeserializationPolicy();
    }

    /**
     * Sets the transaction context of the session.
     *
     * @param transactionContext
     *        provides the means to control a JMS transaction.
     */
    public void setTransactionContext(JmsTransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    /**
     * Returns the transaction context of the session.
     *
     * @return transactionContext
     *         session's transaction context.
     */
    public JmsTransactionContext getTransactionContext() {
        return transactionContext;
    }


    boolean isSessionRecovered() {
        return sessionRecovered;
    }

    void clearSessionRecovered() {
        sessionRecovered = false;
    }

    static void validateSessionMode(int mode) {
        switch (mode) {
            case JMSContext.AUTO_ACKNOWLEDGE:
            case JMSContext.CLIENT_ACKNOWLEDGE:
            case JMSContext.DUPS_OK_ACKNOWLEDGE:
            case JMSContext.SESSION_TRANSACTED:
            case INDIVIDUAL_ACKNOWLEDGE:
            case ARTEMIS_PRE_ACKNOWLEDGE:
            case NO_ACKNOWLEDGE:
                return;
            default:
                throw new JMSRuntimeException("Invalid Session Mode: " + mode);
        }
    }

    boolean redeliveryExceeded(JmsInboundMessageDispatch envelope) {
        LOG.trace("checking envelope with {} redeliveries", envelope.getRedeliveryCount());

        JmsConsumerInfo consumerInfo = envelope.getConsumerInfo();

        JmsRedeliveryPolicy redeliveryPolicy = consumerInfo.getRedeliveryPolicy();
        return redeliveryPolicy != null &&
               redeliveryPolicy.getMaxRedeliveries(consumerInfo.getDestination()) >= 0 &&
               redeliveryPolicy.getMaxRedeliveries(consumerInfo.getDestination()) < envelope.getRedeliveryCount();
    }

    //----- Event handlers ---------------------------------------------------//

    @Override
    public void onInboundMessage(JmsInboundMessageDispatch envelope) {
        deliver(envelope);
    }

    protected void onCompletedMessageSend(final JmsOutboundMessageDispatch envelope) {
        getCompletionExecutor().execute(new AsyncCompletionTask(envelope));
    }

    protected void onFailedMessageSend(final JmsOutboundMessageDispatch envelope, final Throwable cause) {
        getCompletionExecutor().execute(new AsyncCompletionTask(envelope, cause));
    }

    protected void onConnectionInterrupted() {
        transactionContext.onConnectionInterrupted();

        // TODO - Synthesize a better exception
        JMSException failureCause = new JMSException("Send failed due to connection loss");
        getCompletionExecutor().execute(new FailOrCompleteAsyncCompletionsTask(failureCause));

        for (JmsMessageProducer producer : producers.values()) {
            producer.onConnectionInterrupted();
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionInterrupted();
        }
    }

    protected void onConnectionRecovery(Provider provider) throws Exception {
        if (!sessionInfo.isClosed()) {
            ProviderFuture request = provider.newProviderFuture();
            provider.create(sessionInfo, request);
            request.sync();

            transactionContext.onConnectionRecovery(provider);

            for (JmsMessageProducer producer : producers.values()) {
                producer.onConnectionRecovery(provider);
            }

            for (JmsMessageConsumer consumer : consumers.values()) {
                consumer.onConnectionRecovery(provider);
            }
        }
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {
        for (JmsMessageProducer producer : producers.values()) {
            producer.onConnectionRecovered(provider);
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionRecovered(provider);
        }
    }

    protected void onConnectionRestored() {
        for (JmsMessageProducer producer : producers.values()) {
            producer.onConnectionRestored();
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionRestored();
        }
    }

    private void deliver(JmsInboundMessageDispatch envelope) {
        JmsConsumerId id = envelope.getConsumerId();
        if (id == null) {
            this.connection.onException(new JMSException("No ConsumerId set for " + envelope.getMessage()));
        }

        JmsMessageConsumer consumer = consumers.get(id);
        if (consumer != null) {
            consumer.onInboundMessage(envelope);
        }
    }

    void enqueueInSession(Consumer<JmsSession> dispatcher) {
        sessionQueue.add(dispatcher);
    }

    //----- Asynchronous Send Helpers ----------------------------------------//

    private final class FailOrCompleteAsyncCompletionsTask implements Runnable {

        private final JMSException failureCause;
        private final JmsProducerId producerId;

        public FailOrCompleteAsyncCompletionsTask(JMSException failureCause) {
            this(null, failureCause);
        }

        public FailOrCompleteAsyncCompletionsTask(JmsProducerId producerId, JMSException failureCause) {
            this.failureCause = failureCause;
            this.producerId = producerId;
        }

        @Override
        public void run() {
            // For any completion that is not yet marked as complete we fail it
            // otherwise we send the already marked completion state event.
            Iterator<SendCompletion> pending = asyncSendQueue.iterator();
            while (pending.hasNext()) {
                SendCompletion completion = pending.next();

                if (producerId == null || producerId.equals(completion.envelope.getProducerId())) {
                    if (!completion.hasCompleted()) {
                        completion.markAsFailed(failureCause);
                    }

                    try {
                        completion.signalCompletion();
                    } catch (Throwable error) {
                        LOG.error("Failure while performing completion for send: {}", completion.envelope, error);
                    } finally {
                        LOG.trace("Signaled completion of send: {}", completion.envelope);
                    }
                }
            }

            // Only clear on non-discriminating variant to avoid losing track of completions.
            if (producerId == null) {
                asyncSendQueue.clear();
            }

            if (closed.get() && asyncSendsCompletion != null && asyncSendQueue.isEmpty()) {
                asyncSendsCompletion.onSuccess();
            }
        }
    }

    private final class AsyncCompletionTask implements Runnable {

        private final JmsOutboundMessageDispatch envelope;
        private final Throwable cause;

        public AsyncCompletionTask(JmsOutboundMessageDispatch envelope) {
            this(envelope, null);
        }

        public AsyncCompletionTask(JmsOutboundMessageDispatch envelope, Throwable cause) {
            this.envelope = envelope;
            this.cause = cause;
        }

        @Override
        public void run() {
            try {
                SendCompletion completion = asyncSendQueue.peek();
                if (completion.getEnvelope().getDispatchId() == envelope.getDispatchId()) {
                    try {
                        completion = asyncSendQueue.remove();
                        if (cause == null) {
                            completion.markAsComplete();
                        } else {
                            completion.markAsFailed(JmsExceptionSupport.create(cause));
                        }
                        completion.signalCompletion();
                    } catch (Throwable error) {
                        LOG.error("Failure while performing completion for send: {}", envelope, error);
                    }

                    // Signal any trailing completions that have been marked complete
                    // before this one was that they have now that the one in front has
                    Iterator<SendCompletion> pending = asyncSendQueue.iterator();
                    while (pending.hasNext()) {
                        completion = pending.next();
                        if (completion.hasCompleted()) {
                            try {
                                completion.signalCompletion();
                            } catch (Throwable error) {
                                LOG.error("Failure while performing completion for send: {}", envelope, error);
                            } finally {
                                pending.remove();
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    // Not head so mark as complete and wait for the one in front to send
                    // the notification of completion.
                    Iterator<SendCompletion> pending = asyncSendQueue.iterator();
                    while (pending.hasNext()) {
                        completion = pending.next();
                        if (completion.getEnvelope().getDispatchId() == envelope.getDispatchId()) {
                            if (cause == null) {
                                completion.markAsComplete();
                            } else {
                                completion.markAsFailed(JmsExceptionSupport.create(cause));
                            }
                        }
                    }
                }

                if (closed.get() && asyncSendsCompletion != null && asyncSendQueue.isEmpty()) {
                    asyncSendsCompletion.onSuccess();
                }
            } catch (Exception ex) {
                LOG.error("Async completion task encountered unexpected failure", ex);
            }
        }
    }

    private final class SendCompletion {

        private final JmsOutboundMessageDispatch envelope;
        private final CompletionListener listener;

        private Exception failureCause;
        private boolean completed;

        public SendCompletion(JmsOutboundMessageDispatch envelope, CompletionListener listener) {
            this.envelope = envelope;
            this.listener = listener;
        }

        public void markAsComplete() {
            completed = true;
        }

        public void markAsFailed(Exception cause) {
            completed = true;
            failureCause = cause;
        }

        public boolean hasCompleted() {
            return completed;
        }

        public void signalCompletion() {
            JmsMessage message = envelope.getMessage();
            message.onSendComplete();  // Ensure message is returned as readable.

            if (failureCause == null) {
                try {
                    listener.onCompletion(message);
                } catch (Exception ex) {
                    LOG.trace("CompletionListener threw exception from onCompletion for send {}", envelope, ex);
                }
            } else {
                try {
                    listener.onException(message, failureCause);
                } catch (Exception ex) {
                    LOG.trace("CompletionListener threw exception from onException for send {}", envelope, ex);
                }
            }
        }

        public JmsOutboundMessageDispatch getEnvelope() {
            return envelope;
        }
    }
}
