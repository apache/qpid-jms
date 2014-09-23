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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
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

import org.apache.activemq.apollo.filter.FilterException;
import org.apache.activemq.apollo.selector.SelectorParser;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsMessageTransformation;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsMessageId;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

/**
 * JMS Session implementation
 */
@SuppressWarnings("static-access")
public class JmsSession implements Session, QueueSession, TopicSession, JmsMessageDispatcher {

    private final JmsConnection connection;
    private final int acknowledgementMode;
    private final List<JmsMessageProducer> producers = new CopyOnWriteArrayList<JmsMessageProducer>();
    private final Map<JmsConsumerId, JmsMessageConsumer> consumers = new ConcurrentHashMap<JmsConsumerId, JmsMessageConsumer>();
    private MessageListener messageListener;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final LinkedBlockingQueue<JmsInboundMessageDispatch> stoppedMessages =
        new LinkedBlockingQueue<JmsInboundMessageDispatch>(10000);
    private JmsPrefetchPolicy prefetchPolicy;
    private JmsSessionInfo sessionInfo;
    private ExecutorService executor;
    private final ReentrantLock sendLock = new ReentrantLock();

    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong producerIdGenerator = new AtomicLong();
    private JmsLocalTransactionContext transactionContext;
    private JmsMessageFactory messageFactory;

    protected JmsSession(JmsConnection connection, JmsSessionId sessionId, int acknowledgementMode) throws JMSException {
        this.connection = connection;
        this.acknowledgementMode = acknowledgementMode;
        this.prefetchPolicy = new JmsPrefetchPolicy(connection.getPrefetchPolicy());

        setTransactionContext(new JmsLocalTransactionContext(this));

        this.sessionInfo = new JmsSessionInfo(sessionId);
        this.sessionInfo.setAcknowledgementMode(acknowledgementMode);
        this.sessionInfo.setSendAcksAsync(connection.isSendAcksAsync());

        this.sessionInfo = connection.createResource(sessionInfo);
        this.messageFactory = connection.getMessageFactory();
    }

    int acknowledgementMode() {
        return this.acknowledgementMode;
    }

    //////////////////////////////////////////////////////////////////////////
    // Session methods
    //////////////////////////////////////////////////////////////////////////

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkClosed();
        return this.acknowledgementMode;
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return isTransacted();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;
    }

    @Override
    public void recover() throws JMSException {
        checkClosed();
        if (getTransacted()) {
            throw new javax.jms.IllegalStateException("Cannot call recover() on a transacted session");
        }

        this.connection.recover(getSessionId());
    }

    @Override
    public void commit() throws JMSException {
        checkClosed();

        if (!getTransacted()) {
           throw new javax.jms.IllegalStateException("Not a transacted session");
        }

        this.transactionContext.commit();
    }

    @Override
    public void rollback() throws JMSException {
        checkClosed();
        if (!getTransacted()) {
            throw new javax.jms.IllegalStateException("Not a transacted session");
        }

        this.transactionContext.rollback();

        getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for (JmsMessageConsumer c : consumers.values()) {
                    c.drainMessageQueueToListener();
                }
            }
        });
    }

    @Override
    public void run() {
        try {
            checkClosed();
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws JMSException {
        if (!closed.get()) {
            doClose();
        }
    }

    /**
     * Shutdown the Session and release all resources.  Once completed the Session can
     * request that the Provider destroy the Session and it's child resources.
     *
     * @throws JMSException
     */
    protected void doClose() throws JMSException {
        boolean interrupted = Thread.interrupted();
        shutdown();
        this.connection.removeSession(this);
        this.connection.destroyResource(sessionInfo);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method should terminate all Session resources and prepare for disposal of the
     * Session.  It is called either from the Session close method or from the Connection
     * when a close request is made and the Connection wants to cleanup all Session resources.
     *
     * This method should not attempt to send a destroy request to the Provider as that
     * will either be done by another session method or is not needed when done by the parent
     * Connection.
     *
     * @throws JMSException
     */
    protected void shutdown() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            stop();
            for (JmsMessageConsumer consumer : new ArrayList<JmsMessageConsumer>(this.consumers.values())) {
                consumer.shutdown();
            }

            for (JmsMessageProducer producer : this.producers) {
                producer.shutdown();
            }

            try {
                if (getTransactionContext().isInTransaction()) {
                    rollback();
                }
            } catch (JMSException e) {
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Consumer creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @param destination
     * @return a MessageConsumer
     * @throws JMSException
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null);
    }

    /**
     * @param destination
     * @param messageSelector
     * @return MessageConsumer
     * @throws JMSException
     * @see javax.jms.Session#createConsumer(javax.jms.Destination,
     *      java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    /**
     * @param destination
     * @param messageSelector
     * @param NoLocal
     * @return the MessageConsumer
     * @throws JMSException
     * @see javax.jms.Session#createConsumer(javax.jms.Destination,
     *      java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean NoLocal) throws JMSException {
        checkClosed();
        checkDestination(destination);
        messageSelector = checkSelector(messageSelector);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsTopicSubscriber result = new JmsTopicSubscriber(getNextConsumerId(), this, dest, NoLocal, messageSelector);
        result.init();
        return result;
    }

    /**
     * @param queue
     * @return QueueRecevier
     * @throws JMSException
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        checkClosed();
        checkDestination(queue);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, queue);
        JmsQueueReceiver result = new JmsQueueReceiver(getNextConsumerId(), this, dest, "");
        result.init();
        return result;
    }

    /**
     * @param queue
     * @param messageSelector
     * @return QueueReceiver
     * @throws JMSException
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue,
     *      java.lang.String)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(queue);
        messageSelector = checkSelector(messageSelector);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, queue);
        JmsQueueReceiver result = new JmsQueueReceiver(getNextConsumerId(), this, dest, messageSelector);
        result.init();
        return result;
    }

    /**
     * @param destination
     * @return QueueBrowser
     * @throws JMSException
     * @see javax.jms.Session#createBrowser(javax.jms.Queue)
     */
    @Override
    public QueueBrowser createBrowser(Queue destination) throws JMSException {
        return createBrowser(destination, null);
    }

    /**
     * @param destination
     * @param messageSelector
     * @return QueueBrowser
     * @throws JMSException
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueBrowser createBrowser(Queue destination, String messageSelector) throws JMSException {
        checkClosed();
        checkDestination(destination);
        messageSelector = checkSelector(messageSelector);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsQueueBrowser result = new JmsQueueBrowser(this, dest, messageSelector);
        return result;
    }

    /**
     * @param topic
     * @return TopicSubscriber
     * @throws JMSException
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    /**
     * @param topic
     * @param messageSelector
     * @param noLocal
     * @return TopicSubscriber
     * @throws JMSException
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic,
     *      java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        messageSelector = checkSelector(messageSelector);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicSubscriber result = new JmsTopicSubscriber(getNextConsumerId(), this, dest, noLocal, messageSelector);
        result.init();
        return result;
    }

    /**
     * @param topic
     * @param name
     * @return a TopicSubscriber
     * @throws JMSException
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic,
     *      java.lang.String)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    /**
     * @param topic
     * @param name
     * @param messageSelector
     * @param noLocal
     * @return TopicSubscriber
     * @throws JMSException
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic,
     *      java.lang.String, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();
        checkDestination(topic);
        messageSelector = checkSelector(messageSelector);
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicSubscriber result = new JmsDurableTopicSubscriber(getNextConsumerId(), this, dest, name, false, messageSelector);
        result.init();
        return result;
    }

    /**
     * @param name
     * @throws JMSException
     * @see javax.jms.Session#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
        this.connection.unsubscribe(name);
    }

    //////////////////////////////////////////////////////////////////////////
    // Producer creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @param destination
     * @return MessageProducer
     * @throws JMSException
     * @see javax.jms.Session#createProducer(javax.jms.Destination)
     */
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, destination);
        JmsMessageProducer result = new JmsMessageProducer(getNextProducerId(), this, dest);
        add(result);
        return result;
    }

    /**
     * @param queue
     * @return QueueSender
     * @throws JMSException
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
     * @param topic
     * @return TopicPublisher
     * @throws JMSException
     * @see javax.jms.TopicSession#createPublisher(javax.jms.Topic)
     */
    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        checkClosed();
        JmsDestination dest = JmsMessageTransformation.transformDestination(connection, topic);
        JmsTopicPublisher result = new JmsTopicPublisher(getNextProducerId(), this, dest);
        add(result);
        return result;
    }

    //////////////////////////////////////////////////////////////////////////
    // Message creation
    //////////////////////////////////////////////////////////////////////////

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createBytesMessage());
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createMapMessage());
    }

    @Override
    public Message createMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createMessage());
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createObjectMessage(null));
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        return init(messageFactory.createObjectMessage(object));
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createStreamMessage());
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return init(messageFactory.createTextMessage(null));
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        return init(messageFactory.createTextMessage(text));
    }

    //////////////////////////////////////////////////////////////////////////
    // Destination creation
    //////////////////////////////////////////////////////////////////////////

    /**
     * @param queueName
     * @return Queue
     * @throws JMSException
     * @see javax.jms.Session#createQueue(java.lang.String)
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return new JmsQueue(queueName);
    }

    /**
     * @param topicName
     * @return Topic
     * @throws JMSException
     * @see javax.jms.Session#createTopic(java.lang.String)
     */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        checkClosed();
        return new JmsTopic(topicName);
    }

    /**
     * @return TemporaryQueue
     * @throws JMSException
     * @see javax.jms.Session#createTemporaryQueue()
     */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkClosed();
        return connection.createTemporaryQueue();
    }

    /**
     * @return TemporaryTopic
     * @throws JMSException
     * @see javax.jms.Session#createTemporaryTopic()
     */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkClosed();
        return connection.createTemporaryTopic();
    }

    //////////////////////////////////////////////////////////////////////////
    // Session Implementation methods
    //////////////////////////////////////////////////////////////////////////

    protected void add(JmsMessageConsumer consumer) throws JMSException {
        this.consumers.put(consumer.getConsumerId(), consumer);
        this.connection.addDispatcher(consumer.getConsumerId(), this);

        if (started.get()) {
            consumer.start();
        }
    }

    protected void remove(JmsMessageConsumer consumer) throws JMSException {
        this.connection.removeDispatcher(consumer.getConsumerId());
        this.consumers.remove(consumer.getConsumerId());
    }

    protected void add(JmsMessageProducer producer) {
        this.producers.add(producer);
    }

    protected void remove(MessageProducer producer) {
        this.producers.remove(producer);
    }

    protected void onException(Exception ex) {
        this.connection.onException(ex);
    }

    protected void onException(JMSException ex) {
        this.connection.onException(ex);
    }

    protected void send(JmsMessageProducer producer, Destination dest, Message msg, int deliveryMode, int priority, long timeToLive, boolean disableMsgId, boolean disableTimestamp) throws JMSException {
        JmsDestination destination = JmsMessageTransformation.transformDestination(connection, dest);
        send(producer, destination, msg, deliveryMode, priority, timeToLive, disableMsgId, disableTimestamp);
    }

    private void send(JmsMessageProducer producer, JmsDestination destination, Message original, int deliveryMode, int priority, long timeToLive, boolean disableMsgId, boolean disableTimestamp) throws JMSException {
        sendLock.lock();
        try {
            startNextTransaction();

            original.setJMSDeliveryMode(deliveryMode);
            original.setJMSPriority(priority);
            original.setJMSRedelivered(false);

            long timeStamp = 0;
            boolean hasTTL = timeToLive > 0;
            if (!disableTimestamp || hasTTL) {
                timeStamp = System.currentTimeMillis();
            }

            original.setJMSTimestamp(timeStamp);

            if (hasTTL) {
                original.setJMSExpiration(timeStamp + timeToLive);
            }

            JmsMessageId msgId = null;
            if (!disableMsgId) {
                msgId = getNextMessageId(producer);
            }

            boolean isJmsMessageType = original instanceof JmsMessage;
            if (isJmsMessageType) {
                ((JmsMessage) original).setConnection(connection);
                if (!disableMsgId) {
                    ((JmsMessage) original).setJMSMessageID(msgId);
                }
                original.setJMSDestination(destination);
            }

            JmsMessage copy = JmsMessageTransformation.transformMessage(connection, original);

            // Ensure original message gets the destination and message ID as per spec.
            if (!isJmsMessageType) {
                if (!disableMsgId) {
                    original.setJMSMessageID(msgId.toString());
                    copy.setJMSMessageID(msgId);
                }
                original.setJMSDestination(destination);
                copy.setJMSDestination(destination);
            }

            boolean sync = connection.isAlwaysSyncSend() ||
                           (!connection.isForceAsyncSend() && deliveryMode == DeliveryMode.PERSISTENT && !getTransacted());

            copy.onSend();
            JmsOutboundMessageDispatch envelope = new JmsOutboundMessageDispatch();
            envelope.setMessage(copy);
            envelope.setProducerId(producer.getProducerId());
            envelope.setDestination(destination);
            envelope.setSendAsync(!sync);

            this.connection.send(envelope);
        } finally {
            sendLock.unlock();
        }
    }

    void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        startNextTransaction();
        this.connection.acknowledge(envelope, ackType);
    }

    /**
     * Acknowledge all previously delivered messages in this Session as consumed.  This
     * method is usually only called when the Session is in the CLIENT_ACKNOWLEDGE mode.
     *
     * @throws JMSException if an error occurs while the acknowledge is processed.
     */
    void acknowledge() throws JMSException {
        this.connection.acknowledge(sessionInfo.getSessionId());
    }

    public boolean isClosed() {
        return this.closed.get();
    }

    /**
     * Checks whether the session uses transactions.
     *
     * @return true - if the session uses transactions.
     */
    public boolean isTransacted() {
        return this.acknowledgementMode == Session.SESSION_TRANSACTED;
    }

    /**
     * Checks whether the session used client acknowledgment.
     *
     * @return true - if the session uses client acknowledgment.
     */
    protected boolean isClientAcknowledge() {
        return this.acknowledgementMode == Session.CLIENT_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used auto acknowledgment.
     *
     * @return true - if the session uses client acknowledgment.
     */
    public boolean isAutoAcknowledge() {
        return acknowledgementMode == Session.AUTO_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used dup ok acknowledgment.
     *
     * @return true - if the session uses client acknowledgment.
     */
    public boolean isDupsOkAcknowledge() {
        return acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE;
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The MessageProducer is closed");
        }
    }

    // This extra wrapping class around SelectorParser is used to avoid
    // ClassNotFoundException if SelectorParser is not in the class path.
    static class OptionalSectorParser {
        public static void check(String selector) throws InvalidSelectorException {
            try {
                SelectorParser.parse(selector);
            } catch (FilterException e) {
                throw new InvalidSelectorException(e.getMessage());
            }
        }
    }

    static final OptionalSectorParser SELECTOR_PARSER;
    static {
        OptionalSectorParser parser;
        try {
            // lets verify it's working..
            parser = new OptionalSectorParser();
            parser.check("x=1");
        } catch (Throwable e) {
            parser = null;
        }
        SELECTOR_PARSER = parser;
    }

    public static String checkSelector(String selector) throws InvalidSelectorException {
        if (selector != null) {
            if (selector.trim().length() == 0) {
                return null;
            }
            if (SELECTOR_PARSER != null) {
                SELECTOR_PARSER.check(selector);
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
            JmsInboundMessageDispatch message = null;
            while ((message = this.stoppedMessages.poll()) != null) {
                deliver(message);
            }
            for (JmsMessageConsumer consumer : consumers.values()) {
                consumer.start();
            }
        }
    }

    protected void stop() throws JMSException {
        started.set(false);
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.stop();
        }
    }

    protected boolean isStarted() {
        return this.started.get();
    }

    public JmsConnection getConnection() {
        return this.connection;
    }

    Executor getExecutor() {
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor(new ThreadFactory() {

                @Override
                public Thread newThread(Runnable runner) {
                    Thread executor = new Thread(runner);
                    executor.setName("JmsSession ["+ sessionInfo.getSessionId() + "] dispatcher");
                    executor.setDaemon(true);
                    return executor;
                }
            });
        }
        return executor;
    }

    protected JmsSessionInfo getSessionInfo() {
        return this.sessionInfo;
    }

    protected JmsSessionId getSessionId() {
        return this.sessionInfo.getSessionId();
    }

    protected JmsConsumerId getNextConsumerId() {
        return new JmsConsumerId(sessionInfo.getSessionId(), consumerIdGenerator.incrementAndGet());
    }

    protected JmsProducerId getNextProducerId() {
        return new JmsProducerId(sessionInfo.getSessionId(), producerIdGenerator.incrementAndGet());
    }

    private JmsMessageId getNextMessageId(JmsMessageProducer producer) {
        return new JmsMessageId(producer.getProducerId(), producer.getNextMessageSequence());
    }

    private <T extends JmsMessage> T init(T message) {
        message.setConnection(connection);
        return message;
    }

    private synchronized void startNextTransaction() throws JMSException {
        if (getTransacted()) {
            transactionContext.begin();
        }
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

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(JmsPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {
        if (started.get()) {
            deliver(envelope);
        } else {
            this.stoppedMessages.add(envelope);
        }
    }

    protected void onConnectionInterrupted() {
        for (JmsMessageProducer producer : producers) {
            producer.onConnectionInterrupted();
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionInterrupted();
        }
    }

    protected void onConnectionRecovery(Provider provider) throws Exception {

        ProviderFuture request = new ProviderFuture();
        provider.create(sessionInfo, request);
        request.sync();

        if (this.acknowledgementMode == SESSION_TRANSACTED) {
            if (transactionContext.isInTransaction()) {
                transactionContext.clear();
                transactionContext.begin();
            }
        }

        for (JmsMessageProducer producer : producers) {
            producer.onConnectionRecovery(provider);
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionRecovery(provider);
        }
    }

    protected void onConnectionRecovered(Provider provider) throws Exception {

        this.messageFactory = provider.getMessageFactory();

        for (JmsMessageProducer producer : producers) {
            producer.onConnectionRecovered(provider);
        }

        for (JmsMessageConsumer consumer : consumers.values()) {
            consumer.onConnectionRecovered(provider);
        }
    }

    protected void onConnectionRestored() {
        for (JmsMessageProducer producer : producers) {
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
        if (this.messageListener != null) {
            this.messageListener.onMessage(envelope.getMessage());
        } else {
            JmsMessageConsumer consumer = this.consumers.get(id);
            if (consumer != null) {
                consumer.onMessage(envelope);
            }
        }
    }

    /**
     * Sets the transaction context of the session.
     *
     * @param transactionContext
     *        provides the means to control a JMS transaction.
     */
    public void setTransactionContext(JmsLocalTransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    /**
     * Returns the transaction context of the session.
     *
     * @return transactionContext
     *         session's transaction context.
     */
    public JmsLocalTransactionContext getTransactionContext() {
        return transactionContext;
    }
}
