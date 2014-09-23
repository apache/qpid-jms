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

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.exceptions.JmsConnectionFailedException;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderClosedException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a JMS Connection
 */
public class JmsConnection implements Connection, TopicConnection, QueueConnection, ProviderListener {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnection.class);

    private JmsConnectionInfo connectionInfo;

    private final IdGenerator clientIdGenerator;
    private boolean clientIdSet;
    private boolean sendAcksAsync;
    private ExceptionListener exceptionListener;
    private final List<JmsSession> sessions = new CopyOnWriteArrayList<JmsSession>();
    private final Map<JmsConsumerId, JmsMessageDispatcher> dispatchers =
        new ConcurrentHashMap<JmsConsumerId, JmsMessageDispatcher>();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final Object connectLock = new Object();
    private IOException firstFailureError;
    private JmsPrefetchPolicy prefetchPolicy = new JmsPrefetchPolicy();
    private boolean messagePrioritySupported;

    private final ThreadPoolExecutor executor;

    private URI brokerURI;
    private URI localURI;
    private SSLContext sslContext;
    private Provider provider;
    private final Set<JmsConnectionListener> connectionListeners =
        new CopyOnWriteArraySet<JmsConnectionListener>();
    private final Map<JmsDestination, JmsDestination> tempDestinations =
        new ConcurrentHashMap<JmsDestination, JmsDestination>();
    private final AtomicLong sessionIdGenerator = new AtomicLong();
    private final AtomicLong tempDestIdGenerator = new AtomicLong();
    private final AtomicLong transactionIdGenerator = new AtomicLong();
    private JmsMessageFactory messageFactory;

    protected JmsConnection(String connectionId, Provider provider, IdGenerator clientIdGenerator) throws JMSException {

        // This executor can be used for dispatching asynchronous tasks that might block or result
        // in reentrant calls to this Connection that could block.  The thread in this executor
        // will also serve as a means of preventing JVM shutdown should a client application
        // not have it's own mechanism for doing so.
        executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "QpidJMS Connection Executor: ");
                return thread;
            }
        });

        this.provider = provider;
        this.provider.setProviderListener(this);
        try {
            this.provider.start();
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }

        this.clientIdGenerator = clientIdGenerator;
        this.connectionInfo = new JmsConnectionInfo(new JmsConnectionId(connectionId));
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#close()
     */
    @Override
    public void close() throws JMSException {
        boolean interrupted = Thread.interrupted();

        try {

            if (!closed.get() && !failed.get()) {
                // do not fail if already closed as specified by the JMS specification.
                doStop(false);
            }

            synchronized (this) {

                if (closed.get()) {
                    return;
                }

                closing.set(true);

                for (JmsSession session : this.sessions) {
                    session.shutdown();
                }

                this.sessions.clear();
                this.tempDestinations.clear();

                if (isConnected() && !failed.get()) {
                    ProviderFuture request = new ProviderFuture();
                    try {
                        provider.destroy(connectionInfo, request);

                        try {
                            request.sync();
                        } catch (Exception ex) {
                            // TODO - Spec is a bit vague here, we don't fail if already closed but
                            //        in this case we really aren't closed yet so there could be an
                            //        argument that at this point an exception is still valid.
                            if (ex.getCause() instanceof InterruptedException) {
                                throw (InterruptedException) ex.getCause();
                            }
                            LOG.debug("Failed destroying Connection resource: {}", ex.getMessage());
                        }
                    } catch(ProviderClosedException pce) {
                        LOG.debug("Ignoring provider closed exception during connection close");
                    }
                }

                connected.set(false);
                started.set(false);
                closing.set(false);
                closed.set(true);
            }
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        } finally {
            try {
                ThreadPoolUtils.shutdown(executor);
            } catch (Throwable e) {
                LOG.warn("Error shutting down thread pool: " + executor + ". This exception will be ignored.", e);
            }

            if (provider != null) {
                provider.close();
                provider = null;
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Called to free all Connection resources.
     */
    protected void shutdown() throws JMSException {

        // TODO - Once ConnectionConsumer is added we must shutdown those as well.

        for (JmsSession session : this.sessions) {
            session.shutdown();
        }

        if (isConnected() && !failed.get() && !closing.get()) {
            destroyResource(connectionInfo);
        }

        if (clientIdSet) {
            connectionInfo.setClientId(null);
            clientIdSet = false;
        }

        tempDestinations.clear();
        started.set(false);
        connected.set(false);
    }

    /**
     * @param destination
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.Connection#createConnectionConsumer(javax.jms.Destination,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        throw new JMSException("Not supported");
    }

    /**
     * @param topic
     * @param subscriptionName
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     *
     * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic,
     *      java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
                                                              String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        throw new JMSException("Not supported");
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return Session
     * @throws JMSException
     * @see javax.jms.Connection#createSession(boolean, int)
     */
    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsSession result = new JmsSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @return clientId
     * @see javax.jms.Connection#getClientID()
     */
    @Override
    public String getClientID() throws JMSException {
        checkClosedOrFailed();
        return this.connectionInfo.getClientId();
    }

    /**
     * @return connectionInfoData
     * @see javax.jms.Connection#getMetaData()
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosedOrFailed();
        return JmsConnectionMetaData.INSTANCE;
    }

    /**
     * @param clientID
     * @throws JMSException
     * @see javax.jms.Connection#setClientID(java.lang.String)
     */
    @Override
    public synchronized void setClientID(String clientID) throws JMSException {
        checkClosedOrFailed();

        if (this.clientIdSet) {
            throw new IllegalStateException("The clientID has already been set");
        }
        if (clientID == null) {
            throw new IllegalStateException("Cannot have a null clientID");
        }
        if (connected.get()) {
            throw new IllegalStateException("Cannot set the client id once connected.");
        }

        this.connectionInfo.setClientId(clientID);
        this.clientIdSet = true;

        //We weren't connected if we got this far, we should now connect now to ensure the clientID is valid.
        //TODO: determine if any resulting failure is only the result of the ClientID value, or other reasons such as auth.
        connect();
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#start()
     */
    @Override
    public void start() throws JMSException {
        checkClosedOrFailed();
        connect();
        if (this.started.compareAndSet(false, true)) {
            try {
                for (JmsSession s : this.sessions) {
                    s.start();
                }
            } catch (Exception e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    /**
     * @throws JMSException
     * @see javax.jms.Connection#stop()
     */
    @Override
    public void stop() throws JMSException {
        doStop(true);
    }

    /**
     * @see #stop()
     * @param checkClosed <tt>true</tt> to check for already closed and throw
     *                    {@link java.lang.IllegalStateException} if already closed,
     *                    <tt>false</tt> to skip this check
     * @throws JMSException if the JMS provider fails to stop message delivery due to some internal error.
     */
    void doStop(boolean checkClosed) throws JMSException {
        if (checkClosed) {
            checkClosedOrFailed();
        }
        if (started.compareAndSet(true, false)) {
            synchronized(sessions) {
                for (JmsSession s : this.sessions) {
                    s.stop();
                }
            }
        }
    }

    /**
     * @param topic
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.TopicConnection#createConnectionConsumer(javax.jms.Topic,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        return null;
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return TopicSession
     * @throws JMSException
     * @see javax.jms.TopicConnection#createTopicSession(boolean, int)
     */
    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsTopicSession result = new JmsTopicSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @param queue
     * @param messageSelector
     * @param sessionPool
     * @param maxMessages
     * @return ConnectionConsumer
     * @throws JMSException
     * @see javax.jms.QueueConnection#createConnectionConsumer(javax.jms.Queue,
     *      java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
                                                       ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosedOrFailed();
        connect();
        return null;
    }

    /**
     * @param transacted
     * @param acknowledgeMode
     * @return QueueSession
     * @throws JMSException
     * @see javax.jms.QueueConnection#createQueueSession(boolean, int)
     */
    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        connect();
        int ackMode = getSessionAcknowledgeMode(transacted, acknowledgeMode);
        JmsQueueSession result = new JmsQueueSession(this, getNextSessionId(), ackMode);
        addSession(result);
        if (started.get()) {
            result.start();
        }
        return result;
    }

    /**
     * @param ex
     */
    public void onException(Exception ex) {
        onException(JmsExceptionSupport.create(ex));
    }

    /**
     * @param ex
     */
    public void onException(JMSException ex) {
        ExceptionListener l = this.exceptionListener;
        if (l != null) {
            l.onException(JmsExceptionSupport.create(ex));
        }
    }

    protected int getSessionAcknowledgeMode(boolean transacted, int acknowledgeMode) throws JMSException {
        int result = acknowledgeMode;
        if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED) {
            throw new JMSException("acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
        }
        if (transacted) {
            result = Session.SESSION_TRANSACTED;
        }
        return result;
    }

    protected void removeSession(JmsSession session) throws JMSException {
        this.sessions.remove(session);
    }

    protected void addSession(JmsSession s) {
        this.sessions.add(s);
    }

    protected void addDispatcher(JmsConsumerId consumerId, JmsMessageDispatcher dispatcher) {
        dispatchers.put(consumerId, dispatcher);
    }

    protected void removeDispatcher(JmsConsumerId consumerId) {
        dispatchers.remove(consumerId);
    }

    private void connect() throws JMSException {
        synchronized(this.connectLock) {
            if (isConnected() || closed.get()) {
                return;
            }

            if (connectionInfo.getClientId() == null || connectionInfo.getClientId().trim().isEmpty()) {
                connectionInfo.setClientId(clientIdGenerator.generateId());
            }

            this.connectionInfo = createResource(connectionInfo);
            this.connected.set(true);
            this.messageFactory = provider.getMessageFactory();

            // TODO - Advisory Support.
            //
            // Providers should have an interface for adding a listener for temporary
            // destination advisory messages for create / destroy so we can track them
            // and throw exceptions when producers try to send to deleted destinations.
        }
    }

    /**
     * @return a newly initialized TemporaryQueue instance.
     */
    protected TemporaryQueue createTemporaryQueue() throws JMSException {
        String destinationName = connectionInfo.getConnectionId() + ":" + tempDestIdGenerator.incrementAndGet();
        JmsTemporaryQueue queue = new JmsTemporaryQueue(destinationName);
        queue = createResource(queue);
        tempDestinations.put(queue, queue);
        return queue;
    }

    /**
     * @return a newly initialized TemporaryTopic instance.
     */
    protected TemporaryTopic createTemporaryTopic() throws JMSException {
        String destinationName = connectionInfo.getConnectionId() + ":" + tempDestIdGenerator.incrementAndGet();
        JmsTemporaryTopic topic = new JmsTemporaryTopic(destinationName);
        topic = createResource(topic);
        tempDestinations.put(topic, topic);
        return topic;
    }

    protected void deleteDestination(JmsDestination destination) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {

            for (JmsSession session : this.sessions) {
                if (session.isDestinationInUse(destination)) {
                    throw new JMSException("A consumer is consuming from the temporary destination");
                }
            }

            if (destination.isTemporary()) {
                tempDestinations.remove(destination);
            }

            destroyResource(destination);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected void checkClosedOrFailed() throws JMSException {
        checkClosed();
        if (failed.get()) {
            throw new JmsConnectionFailedException(firstFailureError);
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (this.closed.get()) {
            throw new IllegalStateException("The Connection is closed");
        }
    }

    protected JmsSessionId getNextSessionId() {
        return new JmsSessionId(connectionInfo.getConnectionId(), sessionIdGenerator.incrementAndGet());
    }

    protected JmsTransactionId getNextTransactionId() {
        return new JmsTransactionId(connectionInfo.getConnectionId(), transactionIdGenerator.incrementAndGet());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Provider interface methods
    ////////////////////////////////////////////////////////////////////////////

    <T extends JmsResource> T createResource(T resource) throws JMSException {
        checkClosedOrFailed();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.create(resource, request);
            request.sync();
            return resource;
        } catch (Exception ex) {
            throw JmsExceptionSupport.create(ex);
        }
    }

    void startResource(JmsResource resource) throws JMSException {
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.start(resource, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void destroyResource(JmsResource resource) throws JMSException {
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.destroy(resource, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void send(JmsOutboundMessageDispatch envelope) throws JMSException {
        checkClosedOrFailed();
        connect();

        // TODO - We don't currently have a way to say that an operation
        //        should be done asynchronously.  A send can be done async
        //        in many cases, such as non-persistent delivery.  We probably
        //        don't need to do anything here though just have a way to
        //        configure the provider for async sends which we do in the
        //        JmsConnectionInfo.  Here we just need to register a listener
        //        on the request to know when it completes if we want to do
        //        JMS 2.0 style async sends where we signal a callback, then
        //        we can manage order of callback events to async senders at
        //        this level.
        try {
            ProviderFuture request = new ProviderFuture();
            provider.send(envelope, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.acknowledge(envelope, ackType, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void acknowledge(JmsSessionId sessionId) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.acknowledge(sessionId, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void unsubscribe(String name) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.unsubscribe(name, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void commit(JmsSessionId sessionId) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.commit(sessionId, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void rollback(JmsSessionId sessionId) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.rollback(sessionId, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void recover(JmsSessionId sessionId) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.recover(sessionId, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    void pull(JmsConsumerId consumerId, long timeout) throws JMSException {
        checkClosedOrFailed();
        connect();

        try {
            ProviderFuture request = new ProviderFuture();
            provider.pull(consumerId, timeout, request);
            request.sync();
        } catch (Exception ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Property setters and getters
    ////////////////////////////////////////////////////////////////////////////

    /**
     * @return ExceptionListener
     * @see javax.jms.Connection#getExceptionListener()
     */
    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosedOrFailed();
        return this.exceptionListener;
    }

    /**
     * @param listener
     * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
     */
    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosedOrFailed();
        this.exceptionListener = listener;
    }

    /**
     * Adds a JmsConnectionListener so that a client can be notified of events in
     * the underlying protocol provider.
     *
     * @param listener
     *        the new listener to add to the collection.
     */
    public void addConnectionListener(JmsConnectionListener listener) {
        this.connectionListeners.add(listener);
    }

    /**
     * Removes a JmsConnectionListener that was previously registered.
     *
     * @param listener
     *        the listener to remove from the collection.
     */
    public void removeTransportListener(JmsConnectionListener listener) {
        this.connectionListeners.remove(listener);
    }

    public boolean isForceAsyncSend() {
        return connectionInfo.isForceAsyncSend();
    }

    public void setForceAsyncSend(boolean forceAsyncSend) {
        connectionInfo.setForceAsyncSends(forceAsyncSend);
    }

    public boolean isAlwaysSyncSend() {
        return connectionInfo.isAlwaysSyncSend();
    }

    public void setAlwaysSyncSend(boolean alwaysSyncSend) {
        this.connectionInfo.setAlwaysSyncSend(alwaysSyncSend);
    }

    public String getTopicPrefix() {
        return connectionInfo.getTopicPrefix();
    }

    public void setTopicPrefix(String topicPrefix) {
        connectionInfo.setTopicPrefix(topicPrefix);
    }

    public String getTempTopicPrefix() {
        return connectionInfo.getTempTopicPrefix();
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        connectionInfo.setTempTopicPrefix(tempTopicPrefix);
    }

    public String getTempQueuePrefix() {
        return connectionInfo.getTempQueuePrefix();
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        connectionInfo.setTempQueuePrefix(tempQueuePrefix);
    }

    public String getQueuePrefix() {
        return connectionInfo.getQueuePrefix();
    }

    public void setQueuePrefix(String queuePrefix) {
        connectionInfo.setQueuePrefix(queuePrefix);
    }

    public boolean isOmitHost() {
        return connectionInfo.isOmitHost();
    }

    public void setOmitHost(boolean omitHost) {
        connectionInfo.setOmitHost(omitHost);
    }

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(JmsPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    public boolean isMessagePrioritySupported() {
        return messagePrioritySupported;
    }

    public void setMessagePrioritySupported(boolean messagePrioritySupported) {
        this.messagePrioritySupported = messagePrioritySupported;
    }

    public long getCloseTimeout() {
        return connectionInfo.getCloseTimeout();
    }

    public void setCloseTimeout(long closeTimeout) {
        connectionInfo.setCloseTimeout(closeTimeout);
    }

    public long getConnectTimeout() {
        return this.connectionInfo.getConnectTimeout();
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectionInfo.setConnectTimeout(connectTimeout);
    }

    public long getSendTimeout() {
        return connectionInfo.getSendTimeout();
    }

    public void setSendTimeout(long sendTimeout) {
        connectionInfo.setSendTimeout(sendTimeout);
    }

    public long getRequestTimeout() {
        return connectionInfo.getRequestTimeout();
    }

    public void setRequestTimeout(long requestTimeout) {
        connectionInfo.setRequestTimeout(requestTimeout);
    }

    public URI getBrokerURI() {
        return brokerURI;
    }

    public void setBrokerURI(URI brokerURI) {
        this.brokerURI = brokerURI;
    }

    public URI getLocalURI() {
        return localURI;
    }

    public void setLocalURI(URI localURI) {
        this.localURI = localURI;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public String getUsername() {
        return this.connectionInfo.getUsername();
    }

    public void setUsername(String username) {
        this.connectionInfo.setUsername(username);;
    }

    public String getPassword() {
        return this.connectionInfo.getPassword();
    }

    public void setPassword(String password) {
        this.connectionInfo.setPassword(password);
    }

    public Provider getProvider() {
        return provider;
    }

    void setProvider(Provider provider) {
        this.provider = provider;
    }

    public boolean isConnected() {
        return this.connected.get();
    }

    public boolean isStarted() {
        return this.started.get();
    }

    public boolean isClosed() {
        return this.closed.get();
    }

    JmsConnectionId getConnectionId() {
        return this.connectionInfo.getConnectionId();
    }

    public boolean isWatchRemoteDestinations() {
        return this.connectionInfo.isWatchRemoteDestinations();
    }

    public void setWatchRemoteDestinations(boolean watchRemoteDestinations) {
        this.connectionInfo.setWatchRemoteDestinations(watchRemoteDestinations);
    }

    public JmsMessageFactory getMessageFactory() {
        return messageFactory;
    }

    public boolean isSendAcksAsync() {
        return sendAcksAsync;
    }

    public void setSendAcksAsync(boolean sendAcksAsync) {
        this.sendAcksAsync = sendAcksAsync;
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {

        JmsMessage incoming = envelope.getMessage();
        // Ensure incoming Messages are in readonly mode.
        if (incoming != null) {
            incoming.setReadOnlyBody(true);
            incoming.setReadOnlyProperties(true);
        }

        JmsMessageDispatcher dispatcher = dispatchers.get(envelope.getConsumerId());
        if (dispatcher != null) {
            dispatcher.onMessage(envelope);
        }
        for (JmsConnectionListener listener : connectionListeners) {
            listener.onMessage(envelope);
        }
    }

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        for (JmsSession session : sessions) {
            session.onConnectionInterrupted();
        }

        for (JmsConnectionListener listener : connectionListeners) {
            listener.onConnectionInterrupted(remoteURI);
        }
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        // TODO - Recover Advisory Consumer once we can support it.

        LOG.debug("Connection {} is starting recovery.", connectionInfo.getConnectionId());

        ProviderFuture request = new ProviderFuture();
        provider.create(connectionInfo, request);
        request.sync();

        for (JmsDestination tempDestination : tempDestinations.values()) {
            createResource(tempDestination);
        }

        for (JmsSession session : sessions) {
            session.onConnectionRecovery(provider);
        }
    }

    @Override
    public void onConnectionRecovered(Provider provider) throws Exception {
        LOG.debug("Connection {} is finalizing recovery.", connectionInfo.getConnectionId());

        this.messageFactory = provider.getMessageFactory();

        for (JmsSession session : sessions) {
            session.onConnectionRecovered(provider);
        }
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        for (JmsSession session : sessions) {
            session.onConnectionRestored();
        }

        for (JmsConnectionListener listener : connectionListeners) {
            listener.onConnectionRestored(remoteURI);
        }
    }

    @Override
    public void onConnectionFailure(final IOException ex) {
        onAsyncException(ex);
        if (!closing.get() && !closed.get()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    providerFailed(ex);
                    if (provider != null) {
                        try {
                            provider.close();
                        } catch (Throwable error) {
                            LOG.debug("Error while closing failed Provider: {}", error.getMessage());
                        }
                    }

                    try {
                        shutdown();
                    } catch (JMSException e) {
                        LOG.warn("Exception during connection cleanup, " + e, e);
                    }

                    for (JmsConnectionListener listener : connectionListeners) {
                        listener.onConnectionFailure(ex);
                    }
                }
            });
        }
    }

    /**
     * Handles any asynchronous errors that occur from the JMS framework classes.
     *
     * If any listeners are registered they will be notified of the error from a thread
     * in the Connection's Executor service.
     *
     * @param error
     *        The exception that triggered this error.
     */
    public void onAsyncException(Throwable error) {
        if (!closed.get() && !closing.get()) {
            if (this.exceptionListener != null) {

                if (!(error instanceof JMSException)) {
                    error = JmsExceptionSupport.create(error);
                }
                final JMSException jmsError = (JMSException)error;

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        JmsConnection.this.exceptionListener.onException(jmsError);
                    }
                });
            } else {
                LOG.debug("Async exception with no exception listener: " + error, error);
            }
        }
    }

    protected void providerFailed(IOException error) {
        failed.set(true);
        if (firstFailureError == null) {
            firstFailureError = error;
        }
    }
}
