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
package org.apache.qpid.jms.provider.mock;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.exceptions.ProviderClosedException;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderIOException;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock Provider instance used in testing higher level classes that interact
 * with a given Provider instance.
 */
public class MockProvider implements Provider {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProvider.class);

    private static final AtomicInteger PROVIDER_SEQUENCE = new AtomicInteger();

    private final JmsMessageFactory messageFactory = new JmsTestMessageFactory();
    private final MockProviderStats stats;
    private final URI remoteURI;
    private final MockProviderConfiguration configuration;
    private final ScheduledThreadPoolExecutor serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final MockRemotePeer context;
    private final ProviderFutureFactory futureFactory;

    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private String providerId = UUID.randomUUID().toString();
    private MockProviderListener eventListener;
    private ProviderListener listener;

    public MockProvider(URI remoteURI, MockProviderConfiguration configuration, MockRemotePeer context, ProviderFutureFactory futureFactory) {
        this.remoteURI = remoteURI;
        this.configuration = configuration;
        this.context = context;
        this.stats = new MockProviderStats(context != null ? context.getContextStats() : null);
        this.futureFactory = futureFactory;

        serializer = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(MockProvider.this.getClass().getSimpleName() + ":(" +
                               PROVIDER_SEQUENCE.incrementAndGet() + "):[" +
                               getRemoteURI() + "]");
                return serial;
            }
        });

        serializer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        serializer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @Override
    public void connect(JmsConnectionInfo connectionInfo) throws ProviderException {
        checkClosed();

        stats.recordConnectAttempt();

        if (configuration.isFailOnConnect()) {
            throw new ProviderIOException("Failed to connect to: " + remoteURI);
        }

        if (context != null) {
            context.connect(this);
        }
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        checkClosed();

        if (listener == null) {
            throw new IllegalStateException("Must set a provider listener prior to calling start.");
        }

        if (configuration.isFailOnStart()) {
            throw new ProviderException("Error");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stats.recordCloseAttempt();

            final ProviderFuture request = futureFactory.createFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        if (context != null) {
                            context.disconnect(MockProvider.this);
                        }

                        if (configuration.isFailOnClose()) {
                            request.onFailure(new ProviderIOException("Failed on close"));
                        } else {
                            request.onSuccess();
                        }
                    } catch (Throwable e) {
                        LOG.debug("Caught exception while closing the MockProvider");
                        request.onFailure(ProviderExceptionSupport.createOrPassthroughFatal(e));
                    }
                }
            });

            try {
                if (closeTimeout < 0) {
                    request.sync();
                } else {
                    request.sync(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            } finally {
                ThreadPoolUtils.shutdownGraceful(serializer);
            }
        }
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public List<URI> getAlternateURIs() {
        return Collections.emptyList();
    }

    @Override
    public void create(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordCreateResourceCall(resource);

                    if (context != null) {
                        context.createResource(resource);
                    }

                    if (resource instanceof JmsConnectionInfo) {
                        if (listener != null) {
                            listener.onConnectionEstablished(remoteURI);
                        }
                    }

                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordStartResourceCall(resource);

                    if (context != null) {
                        context.startResource(resource);
                    }

                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();

                    if (context != null) {
                        context.stopResource(resource);
                    }

                    stats.recordStopResourceCall(resource);
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordDestroyResourceCall(resource);

                    if (context != null) {
                        context.destroyResource(resource);
                    }

                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordSendCall();

                    if (context != null) {
                        context.recordSend(MockProvider.this, envelope);
                    }

                    // Put the message back to usable state following send complete
                    envelope.getMessage().onSendComplete();

                    request.onSuccess();
                    if (envelope.isCompletionRequired()) {
                        if (context != null && configuration.isDelayCompletionCalls()) {
                            context.recordPendingCompletion(MockProvider.this, envelope);
                        } else {
                            if (listener != null) {
                                listener.onCompletedMessageSend(envelope);
                            }
                        }
                    }
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recoordSessionAcknowledgeCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recoordAcknowledgeCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, final JmsTransactionInfo nextTransactionInfo, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordCommitCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, final JmsTransactionInfo nextTransactionInfo, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordRollbackCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordRecoverCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void unsubscribe(final String subscription, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordUnsubscribeCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    @Override
    public void pull(final JmsConsumerId consumerId, final long timeout, final AsyncResult request) throws ProviderException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordPullCall();
                    request.onSuccess();
                } catch (Throwable error) {
                    request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });
    }

    //----- API for generating provider events to a connection ---------------//

    public void signalConnectionFailed() {
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                if (!closed.get()) {
                    listener.onConnectionFailure(new ProviderIOException("Connection lost"));
                }
            }
        });
    }

    /**
     * Switch state to closed without sending any notifications
     */
    public void silentlyClose() {
        close();
    }

    //----- Property getters and setters -------------------------------------//

    @Override
    public JmsMessageFactory getMessageFactory() {
        return messageFactory;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        if (eventListener != null) {
            eventListener.whenProviderListenerSet(this, listener);
        }
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    public MockProvider setEventListener(MockProviderListener eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public MockProviderConfiguration getConfiguration() {
        return configuration;
    }

    public MockProviderStats getStatistics() {
        return stats;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderId() {
        return this.providerId;
    }

    public long getCloseTimeout() {
        return this.closeTimeout;
    }

    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public ProviderFuture newProviderFuture() {
        return futureFactory.createFuture();
    }

    @Override
    public ProviderFuture newProviderFuture(ProviderSynchronization synchronization) {
        return futureFactory.createFuture(synchronization);
    }

    //----- Implementation details -------------------------------------------//

    private void checkClosed() throws ProviderClosedException {
        if (closed.get()) {
            throw new ProviderClosedException("This Provider is already closed");
        }
    }
}