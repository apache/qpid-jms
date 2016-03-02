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

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;

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
import org.apache.qpid.jms.provider.ProviderClosedException;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
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
    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final MockRemotePeer context;

    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private String providerId = UUID.randomUUID().toString();
    private MockProviderListener eventListener;
    private ProviderListener listener;

    public MockProvider(URI remoteURI, MockProviderConfiguration configuration, MockRemotePeer context) {
        this.remoteURI = remoteURI;
        this.configuration = configuration;
        this.context = context;
        this.stats = new MockProviderStats(context != null ? context.getContextStats() : null);

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

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
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        stats.recordConnectAttempt();

        if (configuration.isFailOnConnect()) {
            throw new IOException("Failed to connect to: " + remoteURI);
        }

        if (context != null) {
            context.connect(this);
        }
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        checkClosed();

        if (listener == null) {
            throw new IllegalStateException("Must set a provider listener prior to calling start.");
        }

        if (configuration.isFailOnStart()) {
            throw new IOException();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stats.recordCloseAttempt();

            final ProviderFuture request = new ProviderFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        if (context != null) {
                            context.disconnect(MockProvider.this);
                        }

                        if (configuration.isFailOnClose()) {
                            request.onFailure(new RuntimeException());
                        } else {
                            request.onSuccess();
                        }
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing the MockProvider");
                        request.onFailure(e);
                    }
                }
            });

            try {
                if (closeTimeout < 0) {
                    request.sync();
                } else {
                    request.sync(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            } finally {
                serializer.shutdown();
            }
        }
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public void create(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
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
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
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
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
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
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
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
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordSendCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recoordSessionAcknowledgeCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recoordAcknowledgeCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordCommitCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordRollbackCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordRecoverCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void unsubscribe(final String subscription, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordUnsubscribeCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void pull(final JmsConsumerId consumerId, final long timeout, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    stats.recordPullCall();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
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
                    listener.onConnectionFailure(new IOException("Connection lost"));
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

    //----- Implementation details -------------------------------------------//


    private void checkClosed() throws ProviderClosedException {
        if (closed.get()) {
            throw new ProviderClosedException("This Provider is already closed");
        }
    }

}