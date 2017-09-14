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
package org.apache.qpid.jms.provider.amqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.JMSSecurityRuntimeException;
import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsDefaultResourceVisitor;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResourceVistor;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.NoOpAsyncResult;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderClosedException;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.amqp.builders.AmqpClosedConnectionBuilder;
import org.apache.qpid.jms.provider.amqp.builders.AmqpConnectionBuilder;
import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.jms.sasl.SaslMechanismFinder;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;

/**
 * An AMQP v1.0 Provider.
 *
 * The AMQP Provider is bonded to a single remote broker instance.  The provider will attempt
 * to connect to only that instance and once failed can not be recovered.  For clients that
 * wish to implement failover type connections a new AMQP Provider instance must be created
 * and state replayed from the JMS layer using the standard recovery process defined in the
 * JMS Provider API.
 *
 * All work within this Provider is serialized to a single Thread.  Any asynchronous exceptions
 * will be dispatched from that Thread and all in-bound requests are handled there as well.
 */
public class AmqpProvider implements Provider, TransportListener , AmqpResourceParent {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProvider.class);

    private static final Logger TRACE_BYTES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".BYTES");
    private static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".FRAMES");
    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;
    // NOTE: Limit default channel max to signed short range to deal with
    //       brokers that don't currently handle the unsigned range well.
    private static final int DEFAULT_CHANNEL_MAX = 32767;
    private static final AtomicInteger PROVIDER_SEQUENCE = new AtomicInteger();
    private static final NoOpAsyncResult NOOP_REQUEST = new NoOpAsyncResult();

    private volatile ProviderListener listener;
    private AmqpConnection connection;
    private AmqpSaslAuthenticator authenticator;
    private final Transport transport;
    private String vhost;
    private boolean traceFrames;
    private boolean traceBytes;
    private boolean saslLayer = true;
    private Set<String> saslMechanisms;
    private JmsConnectionInfo connectionInfo;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int idleTimeout = 60000;
    private int drainTimeout = 60000;
    private long sessionOutoingWindow = -1; // Use proton default
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    private boolean allowNonSecureRedirects;

    private final URI remoteURI;
    private final AtomicBoolean closed = new AtomicBoolean();
    private ScheduledThreadPoolExecutor serializer;
    private final org.apache.qpid.proton.engine.Transport protonTransport =
        org.apache.qpid.proton.engine.Transport.Factory.create();
    private final Collector protonCollector = new CollectorImpl();
    private final Connection protonConnection = Connection.Factory.create();

    private AsyncResult connectionRequest;
    private ScheduledFuture<?> nextIdleTimeoutCheck;

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     * @param transport
     * 		  The underlying Transport that will be used for wire level communications.
     */
    public AmqpProvider(URI remoteURI, Transport transport) {
        this.remoteURI = remoteURI;
        this.transport = transport;

        serializer = new ScheduledThreadPoolExecutor(1, new QpidJMSThreadFactory(
            "AmqpProvider :(" + PROVIDER_SEQUENCE.incrementAndGet() + "):[" +
            remoteURI.getScheme() + "://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "]", true));

        serializer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        serializer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @Override
    public void connect(final JmsConnectionInfo connectionInfo) throws IOException {
        checkClosed();

        final ProviderFuture connectRequest = new ProviderFuture();

        serializer.execute(new Runnable() {

            @Override
            public void run() {

                connectionRequest = connectRequest;
                AmqpProvider.this.connectionInfo = connectionInfo;

                try {
                    protonTransport.setEmitFlowEventOnSend(false);

                    if (getMaxFrameSize() > 0) {
                        protonTransport.setMaxFrameSize(getMaxFrameSize());
                    }

                    protonTransport.setChannelMax(getChannelMax());
                    protonTransport.setIdleTimeout(idleTimeout);
                    protonTransport.bind(protonConnection);
                    protonConnection.collect(protonCollector);

                    SSLContext sslContextOverride = connectionInfo.getSslContextOverride();

                    transport.setTransportListener(AmqpProvider.this);
                    transport.setMaxFrameSize(maxFrameSize);
                    transport.connect(sslContextOverride);

                    if (saslLayer) {
                        Sasl sasl = protonTransport.sasl();
                        sasl.client();

                        String hostname = getVhost();
                        if (hostname == null) {
                            hostname = remoteURI.getHost();
                        } else if (hostname.isEmpty()) {
                            hostname = null;
                        }

                        sasl.setRemoteHostname(hostname);

                        authenticator = new AmqpSaslAuthenticator(sasl, (remoteMechanisms) -> findSaslMechanism(remoteMechanisms));

                        pumpToProtonTransport();
                    } else {
                        connectRequest.onSuccess();
                    }
                } catch (Throwable t) {
                    connectionRequest.onFailure(IOExceptionSupport.create(t));
                }
            }
        });

        if (connectionInfo.getConnectTimeout() != JmsConnectionInfo.INFINITE) {
            connectRequest.sync(connectionInfo.getConnectTimeout(), TimeUnit.MILLISECONDS);
        } else {
            connectRequest.sync();
        }
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        checkClosed();

        if (listener == null) {
            throw new IllegalStateException("No ProviderListener registered.");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderFuture request = new ProviderFuture() {

                @Override
                public void onFailure(Throwable result) {
                    // During close it is fine if the close call fails
                    // this in unrecoverable so we just log the event.
                    onSuccess();
                }
            };

            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        // If we are not connected then there is nothing we can do now
                        // just signal success.
                        if (transport == null || !transport.isConnected()) {
                            request.onSuccess();
                            return;
                        }

                        if (connection != null) {
                            connection.close(request);
                        } else {
                            // If the SASL authentication occurred but failed then we don't
                            // need to do an open / close
                            if (authenticator != null && (!authenticator.isComplete() || !authenticator.wasSuccessful())) {
                                request.onSuccess();
                                return;
                            }

                            // Connection attempt might have been tried and failed so only perform
                            // an open / close cycle if one hasn't been done already.
                            if (protonConnection.getLocalState() == EndpointState.UNINITIALIZED) {
                                AmqpClosedConnectionBuilder builder = new AmqpClosedConnectionBuilder(getProvider(), connectionInfo);
                                builder.buildResource(request);

                                protonConnection.setContext(builder);
                            } else {
                                request.onSuccess();
                            }
                        }

                        pumpToProtonTransport(request);
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection: {}", e.getMessage());
                    } finally {
                        if (nextIdleTimeoutCheck != null) {
                            LOG.trace("Cancelling scheduled IdleTimeoutCheck");
                            nextIdleTimeoutCheck.cancel(false);
                            nextIdleTimeoutCheck = null;
                        }
                    }
                }
            });

            try {
                if (getCloseTimeout() < 0) {
                    request.sync();
                } else {
                    request.sync(getCloseTimeout(), TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: {}", e.getMessage() != null ? e.getMessage() : "<Unknown Error>");
            } finally {
                try {
                    if (transport != null) {
                        try {
                            transport.close();
                        } catch (Exception e) {
                            LOG.debug("Caught exception while closing down Transport: {}", e.getMessage());
                        }
                    }
                } finally {
                    ThreadPoolUtils.shutdownGraceful(serializer);
                }
            }
        }
    }

    @Override
    public void create(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsResourceVistor() {
                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            connection.createSession(sessionInfo, request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            session.createProducer(producerInfo, request);
                        }

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            session.createConsumer(consumerInfo, request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            AmqpProvider.this.connectionInfo = connectionInfo;

                            AmqpConnectionBuilder builder = new AmqpConnectionBuilder(AmqpProvider.this, connectionInfo);
                            connectionRequest = new AsyncResult() {
                                @Override
                                public void onSuccess() {
                                    fireConnectionEstablished();
                                    request.onSuccess();
                                }

                                @Override
                                public void onFailure(Throwable result) {
                                    request.onFailure(result);
                                }

                                @Override
                                public boolean isComplete() {
                                    return request.isComplete();
                                }
                            };

                            builder.buildResource(connectionRequest);
                        }

                        @Override
                        public void processDestination(JmsTemporaryDestination destination) throws Exception {
                            if (destination.isTemporary()) {
                                connection.createTemporaryDestination(destination, request);
                            } else {
                                request.onSuccess();
                            }
                        }

                        @Override
                        public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
                            AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                            session.begin(transactionInfo.getId(), request);
                        }
                    });

                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.getConsumer(consumerInfo);
                            consumer.start(request);
                        }
                    });

                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.getConsumer(consumerInfo);
                            consumer.stop(request);
                        }
                    });

                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            final AmqpSession session = connection.getSession(sessionInfo.getId());
                            session.close(new AsyncResult() {
                                // TODO: bit of a hack, but works. Similarly below for locally initiated consumer close.
                                @Override
                                public void onSuccess() {
                                    onComplete();
                                    request.onSuccess();
                                }

                                @Override
                                public void onFailure(Throwable result) {
                                    onComplete();
                                    request.onFailure(result);
                                }

                                @Override
                                public boolean isComplete() {
                                    return request.isComplete();
                                }

                                void onComplete() {
                                    // Mark the sessions resources closed, which in turn calls
                                    // the subscription cleanup.
                                    session.handleResourceClosure(AmqpProvider.this, null);
                                }
                            });
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            AmqpProducer producer = session.getProducer(producerInfo);
                            producer.close(request);
                        }

                        @Override
                        public void processConsumerInfo(final JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            final AmqpConsumer consumer = session.getConsumer(consumerInfo);
                            consumer.close(new AsyncResult() {
                                // TODO: bit of a hack, but works. Similarly above for locally initiated session close.
                                @Override
                                public void onSuccess() {
                                    onComplete();
                                    request.onSuccess();
                                }

                                @Override
                                public void onFailure(Throwable result) {
                                    onComplete();
                                    request.onFailure(result);
                                }

                                @Override
                                public boolean isComplete() {
                                    return request.isComplete();
                                }

                                void onComplete() {
                                    connection.getSubTracker().consumerRemoved(consumerInfo);
                                }
                            });
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            connection.close(request);
                        }

                        @Override
                        public void processDestination(JmsTemporaryDestination destination) throws Exception {
                            AmqpTemporaryDestination temporary = connection.getTemporaryDestination(destination);
                            if (temporary != null) {
                                temporary.close(request);
                            } else {
                                LOG.debug("Could not find temporary destination {} to delete.", destination);
                                request.onSuccess();
                            }
                        }
                    });

                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();

                    JmsProducerId producerId = envelope.getProducerId();
                    AmqpProducer producer = null;

                    if (producerId.getProviderHint() instanceof AmqpFixedProducer) {
                        producer = (AmqpFixedProducer) producerId.getProviderHint();
                    } else {
                        AmqpSession session = connection.getSession(producerId.getParentId());
                        producer = session.getProducer(producerId);
                    }

                    producer.send(envelope, request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession amqpSession = connection.getSession(sessionId);
                    amqpSession.acknowledge(ackType);
                    pumpToProtonTransport(request);
                    request.onSuccess();
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();

                    JmsConsumerId consumerId = envelope.getConsumerId();
                    AmqpConsumer consumer = null;

                    if (consumerId.getProviderHint() instanceof AmqpConsumer) {
                        consumer = (AmqpConsumer) consumerId.getProviderHint();
                    } else {
                        AmqpSession session = connection.getSession(consumerId.getParentId());
                        consumer = session.getConsumer(consumerId);
                    }

                    consumer.acknowledge(envelope, ackType);

                    if (consumer.getSession().isAsyncAck()) {
                        request.onSuccess();
                        pumpToProtonTransport(request);
                    } else {
                        pumpToProtonTransport(request);
                        request.onSuccess();
                    }
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                    session.commit(transactionInfo, nextTransactionId, request);
                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                    session.rollback(transactionInfo, nextTransactionId, request);
                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
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
                    AmqpSession session = connection.getSession(sessionId);
                    session.recover();
                    pumpToProtonTransport(request);
                    request.onSuccess();
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    @Override
    public void unsubscribe(final String subscription, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    connection.unsubscribe(subscription, request);
                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
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
                    AmqpConsumer consumer = null;

                    if (consumerId.getProviderHint() instanceof AmqpConsumer) {
                        consumer = (AmqpConsumer) consumerId.getProviderHint();
                    } else {
                        AmqpSession session = connection.getSession(consumerId.getParentId());
                        consumer = session.getConsumer(consumerId);
                    }

                    consumer.pull(timeout, request);
                    pumpToProtonTransport(request);
                } catch (Throwable t) {
                    request.onFailure(t);
                }
            }
        });
    }

    //---------- Event handlers and Utility methods  -------------------------//

    private void updateTracer() {
        if (isTraceFrames()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("RECV: {}", transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("SENT: {}", transportFrame.getBody());
                }
            });
        }
    }

    @Override
    public void onData(final ByteBuf input) {

        // We need to retain until the serializer gets around to processing it.
        ReferenceCountUtil.retain(input);

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    if (isTraceBytes()) {
                        TRACE_BYTES.info("Received: {}", ByteBufUtil.hexDump(input));
                    }

                    ByteBuffer source = input.nioBuffer();

                    do {
                        ByteBuffer buffer = protonTransport.getInputBuffer();
                        int limit = Math.min(buffer.remaining(), source.remaining());
                        ByteBuffer duplicate = source.duplicate();
                        duplicate.limit(source.position() + limit);
                        buffer.put(duplicate);
                        protonTransport.processInput().checkIsOk();
                        source.position(source.position() + limit);
                    } while (source.hasRemaining());

                    ReferenceCountUtil.release(input);

                    // Process the state changes from the latest data and then answer back
                    // any pending updates to the Broker.
                    processUpdates();
                    pumpToProtonTransport();
                } catch (Throwable t) {
                    LOG.warn("Caught problem during data processing: {}", t.getMessage(), t);

                    fireProviderException(t);
                }
            }
        });
    }

    /**
     * Callback method for the Transport to report connection errors.  When called
     * the method will queue a new task to fire the failure error back to the listener.
     *
     * @param error
     *        the error that causes the transport to fail.
     */
    @Override
    public void onTransportError(final Throwable error) {
        if (!serializer.isShutdown()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Transport failed: {}", error.getMessage());
                    if (!closed.get()) {
                        // We can't send any more output, so close the transport
                        protonTransport.close_head();
                        fireProviderException(error);
                    }
                }
            });
        }
    }

    public void scheduleExecuteAndPump(Runnable task) {
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        task.run();
                    } finally {
                        pumpToProtonTransport();
                    }
                } catch (Throwable t) {
                    LOG.warn("Caught problem during task processing: {}", t.getMessage(), t);

                    fireProviderException(t);
                }
            }
        });
    }

    /**
     * Callback method for the Transport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raised
     * to the registered ProviderListener to indicate connection loss.
     */
    @Override
    public void onTransportClosed() {
        if (!serializer.isShutdown()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.debug("Transport connection remotely closed");
                    if (!closed.get()) {
                        // We can't send any more output, so close the transport
                        protonTransport.close_head();
                        fireProviderException(new IOException("Transport connection remotely closed."));
                    }
                }
            });
        }
    }

    private void processUpdates() {
        try {
            Event protonEvent = null;
            while ((protonEvent = protonCollector.peek()) != null) {
                if (!protonEvent.getType().equals(Type.TRANSPORT)) {
                    LOG.trace("New Proton Event: {}", protonEvent.getType());
                }

                AmqpEventSink amqpEventSink = null;
                switch (protonEvent.getType()) {
                    case CONNECTION_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getConnection().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteClose(this);
                        }
                        break;
                    case CONNECTION_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getConnection().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteOpen(this);
                        }
                        break;
                    case SESSION_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getSession().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteClose(this);
                        }
                        break;
                    case SESSION_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getSession().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteOpen(this);
                        }
                        break;
                    case LINK_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteClose(this);
                        }
                        break;
                    case LINK_REMOTE_DETACH:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteDetach(this);
                        }
                        break;
                    case LINK_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processRemoteOpen(this);
                        }
                        break;
                    case LINK_FLOW:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processFlowUpdates(this);
                        }
                        break;
                    case DELIVERY:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        if (amqpEventSink != null) {
                            amqpEventSink.processDeliveryUpdates(this, (Delivery) protonEvent.getContext());
                        }
                        break;
                    default:
                        break;
                }

                protonCollector.pop();
            }

            // We have to do this to pump SASL bytes in as SASL is not event driven yet.
            processSaslAuthentication();
        } catch (Throwable t) {
            try {
                LOG.warn("Caught problem during update processing: {}", t.getMessage(), t);
            } finally {
                fireProviderException(t);
            }
        }
    }

    private void processSaslAuthentication() {
        if (authenticator == null) {
            return;
        }

        try {
            authenticator.tryAuthenticate();

            if (authenticator.isComplete()) {
                if (!authenticator.wasSuccessful()) {
                    // Close the transport to avoid emitting any additional frames if the
                    // authentication process was unsuccessful, then signal the completion
                    // to avoid any race with the caller triggering any other traffic.
                    // Don't release the authenticator as we need it on close to know what
                    // the state of authentication was.
                    org.apache.qpid.proton.engine.Transport t = protonConnection.getTransport();
                    t.close_head();
                    connectionRequest.onFailure(authenticator.getFailureCause());
                } else {
                    // Signal completion and release the authenticator we won't use it again.
                    connectionRequest.onSuccess();
                    authenticator = null;
                }
            }
        } catch (Throwable ex) {
            try {
                org.apache.qpid.proton.engine.Transport t = protonConnection.getTransport();
                t.close_head();
            } finally {
                fireProviderException(ex);
            }
        }
    }

    protected boolean pumpToProtonTransport() {
        return pumpToProtonTransport(NOOP_REQUEST);
    }

    protected boolean pumpToProtonTransport(AsyncResult request) {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    ByteBuf outbound = transport.allocateSendBuffer(toWrite.remaining());
                    outbound.writeBytes(toWrite);

                    if (isTraceBytes()) {
                        TRACE_BYTES.info("Sending: {}", ByteBufUtil.hexDump(outbound));
                    }

                    transport.send(outbound);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireProviderException(e);
            request.onFailure(e);
            return false;
        }

        return true;
    }

    void fireConnectionEstablished() {
        // The request onSuccess calls this method
        connectionRequest = null;

        // Using nano time since it is not related to the wall clock, which may change
        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long deadline = protonTransport.tick(now);
        if (deadline > 0) {
            long delay = deadline - now;
            LOG.trace("IdleTimeoutCheck being initiated, initial delay: {}", delay);
            nextIdleTimeoutCheck = serializer.schedule(new IdleTimeoutCheck(), delay, TimeUnit.MILLISECONDS);
        }

        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onConnectionEstablished(remoteURI);
        }
    }

    void fireNonFatalProviderException(Exception ex) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onProviderException(ex);
        }
    }

    void fireProviderException(Throwable ex) {
        if (connectionRequest != null) {
            connectionRequest.onFailure(ex);
            connectionRequest = null;
        }

        if (nextIdleTimeoutCheck != null) {
            nextIdleTimeoutCheck.cancel(true);
            nextIdleTimeoutCheck = null;
        }

        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onConnectionFailure(IOExceptionSupport.create(ex));
        }
    }

    void fireResourceClosed(JmsResource resource, Throwable cause) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onResourceClosed(resource, cause);
        }
    }

    public void fireRemotesDiscovered(List<URI> remotes) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onRemoteDiscovery(remotes);
        }
    }

    @Override
    public void addChildResource(AmqpResource resource) {
        if (resource instanceof AmqpConnection) {
            this.connection = (AmqpConnection) resource;
        }
    }

    @Override
    public void removeChildResource(AmqpResource resource) {
        // No need to remove resources
    }

    //---------- Property Setters and Getters --------------------------------//

    @Override
    public JmsMessageFactory getMessageFactory() {
        if (connection == null) {
            throw new RuntimeException("Message Factory is not accessible when not connected.");
        }
        return connection.getAmqpMessageFactory();
    }

    public void setTraceFrames(boolean trace) {
        this.traceFrames = trace;
        updateTracer();
    }

    public boolean isTraceFrames() {
        return this.traceFrames;
    }

    public void setTraceBytes(boolean trace) {
        this.traceBytes = trace;
    }

    public boolean isTraceBytes() {
        return this.traceBytes;
    }

    public boolean isSaslLayer() {
        return saslLayer;
    }

    /**
     * Sets whether a sasl layer is used for the connection or not.
     *
     * @param saslLayer true to enable the sasl layer, false to disable it.
     */
    public void setSaslLayer(boolean saslLayer) {
        this.saslLayer = saslLayer;
    }

    public Set<String> getSaslMechanisms() {
        return saslMechanisms;
    }

    /**
     * Sets a selection of mechanisms to restrict the choice to, enabling only
     * a subset of the servers offered mechanisms to be selectable.
     *
     * @param saslMechanisms the mechanisms to restrict choice to, or null not to restrict.
     */
    public void setSaslMechanisms(String[] saslMechanisms) {
        Set<String> saslMechanismSet = null;

        if (saslMechanisms != null && saslMechanisms.length > 0) {
            Set<String> mechs = new HashSet<String>();
            for (int i = 0; i < saslMechanisms.length; i++) {
                String mech = saslMechanisms[i];
                if (!mech.trim().isEmpty()) {
                    mechs.add(mech);
                }
            }

            if (!mechs.isEmpty()) {
                saslMechanismSet = mechs;
            }
        }

        this.saslMechanisms = saslMechanismSet;
    }

    public String getVhost() {
        return vhost;
    }

    /**
     * Sets the hostname to be used in the AMQP SASL Init and Open frames.
     *
     * If set null, the host provided in the remoteURI will be used. If set to
     * the empty string, the hostname field of the frames will be cleared.
     *
     * @param vhost the hostname to include in SASL Init and Open frames.
     */
    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets the idle timeout (in milliseconds) after which the connection will
     * be closed if the peer has not send any data. The provided value will be
     * halved before being transmitted as our advertised idle-timeout in the
     * AMQP Open frame.
     *
     * @param idleTimeout the timeout in milliseconds.
     */
    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getDrainTimeout() {
        return drainTimeout;
    }

    /**
     * Sets the drain timeout (in milliseconds) after which a consumer will be
     * treated as having failed and will be closed due to unknown state of the
     * remote having not responded to the requested drain.
     *
     * @param drainTimeout
     *      the drainTimeout to use for receiver links.
     */
    public void setDrainTimeout(int drainTimeout) {
        this.drainTimeout = drainTimeout;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the max frame size (in bytes).
     *
     * Values of -1 indicates to use the proton default.
     *
     * @param maxFrameSize the frame size in bytes.
     */
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public long getSessionOutgoingWindow() {
        return sessionOutoingWindow;
    }

    /**
     * Sets the outgoing window size for the AMQP session. Values may
     * be between -1 and 2^32-1, where -1 indicates to use the default.
     *
     * @param sessionOutoingWindow the outgoing window size
     */
    public void setSessionOutgoingWindow(long sessionOutoingWindow) {
        this.sessionOutoingWindow = sessionOutoingWindow;
    }

    public boolean isAllowNonSecureRedirects() {
        return allowNonSecureRedirects;
    }

    /**
     * Should the AMQP connection allow a redirect or failover server update that redirects
     * from a secure connection to an non-secure one (SSL to TCP).
     *
     * @param allowNonSecureRedirects
     * 		the allowNonSecureRedirects value to apply to this AMQP connection.
     */
    public void setAllowNonSecureRedirects(boolean allowNonSecureRedirects) {
        this.allowNonSecureRedirects = allowNonSecureRedirects;
    }

    public long getCloseTimeout() {
        return connectionInfo != null ? connectionInfo.getCloseTimeout() : JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    }

    public long getConnectTimeout() {
        return connectionInfo != null ? connectionInfo.getConnectTimeout() : JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    }

    public long getRequestTimeout() {
        return connectionInfo != null ? connectionInfo.getRequestTimeout() : JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
    }

    public long getSendTimeout() {
        return connectionInfo != null ? connectionInfo.getSendTimeout() : JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    }

    @Override
    public String toString() {
        return "AmqpProvider: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    public Transport getTransport() {
        return transport;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    public org.apache.qpid.proton.engine.Transport getProtonTransport() {
        return protonTransport;
    }

    public Connection getProtonConnection() {
        return protonConnection;
    }

    ScheduledExecutorService getScheduler() {
        return this.serializer;
    }

    @Override
    public AmqpProvider getProvider() {
        return this;
    }

    /**
     * Allows a resource to request that its parent resource schedule a future
     * cancellation of a request and return it a {@link Future} instance that
     * can be used to cancel the scheduled automatic failure of the request.
     *
     * @param request
     *      The request that should be marked as failed based on configuration.
     * @param timeout
     *      The time to wait before marking the request as failed.
     * @param error
     *      The error to use when failing the pending request.
     *
     * @return a {@link ScheduledFuture} that can be stored by the caller.
     */
    public ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult request, long timeout, final Exception error) {
        if (timeout != JmsConnectionInfo.INFINITE) {
            return serializer.schedule(new Runnable() {

                @Override
                public void run() {
                    request.onFailure(error);
                    pumpToProtonTransport();
                }

            }, timeout, TimeUnit.MILLISECONDS);
        }

        return null;
    }

    /**
     * Allows a resource to request that its parent resource schedule a future
     * cancellation of a request and return it a {@link Future} instance that
     * can be used to cancel the scheduled automatic failure of the request.
     *
     * @param request
     *      The request that should be marked as failed based on configuration.
     * @param timeout
     *      The time to wait before marking the request as failed.
     * @param builder
     *      An AmqpExceptionBuilder to use when creating a timed out exception.
     *
     * @return a {@link ScheduledFuture} that can be stored by the caller.
     */
    public ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult request, long timeout, final AmqpExceptionBuilder builder) {
        if (timeout != JmsConnectionInfo.INFINITE) {
            return serializer.schedule(new Runnable() {

                @Override
                public void run() {
                    request.onFailure(builder.createException());
                    pumpToProtonTransport();
                }

            }, timeout, TimeUnit.MILLISECONDS);
        }

        return null;
    }

    //----- Internal implementation ------------------------------------------//

    private void checkClosed() throws ProviderClosedException {
        if (closed.get()) {
            throw new ProviderClosedException("This Provider is already closed");
        }
    }

    private Mechanism findSaslMechanism(String[] remoteMechanisms) throws JMSSecurityRuntimeException {

        Mechanism mechanism = SaslMechanismFinder.findMatchingMechanism(
            connectionInfo.getUsername(), connectionInfo.getPassword(), transport.getLocalPrincipal(), saslMechanisms, remoteMechanisms);

        mechanism.setUsername(connectionInfo.getUsername());
        mechanism.setPassword(connectionInfo.getPassword());

        try {
            Map<String, String> saslOptions = PropertyUtil.filterProperties(PropertyUtil.parseQuery(getRemoteURI()), "sasl.options.");
            if (!saslOptions.containsKey("serverName")) {
                saslOptions.put("serverName", remoteURI.getHost());
            }

            mechanism.init(Collections.unmodifiableMap(saslOptions));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to apply sasl options to mechanism: " + mechanism.getName() + ", reason: " + ex.toString(), ex);
        }

        return mechanism;
    }

    private final class IdleTimeoutCheck implements Runnable {
        @Override
        public void run() {
            boolean checkScheduled = false;

            if (connection.getLocalState() == EndpointState.ACTIVE) {
                // Using nano time since it is not related to the wall clock, which may change
                long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                long deadline = protonTransport.tick(now);

                boolean pumpSucceeded = pumpToProtonTransport();

                if (protonTransport.isClosed()) {
                    LOG.info("IdleTimeoutCheck closed the transport due to the peer exceeding our requested idle-timeout.");
                    if (pumpSucceeded) {
                        fireProviderException(new IOException("Transport closed due to the peer exceeding our requested idle-timeout"));
                    }
                } else {
                    if (deadline > 0) {
                        long delay = deadline - now;
                        checkScheduled = true;
                        LOG.trace("IdleTimeoutCheck rescheduling with delay: {}", delay);
                        nextIdleTimeoutCheck = serializer.schedule(this, delay, TimeUnit.MILLISECONDS);
                    }
                }
            } else {
                LOG.trace("IdleTimeoutCheck skipping check, connection is not active.");
            }

            if (!checkScheduled) {
                nextIdleTimeoutCheck = null;
                LOG.trace("IdleTimeoutCheck exiting");
            }
        }
    }
}
