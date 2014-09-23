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
package org.apache.qpid.jms.provider.amqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
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
import org.apache.qpid.jms.provider.AbstractProvider;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.transports.TcpTransport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

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
public class AmqpProvider extends AbstractProvider implements TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProvider.class);

    private static final Logger TRACE_BYTES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".BYTES");
    private static final Logger TRACE_FRAMES = LoggerFactory.getLogger(AmqpConnection.class.getPackage().getName() + ".FRAMES");
    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;

    private AmqpConnection connection;
    private org.apache.qpid.jms.transports.Transport transport;
    private boolean traceFrames;
    private boolean traceBytes;
    private boolean presettleConsumers;
    private boolean presettleProducers;
    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private long requestTimeout = JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
    private long sendTimeout = JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;

    private final Transport protonTransport = Transport.Factory.create();
    private final Collector protonCollector = new CollectorImpl();

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     */
    public AmqpProvider(URI remoteURI) {
        this(remoteURI, null);
    }

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     */
    public AmqpProvider(URI remoteURI, Map<String, String> extraOptions) {
        super(remoteURI);
        updateTracer();
    }

    @Override
    public void connect() throws IOException {
        checkClosed();

        transport = createTransport(getRemoteURI());

        Map<String, String> map = Collections.emptyMap();
        try {
            map = PropertyUtil.parseQuery(remoteURI.getQuery());
        } catch (Exception e) {
            IOExceptionSupport.create(e);
        }
        Map<String, String> providerOptions = PropertyUtil.filterProperties(map, "transport.");

        if (!PropertyUtil.setProperties(transport, providerOptions)) {
            String msg = ""
                + " Not all transport options could be set on the AMQP Provider transport."
                + " Check the options are spelled correctly."
                + " Given parameters=[" + providerOptions + "]."
                + " This provider instance cannot be started.";
            throw new IOException(msg);
        }

        transport.connect();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderFuture request = new ProviderFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        // If we are not connected then there is nothing we can do now
                        // just signal success.
                        if (!transport.isConnected()) {
                            request.onSuccess();
                        }

                        if (connection != null) {
                            connection.close(request);
                        } else {
                            request.onSuccess();
                        }

                        pumpToProtonTransport();
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection");
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
                if (transport != null) {
                    try {
                        transport.close();
                    } catch (Exception e) {
                        LOG.debug("Cuaght exception while closing down Transport: {}", e.getMessage());
                    }
                }

                if (serializer != null) {
                    serializer.shutdown();
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
                            AmqpSession session = connection.createSession(sessionInfo);
                            session.open(request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            AmqpProducer producer = session.createProducer(producerInfo);
                            producer.open(request);
                        }

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.createConsumer(consumerInfo);
                            consumer.open(request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            closeTimeout = connectionInfo.getCloseTimeout();
                            connectTimeout = connectionInfo.getConnectTimeout();
                            sendTimeout = connectionInfo.getSendTimeout();
                            requestTimeout = connectionInfo.getRequestTimeout();

                            Connection protonConnection = Connection.Factory.create();
                            protonTransport.setMaxFrameSize(getMaxFrameSize());
                            protonTransport.bind(protonConnection);
                            protonConnection.collect(protonCollector);
                            Sasl sasl = protonTransport.sasl();
                            if (sasl != null) {
                                sasl.client();
                            }
                            connection = new AmqpConnection(AmqpProvider.this, protonConnection, sasl, connectionInfo);
                            connection.open(request);
                        }

                        @Override
                        public void processDestination(JmsDestination destination) throws Exception {
                            if (destination.isTemporary()) {
                                AmqpTemporaryDestination temporary = connection.createTemporaryDestination(destination);
                                temporary.open(request);
                            } else {
                                request.onSuccess();
                            }
                        }

                        @Override
                        public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
                            AmqpSession session = connection.getSession(transactionInfo.getParentId());
                            session.begin(transactionInfo.getTransactionId(), request);
                        }
                    });

                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
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

                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult request) throws IOException {
        //TODO: improve or delete this logging
        LOG.debug("Destroy called");

        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    //TODO: improve or delete this logging
                    LOG.debug("Processing resource destroy request");
                    checkClosed();
                    resource.visit(new JmsDefaultResourceVisitor() {

                        @Override
                        public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                            AmqpSession session = connection.getSession(sessionInfo.getSessionId());
                            session.close(request);
                        }

                        @Override
                        public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                            AmqpSession session = connection.getSession(producerInfo.getParentId());
                            AmqpProducer producer = session.getProducer(producerInfo);
                            producer.close(request);
                        }

                        @Override
                        public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                            AmqpSession session = connection.getSession(consumerInfo.getParentId());
                            AmqpConsumer consumer = session.getConsumer(consumerInfo);
                            consumer.close(request);
                        }

                        @Override
                        public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                            connection.close(request);
                        }

                        @Override
                        public void processDestination(JmsDestination destination) throws Exception {
                            // TODO - Delete remote temporary Topic or Queue
                            request.onSuccess();
                        }
                    });

                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
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

                    boolean couldSend = producer.send(envelope, request);
                    pumpToProtonTransport();
                    if (couldSend && envelope.isSendAsync()) {
                        request.onSuccess();
                    }
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession amqpSession = connection.getSession(sessionId);
                    amqpSession.acknowledge();
                    pumpToProtonTransport();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
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
                        pumpToProtonTransport();
                    } else {
                        pumpToProtonTransport();
                        request.onSuccess();
                    }
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void commit(final JmsSessionId sessionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession session = connection.getSession(sessionId);
                    session.commit(request);
                    pumpToProtonTransport();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    @Override
    public void rollback(final JmsSessionId sessionId, final AsyncResult request) throws IOException {
        checkClosed();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    checkClosed();
                    AmqpSession session = connection.getSession(sessionId);
                    session.rollback(request);
                    pumpToProtonTransport();
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
                    AmqpSession session = connection.getSession(sessionId);
                    session.recover();
                    pumpToProtonTransport();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
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
                    pumpToProtonTransport();
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
                    AmqpConsumer consumer = null;

                    if (consumerId.getProviderHint() instanceof AmqpConsumer) {
                        consumer = (AmqpConsumer) consumerId.getProviderHint();
                    } else {
                        AmqpSession session = connection.getSession(consumerId.getParentId());
                        consumer = session.getConsumer(consumerId);
                    }

                    consumer.pull(timeout);
                    pumpToProtonTransport();
                    request.onSuccess();
                } catch (Exception error) {
                    request.onFailure(error);
                }
            }
        });
    }

    /**
     * Provides an extension point for subclasses to insert other types of transports such
     * as SSL etc.
     *
     * @param remoteLocation
     *        The remote location where the transport should attempt to connect.
     *
     * @return the newly created transport instance.
     */
    protected org.apache.qpid.jms.transports.Transport createTransport(URI remoteLocation) {
        return new TcpTransport(this, remoteLocation);
    }

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
    public void onData(Buffer input) {

        // Create our own copy since we will process later.
        final ByteBuffer source = ByteBuffer.wrap(input.getBytes());

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                LOG.trace("Received from Broker {} bytes:", source.remaining());

                do {
                    ByteBuffer buffer = protonTransport.getInputBuffer();
                    int limit = Math.min(buffer.remaining(), source.remaining());
                    ByteBuffer duplicate = source.duplicate();
                    duplicate.limit(source.position() + limit);
                    buffer.put(duplicate);
                    protonTransport.processInput();
                    source.position(source.position() + limit);
                } while (source.hasRemaining());

                // Process the state changes from the latest data and then answer back
                // any pending updates to the Broker.
                processUpdates();
                pumpToProtonTransport();
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
                        fireProviderException(error);
                        if (connection != null) {
                            connection.closed();
                        }
                    }
                }
            });
        }
    }

    /**
     * Callback method for the Transport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raied
     * to the registered ProviderListener to indicate connection loss.
     */
    @Override
    public void onTransportClosed() {
        // TODO: improve or delete this logging
        LOG.debug("onTransportClosed listener called");
        if (!serializer.isShutdown()) {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.debug("Transport connection remotely closed");
                    if (!closed.get()) {
                        fireProviderException(new IOException("Connection remotely closed."));
                        if (connection != null) {
                            connection.closed();
                        }
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

                AmqpResource amqpResource = null;
                switch (protonEvent.getType()) {
                    case CONNECTION_REMOTE_CLOSE:
                    case CONNECTION_REMOTE_OPEN:
                        AmqpConnection connection = (AmqpConnection) protonEvent.getConnection().getContext();
                        connection.processStateChange();
                        break;
                    case SESSION_REMOTE_CLOSE:
                    case SESSION_REMOTE_OPEN:
                        AmqpSession session = (AmqpSession) protonEvent.getSession().getContext();
                        session.processStateChange();
                        break;
                    case LINK_REMOTE_CLOSE:
                    case LINK_REMOTE_OPEN:
                        AmqpResource resource = (AmqpResource) protonEvent.getLink().getContext();
                        resource.processStateChange();
                        break;
                    case LINK_FLOW:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processFlowUpdates();
                        break;
                    case DELIVERY:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processDeliveryUpdates();
                        break;
                    default:
                        break;
                }

                protonCollector.pop();
            }

            // We have to do this to pump SASL bytes in as SASL is not event driven yet.
            if (connection != null) {
                connection.processSaslAuthentication();
            }
        } catch (Exception ex) {
            LOG.warn("Caught Exception during update processing: {}", ex.getMessage(), ex);
            fireProviderException(ex);
        }
    }

    private void pumpToProtonTransport() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    // TODO - Get Bytes in a readable form
                    if (isTraceBytes()) {
                        TRACE_BYTES.info("Sending: {}", toWrite.toString());
                    }
                    transport.send(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireProviderException(e);
        }
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

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public long getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public void setPresettle(boolean presettle) {
        setPresettleConsumers(presettle);
        setPresettleProducers(presettle);
    }

    public boolean isPresettleConsumers() {
        return this.presettleConsumers;
    }

    public void setPresettleConsumers(boolean presettle) {
        this.presettleConsumers = presettle;
    }

    public boolean isPresettleProducers() {
        return this.presettleProducers;
    }

    public void setPresettleProducers(boolean presettle) {
        this.presettleProducers = presettle;
    }

    /**
     * @return the currently set Max Frame Size value.
     */
    public int getMaxFrameSize() {
        return DEFAULT_MAX_FRAME_SIZE;
    }

    @Override
    public String toString() {
        return "AmqpProvider: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }
}
