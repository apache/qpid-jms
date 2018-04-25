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
package org.apache.qpid.jms.transports.netty;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.CONTAINER_ID;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.INVALID_FIELD;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PLATFORM;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PRODUCT;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SOLE_CONNECTION_CAPABILITY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Simple Netty based server that can handle a small subset of AMQP events
 * using Proton-J as the protocol engine.
 */
@SuppressWarnings( "unused" )
public class NettySimpleAmqpServer extends NettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(NettySimpleAmqpServer.class);

    private static final AtomicInteger SERVER_SEQUENCE = new AtomicInteger();

    private static final int CHANNEL_MAX = 32767;
    private static final int HEADER_SIZE = 8;
    private static final int SASL_PROTOCOL = 3;

    private final Map<String, List<Connection>> connections = new HashMap<String, List<Connection>>();
    private final ScheduledExecutorService serializer;

    private boolean allowNonSaslConnections;
    private ConnectionIntercepter connectionIntercepter;

    public NettySimpleAmqpServer(TransportOptions options, boolean secure) {
        this(options, false, false);
    }

    public NettySimpleAmqpServer(TransportOptions options, boolean secure, boolean needClientAuth) {
        this(options, secure, needClientAuth, false);
    }

    public NettySimpleAmqpServer(TransportOptions options, boolean secure, boolean needClientAuth, boolean webSocketServer) {
        super(options, secure, needClientAuth, webSocketServer);

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(NettySimpleAmqpServer.this.getClass().getSimpleName() + ":(" +
                               SERVER_SEQUENCE.incrementAndGet() + "):Worker");
                return serial;
            }
        });
    }

    @Override
    protected ChannelHandler getServerHandler() {
        return new ProtonConnection();
    }

    /**
     * @return true if this server allows non-SASL connection.
     */
    public boolean isAllowNonSaslConnections() {
        return allowNonSaslConnections;
    }

    /**
     * Used to control whether the server allows non-SASL connections or not.
     *
     * @param allowNonSaslConnections
     *      the configuration for allowing non-SASL connections
     */
    public void setAllowNonSaslConnections(boolean allowNonSaslConnections) {
        this.allowNonSaslConnections = allowNonSaslConnections;
    }

    //----- Connection Handler -----------------------------------------------//

    private final class ProtonConnection extends SimpleChannelInboundHandler<ByteBuf>  {

        private final IdGenerator sessionIdGenerator = new IdGenerator();

        private final Transport protonTransport = Proton.transport();
        private final Connection protonConnection = Proton.connection();
        private final Collector eventCollector = new CollectorImpl();
        private SaslAuthenticator authenticator;

        private final Map<String, ProtonSession> sessions = new HashMap<String, ProtonSession>();

        private boolean exclusiveContainerId;
        private boolean headerRead;
        private final ByteBuf headerBuf = Unpooled.buffer(HEADER_SIZE, HEADER_SIZE);

        public ProtonConnection() {
            this.protonTransport.bind(this.protonConnection);
            this.protonTransport.setChannelMax(CHANNEL_MAX);
            this.protonTransport.setEmitFlowEventOnSend(false);

            this.protonConnection.collect(eventCollector);
        }

        public boolean isExclusiveContainerId() {
            return exclusiveContainerId;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf input) {
            LOG.debug("AMQP Server Channel read: {}", input);

            ChannelFutureListener writeCompletionAction = null;

            try {
                if (!headerRead) {
                    processIncomingHeader(ctx, input);
                } else {
                    pourIntoProton(input);
                }
            } catch (Throwable e) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                return;
            }

            try {
                if (authenticator != null) {
                    processSaslExchange();
                } else {
                    processProtonEvents();
                }
            } catch (Throwable e) {
                writeCompletionAction = ChannelFutureListener.CLOSE;
            } finally {
                if (protonConnection.getLocalState() == EndpointState.CLOSED) {
                    writeCompletionAction = ChannelFutureListener.CLOSE;
                }

                pumpProtonToChannel(ctx, writeCompletionAction);
            }
        }

        private void processIncomingHeader(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
            headerBuf.writeBytes(buffer, Math.min(HEADER_SIZE, buffer.readableBytes()));

            if (headerBuf.writableBytes() == 0) {
                headerRead = true;
                AmqpHeader header = new AmqpHeader(headerBuf);

                if (isHeaderValid(header, authenticator != null)) {
                    LOG.trace("Connection from an AMQP v1.0 client initiated. {}", header);
                } else {
                    LOG.warn("Connection attempt from unsupported AMQP client. {}", header);
                    AmqpHeader reply = getMinimallySupportedHeader();
                    ctx.write(reply.getBuffer());
                    throw new IOException("Connection from client using unsupported AMQP attempted");
                }

                switch (header.getProtocolId()) {
                    case 0: // non-SASL
                        authenticator = null;
                        break;
                    case 3: // SASL
                        authenticator = new SaslAuthenticator(this);
                        break;
                    default:
                }

                pourIntoProton(headerBuf);
            }

            pourIntoProton(buffer);
        }

        private void processSaslExchange() {
            authenticator.processSaslExchange();
            if (authenticator.isDone()) {
                headerRead = false;
                headerBuf.resetWriterIndex();
            }
        }

        private void processProtonEvents() throws Exception {
            Event event = null;
            while ((event = eventCollector.peek()) != null) {
                LOG.trace("Server: Processing event: {}", event.getType());
                switch (event.getType()) {
                    case CONNECTION_REMOTE_OPEN:
                        processConnectionOpen(event.getConnection());
                        break;
                    case CONNECTION_REMOTE_CLOSE:
                        processConnectionClose(event.getConnection());
                        break;
                    case SESSION_REMOTE_OPEN:
                        processSessionOpen(event.getSession());
                        break;
                    case SESSION_REMOTE_CLOSE:
                        processSessionClose(event.getSession());
                        break;
                    case LINK_REMOTE_OPEN:
                        //processLinkOpen(event.getLink());
                        break;
                    case LINK_REMOTE_DETACH:
                        //processLinkDetach(event.getLink());
                        break;
                    case LINK_REMOTE_CLOSE:
                        //processLinkClose(event.getLink());
                        break;
                    case LINK_FLOW:
                        //processLinkFlow(event.getLink());
                        break;
                    case DELIVERY:
                        //processDelivery(event.getDelivery());
                        break;
                    default:
                        break;
                }

                eventCollector.pop();
            }
        }

        protected void processConnectionOpen(Connection connection) throws Exception {
            ErrorCondition failure = null;

            protonConnection.setContext(this);
            protonConnection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
            protonConnection.setProperties(getConnetionProperties());
            protonConnection.setContainer("Netty-Server");
            protonConnection.open();

            if (connectionIntercepter != null) {
                failure = connectionIntercepter.interceptConnectionAttempt(connection);
            }

            if (failure == null) {
                failure = handleConnectionRegistration(connection);
            }

            if (failure != null) {
                protonConnection.setProperties(getFailedConnetionProperties());
                protonConnection.setCondition(failure);
                protonConnection.close();
            }
        }

        protected void processConnectionClose(Connection connection) throws Exception {
            unregisterConnection(connection);

            protonConnection.close();
            protonConnection.free();
        }

        private void processSessionClose(Session session) {
            ProtonSession protonSession = (ProtonSession) session.getContext();

            sessions.remove(protonSession.getId());

            session.close();
            session.free();
        }

        private void processSessionOpen(Session session) {
            ProtonSession protonSession = new ProtonSession(sessionIdGenerator.generateId(), session);
            sessions.put(protonSession.getId(), protonSession);
            session.open();
        }

        void pumpProtonToChannel(ChannelHandlerContext ctx, ChannelFutureListener writeCompletionAction) {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    LOG.trace("Server: Sending {} bytes out", toWrite.limit());
                    ctx.write(Unpooled.wrappedBuffer(toWrite));
                    toWrite.position(toWrite.limit());
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }

            if (writeCompletionAction != null) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(writeCompletionAction);
            } else {
                ctx.flush();
            }
        }

        private void pourIntoProton(ByteBuf buffer) {
            if (buffer.readableBytes() > 0) {
                ByteBuffer source = buffer.nioBuffer();

                do {
                    ByteBuffer input = protonTransport.getInputBuffer();
                    int limit = Math.min(input.remaining(), source.remaining());
                    ByteBuffer duplicate = source.duplicate();
                    duplicate.limit(source.position() + limit);
                    input.put(duplicate);
                    protonTransport.processInput();
                    source.position(source.position() + limit);
                } while (source.hasRemaining());
            }
        }

        private ErrorCondition handleConnectionRegistration(Connection connection) {
            ErrorCondition failure = null;

            Symbol[] desiredCapabilities = connection.getRemoteDesiredCapabilities();
            for (Symbol desiredCapability : desiredCapabilities) {
                if (desiredCapability.equals(SOLE_CONNECTION_CAPABILITY)) {
                    exclusiveContainerId = true;
                }
            }

            String containerId = connection.getContainer();
            if (exclusiveContainerId) {
                if (containerId == null || containerId.isEmpty()) {
                    failure = new ErrorCondition(AmqpError.INVALID_FIELD,
                        "Connection with sole connection capability must provide a container ID");

                    Map<Symbol, Object> infoMap = new HashMap<Symbol, Object> ();
                    infoMap.put(INVALID_FIELD, CONTAINER_ID);
                    failure.setInfo(infoMap);
                }
            }

            if (failure == null && !registerNewConnection(connection, exclusiveContainerId)) {
                failure = new ErrorCondition(AmqpError.INVALID_FIELD,
                    "Client already connected with exclusive container Id.");

                Map<Symbol, Object> infoMap = new HashMap<Symbol, Object> ();
                infoMap.put(INVALID_FIELD, CONTAINER_ID);
                failure.setInfo(infoMap);
            }

            return failure;
        }
    }

    //----- Server Helper Methods --------------------------------------------//

    private synchronized boolean registerNewConnection(Connection connection, boolean exclusiveContainerId) {
        String containerId = connection.getContainer();

        List<Connection> connectionList = this.connections.get(containerId);
        if (connectionList == null) {
            connectionList = new LinkedList<Connection>();
            connections.put(containerId, connectionList);
        }

        if (exclusiveContainerId && !connectionList.isEmpty()) {
            return false;
        } else {
            Connection head = connectionList.isEmpty() ? null : connectionList.get(0);
            if (head != null) {
                ProtonConnection protonConnection = (ProtonConnection)head.getContext();
                if (protonConnection.isExclusiveContainerId()) {
                    return false;
                }
            }

            connectionList.add(connection);
            return true;
        }
    }

    private synchronized void unregisterConnection(Connection connection) {
        String containerId = connection.getContainer();

        List<Connection> connectionList = this.connections.get(containerId);
        if (connectionList != null) {
            Iterator<Connection> iterator = connectionList.iterator();
            while (iterator.hasNext()) {
                Connection registeredConnection = iterator.next();
                if (registeredConnection.equals(connection)) {
                    iterator.remove();
                    return;
                }
            }
        }
    }

    private Symbol[] getConnectionCapabilitiesOffered() {
        return new Symbol[]{ ANONYMOUS_RELAY, DELAYED_DELIVERY };
    }

    private Map<Symbol, Object> getConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(PRODUCT, "Qpid-JMS-NettyServer");
        properties.put(VERSION, "1.0");
        properties.put(PLATFORM, "java");

        return properties;
    }

    private Map<Symbol, Object> getFailedConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(CONNECTION_OPEN_FAILED, true);

        return properties;
    }

    private boolean isHeaderValid(AmqpHeader header, boolean authenticated) {
        if (!header.hasValidPrefix()) {
            return false;
        }

        if (!(header.getProtocolId() == 0 || header.getProtocolId() == SASL_PROTOCOL)) {
            return false;
        }

        if (!authenticated && !isAllowNonSaslConnections() && header.getProtocolId() != SASL_PROTOCOL) {
            return false;
        }

        if (header.getMajor() != 1 || header.getMinor() != 0 || header.getRevision() != 0) {
            return false;
        }

        return true;
    }

    private AmqpHeader getMinimallySupportedHeader() {
        AmqpHeader header = new AmqpHeader();
        if (!isAllowNonSaslConnections()) {
            header.setProtocolId(3);
        }

        return header;
    }

    //----- Server Event Intercepter -----------------------------------------//

    public ConnectionIntercepter getConnectionIntercepter() {
        return connectionIntercepter;
    }

    public void setConnectionIntercepter(ConnectionIntercepter connectionIntercepter) {
        this.connectionIntercepter = connectionIntercepter;
    }

    public interface ConnectionIntercepter {

        ErrorCondition interceptConnectionAttempt(Connection connection);

    }

    //----- Session Manager --------------------------------------------------//

    private class ProtonSession {

        private final String sessionId;
        private final Session session;

        private Map<String, ProtonSender> senders = new HashMap<String, ProtonSender>();
        private Map<String, ProtonReceiver> receivers = new HashMap<String, ProtonReceiver>();

        public ProtonSession(String sessionId, Session session) {
            this.sessionId = sessionId;
            this.session = session;
            this.session.setContext(this);
        }

        public Session getSession() {
            return session;
        }

        public String getId() {
            return sessionId;
        }
    }

    //----- Sender Manager ---------------------------------------------------//

    private class ProtonSender {

        private final String senderId;
        private final Sender sender;

        public ProtonSender(String senderId, Sender sender) {
            this.senderId = senderId;
            this.sender = sender;
        }

        public String getId() {
            return senderId;
        }

        public Sender getSender() {
            return sender;
        }
    }

    //----- Receiver Manager ---------------------------------------------------//

    private class ProtonReceiver {

        private final String receiverId;
        private final Receiver receiver;

        public ProtonReceiver(String receiverId, Receiver receiver) {
            this.receiverId = receiverId;
            this.receiver = receiver;
        }

        public String getId() {
            return receiverId;
        }

        public Receiver getReceiver() {
            return receiver;
        }
    }

    //----- SASL Authentication Manager --------------------------------------//

    private class SaslAuthenticator {

        private final String[] mechanisms = new String[] { "PLAIN", "ANONYMOUS" };

        private final Sasl protonSasl;

        public SaslAuthenticator(ProtonConnection connection) {
            protonSasl = connection.protonTransport.sasl();
            protonSasl.setMechanisms(mechanisms);
            protonSasl.server();
        }

        public boolean isDone() {
            return protonSasl.getOutcome() != Sasl.SaslOutcome.PN_SASL_NONE;
        }

        public void processSaslExchange() {
            if (protonSasl.getRemoteMechanisms().length > 0) {

                String[] mechanisms = protonSasl.getRemoteMechanisms();
                if (mechanisms != null && mechanisms.length > 0) {
                    LOG.debug("SASL [{}} Handshake started.", mechanisms[0]);

                    if (mechanisms[0].equalsIgnoreCase("PLAIN")) {
                        byte[] data = new byte[protonSasl.pending()];
                        protonSasl.recv(data, 0, data.length);
                        protonSasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                    } else if (mechanisms[0].equalsIgnoreCase("ANONYMOUS")) {
                        protonSasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                    } else {
                        protonSasl.done(Sasl.SaslOutcome.PN_SASL_PERM);
                    }

                } else {
                    LOG.info("SASL: could not find supported mechanism");
                    protonSasl.done(Sasl.SaslOutcome.PN_SASL_PERM);
                }
            }
        }
    }

    //----- Simple AMQP Header Wrapper ---------------------------------------//

    private class AmqpHeader {

        private final byte[] PREFIX = new byte[] { 'A', 'M', 'Q', 'P' };

        private final byte[] buffer = new byte[HEADER_SIZE];

        public AmqpHeader() {
            this(Unpooled.wrappedBuffer(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 }));
        }

        public AmqpHeader(ByteBuf buffer) {
            this(buffer, true);
        }

        public AmqpHeader(ByteBuf buffer, boolean validate) {
            setBuffer(buffer, validate);
        }

        public int getProtocolId() {
            return buffer[4] & 0xFF;
        }

        public void setProtocolId(int value) {
            buffer[4] = (byte) value;
        }

        public int getMajor() {
            return buffer[5] & 0xFF;
        }

        public void setMajor(int value) {
            buffer[5] = (byte) value;
        }

        public int getMinor() {
            return buffer[6] & 0xFF;
        }

        public void setMinor(int value) {
            buffer[6] = (byte) value;
        }

        public int getRevision() {
            return buffer[7] & 0xFF;
        }

        public void setRevision(int value) {
            buffer[7] = (byte) value;
        }

        public ByteBuf getBuffer() {
            return Unpooled.wrappedBuffer(buffer);
        }

        public void setBuffer(ByteBuf value) {
            setBuffer(value, true);
        }

        public void setBuffer(ByteBuf value, boolean validate) {
            if (validate && !hasValidPrefix(value) || value.array().length != 8) {
                throw new IllegalArgumentException("Not an AMQP header buffer");
            }

            value.getBytes(0, buffer, 0, 8);
        }

        public boolean hasValidPrefix() {
            return hasValidPrefix(getBuffer());
        }

        private boolean hasValidPrefix(ByteBuf buffer) {
            return buffer.getByte(0) == PREFIX[0] &&
                   buffer.getByte(1) == PREFIX[1] &&
                   buffer.getByte(2) == PREFIX[2] &&
                   buffer.getByte(3) == PREFIX[3];
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < HEADER_SIZE; ++i) {
                char value = (char) buffer[i];
                if (Character.isLetter(value)) {
                    builder.append(value);
                } else {
                    builder.append(",");
                    builder.append((int) value);
                }
            }
            return builder.toString();
        }
    }
}
