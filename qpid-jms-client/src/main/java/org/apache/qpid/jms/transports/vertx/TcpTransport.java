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
package org.apache.qpid.jms.transports.vertx;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertxFactory;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

/**
 * Vertex based TCP transport for raw data packets.
 */
public class TcpTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransport.class);

    private final Vertx vertx;
    private final NetClient client;
    private final URI remoteLocation;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> connectionError = new AtomicReference<Throwable>();

    private NetSocket socket;
    private TransportListener listener;
    private TransportOptions options;

    /**
     * Create a new transport instance
     *
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public TcpTransport(URI remoteLocation, TransportOptions options) {
        this(null, remoteLocation, options);
    }

    /**
     * Create a new transport instance
     *
     * @param listener
     *        the TransportListener that will receive events from this Transport.
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public TcpTransport(TransportListener listener, URI remoteLocation, TransportOptions options) {
        this.options = options;
        this.listener = listener;
        this.remoteLocation = remoteLocation;

        DefaultVertxFactory vertxFactory = new DefaultVertxFactory();
        this.vertx = vertxFactory.createVertx();
        this.client = vertx.createNetClient();
    }

    @Override
    public void connect() throws IOException {
        final CountDownLatch connectLatch = new CountDownLatch(1);

        if (listener == null) {
            throw new IllegalStateException("A transport listener must be set before connection attempts.");
        }

        configureNetClient(client, getTransportOptions());

        try {
            client.connect(remoteLocation.getPort(), remoteLocation.getHost(), new AsyncResultHandler<NetSocket>() {
                @Override
                public void handle(AsyncResult<NetSocket> asyncResult) {
                    if (asyncResult.succeeded()) {
                        socket = asyncResult.result();
                        LOG.info("We have connected! Socket is {}", socket);

                        connected.set(true);
                        connectLatch.countDown();

                        socket.dataHandler(new Handler<Buffer>() {
                            @Override
                            public void handle(Buffer event) {
                                listener.onData(event.getByteBuf());
                            }
                        });

                        socket.closeHandler(new Handler<Void>() {
                            @Override
                            public void handle(Void event) {
                                if (!closed.get()) {
                                    connected.set(false);
                                    listener.onTransportClosed();
                                }
                            }
                        });

                        socket.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                if (!closed.get()) {
                                    connected.set(false);
                                    listener.onTransportError(event);
                                }
                            }
                        });

                    } else {
                        connected.set(false);
                        connectionError.set(asyncResult.cause());
                        connectLatch.countDown();
                    }
                }
            });
        } catch (Throwable reason) {
            LOG.info("Failed to connect to target Broker: {}", reason);
            throw IOExceptionSupport.create(reason);
        }

        try {
            connectLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (connectionError.get() != null) {
            throw IOExceptionSupport.create(connectionError.get());
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (connected.get()) {
                socket.close();
                connected.set(false);
            }

            vertx.stop();
        }
    }

    @Override
    public void send(ByteBuffer output) throws IOException {
        checkConnected();
        int length = output.remaining();
        if (length == 0) {
            return;
        }

        byte[] copy = new byte[length];
        output.get(copy);
        Buffer sendBuffer = new Buffer(copy);

        vertx.eventBus().send(socket.writeHandlerID(), sendBuffer);
    }

    @Override
    public void send(ByteBuf output) throws IOException {
        checkConnected();
        int length = output.readableBytes();
        if (length == 0) {
            return;
        }

        Buffer sendBuffer = new Buffer(output.copy());
        vertx.eventBus().send(socket.writeHandlerID(), sendBuffer);
    }

    /**
     * Allows a subclass to configure the NetClient beyond what this transport might do.
     *
     * @throws IOException if an error occurs.
     */
    protected void configureNetClient(NetClient client, TransportOptions options) throws IOException {
        client.setSendBufferSize(options.getSendBufferSize());
        client.setReceiveBufferSize(options.getReceiveBufferSize());
        client.setSoLinger(options.getSoLinger());
        client.setTCPKeepAlive(options.isTcpKeepAlive());
        client.setTCPNoDelay(options.isTcpNoDelay());
        if (options.getConnectTimeout() >= 0) {
            client.setConnectTimeout(options.getConnectTimeout());
        }
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    @Override
    public TransportListener getTransportListener() {
        return this.listener;
    }

    @Override
    public void setTransportListener(TransportListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be set to null");
        }

        this.listener = listener;
    }

    /**
     * @return the options used to configure the TCP socket.
     */
    public TransportOptions getTransportOptions() {
        if (options == null) {
            options = TransportOptions.DEFAULT_OPTIONS;
        }

        return options;
    }
}
