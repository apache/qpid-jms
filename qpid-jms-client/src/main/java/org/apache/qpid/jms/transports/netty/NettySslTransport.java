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

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.URI;
import java.security.Principal;

import org.apache.qpid.jms.transports.SSLTransport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the Netty based TCP transport to add SSL support.
 */
public class NettySslTransport extends NettyTcpTransport implements SSLTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NettySslTransport.class);

    /**
     * Create a new transport instance
     *
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public NettySslTransport(URI remoteLocation, TransportOptions options) {
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
    public NettySslTransport(TransportListener listener, URI remoteLocation, TransportOptions options) {
        super(listener, remoteLocation, options);
    }

    @Override
    protected void configureChannel(final Channel channel) throws Exception {
        SslHandler sslHandler = TransportSupport.createSslHandler(getRemoteLocation(), getSslOptions());
        sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
            @Override
            public void operationComplete(Future<Channel> future) throws Exception {
                if (future.isSuccess()) {
                    LOG.trace("SSL Handshake has completed: {}", channel);
                    connectionEstablished(channel);
                } else {
                    LOG.trace("SSL Handshake has failed: {}", channel);
                    connectionFailed(channel, IOExceptionSupport.create(future.cause()));
                }
            }
        });

        channel.pipeline().addLast(sslHandler);

        super.configureChannel(channel);
    }

    @Override
    protected void handleConnected(final Channel channel) throws Exception {
        // In this transport, the next step is taken by the handshake future
        // completion listener added when configuring the channel above
    }

    @Override
    public TransportOptions getTransportOptions() {
        if (options == null) {
            options = TransportSslOptions.INSTANCE;
        }

        return options;
    }

    @Override
    protected int getRemotePort() {
        return remote.getPort() != -1 ? remote.getPort() : getSslOptions().getDefaultSslPort();
    }

    private TransportSslOptions getSslOptions() {
        return (TransportSslOptions) getTransportOptions();
    }

    @Override
    public Principal getLocalPrincipal() {
        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);

        return sslHandler.engine().getSession().getLocalPrincipal();
    }
}
