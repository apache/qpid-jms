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

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * TCP based transport that uses Netty as the underlying IO layer.
 */
public class NettyTcpTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTcpTransport.class);

    public static final int SHUTDOWN_TIMEOUT = 50;
    public static final int DEFAULT_MAX_FRAME_SIZE = 65535;

    protected Bootstrap bootstrap;
    protected EventLoopGroup group;
    protected Channel channel;
    protected TransportListener listener;
    protected int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    private final TransportOptions options;
    private final URI remote;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private volatile IOException failureCause;

    /**
     * Create a new transport instance
     *
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     */
    public NettyTcpTransport(URI remoteLocation, TransportOptions options) {
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
    public NettyTcpTransport(TransportListener listener, URI remoteLocation, TransportOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Transport Options cannot be null");
        }

        if (remoteLocation == null) {
            throw new IllegalArgumentException("Transport remote location cannot be null");
        }

        this.options = options;
        this.listener = listener;
        this.remote = remoteLocation;
    }

    @Override
    public void connect(SSLContext sslContextOverride) throws IOException {

        if (listener == null) {
            throw new IllegalStateException("A transport listener must be set before connection attempts.");
        }

        final SslHandler sslHandler;
        if (isSecure()) {
            try {
                TransportSslOptions sslOptions = getSslOptions();
                sslOptions.setSslContextOverride(sslContextOverride);

                sslHandler = TransportSupport.createSslHandler(getRemoteLocation(), sslOptions);
            } catch (Exception ex) {
                // TODO: can we stop it throwing Exception?
                throw IOExceptionSupport.create(ex);
            }
        } else {
            sslHandler = null;
        }

        boolean useKQueue = getTransportOptions().isUseKQueue() && KQueue.isAvailable();
        boolean useEpoll = getTransportOptions().isUseEpoll() && Epoll.isAvailable();

        if (useKQueue) {
            LOG.trace("Netty Transport using KQueue mode");
            group = new KQueueEventLoopGroup(1);
        } else if (useEpoll) {
            LOG.trace("Netty Transport using Epoll mode");
            group = new EpollEventLoopGroup(1);
        } else {
            LOG.trace("Netty Transport using NIO mode");
            group = new NioEventLoopGroup(1);
        }

        bootstrap = new Bootstrap();
        bootstrap.group(group);
        if (useKQueue) {
            bootstrap.channel(KQueueSocketChannel.class);
        } else if (useEpoll) {
            bootstrap.channel(EpollSocketChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel connectedChannel) throws Exception {
                configureChannel(connectedChannel, sslHandler);
            }
        });

        configureNetty(bootstrap, getTransportOptions());

        ChannelFuture future = bootstrap.connect(getRemoteHost(), getRemotePort());
        future.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    handleException(future.channel(), IOExceptionSupport.create(future.cause()));
                }
            }
        });

        try {
            connectLatch.await();
        } catch (InterruptedException ex) {
            LOG.debug("Transport connection was interrupted.");
            Thread.interrupted();
            failureCause = IOExceptionSupport.create(ex);
        }

        if (failureCause != null) {
            // Close out any Netty resources now as they are no longer needed.
            if (channel != null) {
                channel.close().syncUninterruptibly();
                channel = null;
            }
            if (group != null) {
                Future<?> fut = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT)) {
                    LOG.trace("Channel group shutdown failed to complete in allotted time");
                }
                group = null;
            }

            throw failureCause;
        } else {
            // Connected, allow any held async error to fire now and close the transport.
            channel.eventLoop().execute(new Runnable() {

                @Override
                public void run() {
                    if (failureCause != null) {
                        channel.pipeline().fireExceptionCaught(failureCause);
                    }
                }
            });
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public boolean isSecure() {
        return options.isSSL();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            try {
                if (channel != null) {
                    channel.close().syncUninterruptibly();
                }
            } finally {
                if (group != null) {
                    Future<?> fut = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT)) {
                        LOG.trace("Channel group shutdown failed to complete in allotted time");
                    }
                }
            }
        }
    }

    @Override
    public ByteBuf allocateSendBuffer(int size) throws IOException {
        checkConnected();
        return channel.alloc().ioBuffer(size, size);
    }

    @Override
    public void send(ByteBuf output) throws IOException {
        checkConnected(output);

        int length = output.readableBytes();
        if (length == 0) {
            return;
        }

        LOG.trace("Attempted write of: {} bytes", length);

        channel.writeAndFlush(output);
    }

    @Override
    public TransportListener getTransportListener() {
        return listener;
    }

    @Override
    public void setTransportListener(TransportListener listener) {
        this.listener = listener;
    }

    @Override
    public TransportOptions getTransportOptions() {
        return options;
    }

    @Override
    public URI getRemoteLocation() {
        return remote;
    }

    @Override
    public Principal getLocalPrincipal() {
        Principal result = null;

        if (isSecure()) {
            SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            result = sslHandler.engine().getSession().getLocalPrincipal();
        }

        return result;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        if (connected.get()) {
            throw new IllegalStateException("Cannot change Max Frame Size while connected.");
        }

        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    //----- Internal implementation details, can be overridden as needed -----//

    protected String getRemoteHost() {
        return remote.getHost();
    }

    protected int getRemotePort() {
        if (remote.getPort() != -1) {
            return remote.getPort();
        } else {
            return isSecure() ? getSslOptions().getDefaultSslPort() : getTransportOptions().getDefaultTcpPort();
        }
    }

    protected void addAdditionalHandlers(ChannelPipeline pipeline) {

    }

    protected ChannelInboundHandlerAdapter createChannelHandler() {
        return new NettyTcpTransportHandler();
    }

    //----- Event Handlers which can be overridden in subclasses -------------//

    protected void handleConnected(Channel channel) throws Exception {
        LOG.trace("Channel has become active! Channel is {}", channel);
        connectionEstablished(channel);
    }

    protected void handleChannelInactive(Channel channel) throws Exception {
        LOG.trace("Channel has gone inactive! Channel is {}", channel);
        if (connected.compareAndSet(true, false) && !closed.get()) {
            LOG.trace("Firing onTransportClosed listener");
            listener.onTransportClosed();
        }
    }

    protected void handleException(Channel channel, Throwable cause) throws Exception {
        LOG.trace("Exception on channel! Channel is {}", channel);
        if (connected.compareAndSet(true, false) && !closed.get()) {
            LOG.trace("Firing onTransportError listener");
            if (failureCause != null) {
                listener.onTransportError(failureCause);
            } else {
                listener.onTransportError(cause);
            }
        } else {
            // Hold the first failure for later dispatch if connect succeeds.
            // This will then trigger disconnect using the first error reported.
            if (failureCause == null) {
                LOG.trace("Holding error until connect succeeds: {}", cause.getMessage());
                failureCause = IOExceptionSupport.create(cause);
            }

            connectionFailed(channel, failureCause);
        }
    }

    //----- State change handlers and checks ---------------------------------//

    protected final void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    private void checkConnected(ByteBuf output) throws IOException {
        if (!connected.get()) {
            ReferenceCountUtil.release(output);
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    /*
     * Called when the transport has successfully connected and is ready for use.
     */
    private void connectionEstablished(Channel connectedChannel) {
        channel = connectedChannel;
        connected.set(true);
        connectLatch.countDown();
    }

    /*
     * Called when the transport connection failed and an error should be returned.
     */
    private void connectionFailed(Channel failedChannel, IOException cause) {
        failureCause = cause;
        channel = failedChannel;
        connected.set(false);
        connectLatch.countDown();
    }

    private TransportSslOptions getSslOptions() {
        return (TransportSslOptions) getTransportOptions();
    }

    private void configureNetty(Bootstrap bootstrap, TransportOptions options) {
        bootstrap.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, options.isTcpKeepAlive());
        bootstrap.option(ChannelOption.SO_LINGER, options.getSoLinger());

        if (options.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, options.getSendBufferSize());
        }

        if (options.getReceiveBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_RCVBUF, options.getReceiveBufferSize());
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(options.getReceiveBufferSize()));
        }

        if (options.getTrafficClass() != -1) {
            bootstrap.option(ChannelOption.IP_TOS, options.getTrafficClass());
        }
    }

    private void configureChannel(final Channel channel, final SslHandler sslHandler) throws Exception {
        if (isSecure()) {
            channel.pipeline().addLast(sslHandler);
        }

        if (getTransportOptions().isTraceBytes()) {
            channel.pipeline().addLast("logger", new LoggingHandler(getClass()));
        }

        addAdditionalHandlers(channel.pipeline());

        channel.pipeline().addLast(createChannelHandler());
    }

    //----- Default implementation of Netty handler --------------------------//

    protected abstract class NettyDefaultHandler<E> extends SimpleChannelInboundHandler<E> {

        @Override
        public void channelRegistered(ChannelHandlerContext context) throws Exception {
            channel = context.channel();
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            // In the Secure case we need to let the handshake complete before we
            // trigger the connected event.
            if (!isSecure()) {
                handleConnected(context.channel());
            } else {
                SslHandler sslHandler = context.pipeline().get(SslHandler.class);
                sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
                        if (future.isSuccess()) {
                            LOG.trace("SSL Handshake has completed: {}", channel);
                            handleConnected(channel);
                        } else {
                            LOG.trace("SSL Handshake has failed: {}", channel);
                            handleException(channel, future.cause());
                        }
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            handleChannelInactive(context.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            handleException(context.channel(), cause);
        }
    }

    //----- Handle binary data over socket connections -----------------------//

    protected class NettyTcpTransportHandler extends NettyDefaultHandler<ByteBuf> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
            LOG.trace("New data read: {} bytes incoming: {}", buffer.readableBytes(), buffer);
            listener.onData(buffer);
        }
    }
}
