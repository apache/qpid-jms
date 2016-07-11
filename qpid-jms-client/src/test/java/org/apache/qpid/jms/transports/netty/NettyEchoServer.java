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
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Simple Netty Server used to echo all data.
 */
public class NettyEchoServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NettyEchoServer.class);

    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
    static final String WEBSOCKET_PATH = "/";

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final TransportOptions options;
    private int serverPort;
    private final boolean needClientAuth;
    private final boolean webSocketServer;
    private volatile SslHandler sslHandler;

    private final AtomicBoolean started = new AtomicBoolean();

    public NettyEchoServer(TransportOptions options) {
        this(options, false);
    }

    public NettyEchoServer(TransportOptions options, boolean needClientAuth) {
        this(options, needClientAuth, false);
    }

    public NettyEchoServer(TransportOptions options, boolean needClientAuth, boolean webSocketServer) {
        this.options = options;
        this.needClientAuth = needClientAuth;
        this.webSocketServer = webSocketServer;
    }

    public void start() throws Exception {

        if (started.compareAndSet(false, true)) {

            // Configure the server.
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup);
            server.channel(NioServerSocketChannel.class);
            server.option(ChannelOption.SO_BACKLOG, 100);
            server.handler(new LoggingHandler(LogLevel.INFO));
            server.childHandler(new ChannelInitializer<Channel>() {

                @Override
                public void initChannel(Channel ch) throws Exception {
                    if (options instanceof TransportSslOptions) {
                        TransportSslOptions sslOptions = (TransportSslOptions) options;
                        SSLContext context = TransportSupport.createSslContext(sslOptions);
                        SSLEngine engine = TransportSupport.createSslEngine(context, sslOptions);
                        engine.setUseClientMode(false);
                        engine.setNeedClientAuth(needClientAuth);
                        sslHandler = new SslHandler(engine);
                        ch.pipeline().addLast(sslHandler);
                    }

                    if (webSocketServer) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(65536));
                        ch.pipeline().addLast(new WebSocketServerProtocolHandler(WEBSOCKET_PATH, "amqp", true));
                    }

                    ch.pipeline().addLast(new EchoServerHandler());
                }
            });

            // Start the server.
            serverChannel = server.bind(getServerPort()).sync().channel();
        }
    }

    public void stop() throws InterruptedException {
        if (started.compareAndSet(true, false)) {
            try {
                LOG.info("Syncing channel close");
                serverChannel.close().sync();
            } catch (InterruptedException e) {
            }

            // Shut down all event loops to terminate all threads.
            LOG.info("Shutting down boss group");
            bossGroup.shutdownGracefully(10, 100, TimeUnit.MILLISECONDS);
            LOG.info("Shutting down worker group");
            workerGroup.shutdownGracefully(10, 100, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() throws InterruptedException {
        stop();
    }

    public int getServerPort() {
        if (serverPort == 0) {
            ServerSocket ss = null;
            try {
                ss = ServerSocketFactory.getDefault().createServerSocket(0);
                serverPort = ss.getLocalPort();
            } catch (IOException e) { // revert back to default
                serverPort = PORT;
            } finally {
                try {
                    if (ss != null ) {
                        ss.close();
                    }
                } catch (IOException e) { // ignore
                }
            }
        }
        return serverPort;
    }

    private class EchoServerHandler extends SimpleChannelInboundHandler<Object>  {

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.info("Server -> New active channel: {}", ctx.channel());
            SslHandler handler = ctx.pipeline().get(SslHandler.class);
            if (handler != null) {
                handler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
                        LOG.info("Server -> SSL handshake completed. Succeeded: {}", future.isSuccess());
                        if (!future.isSuccess()) {
                            sslHandler.close();
                            ctx.close();
                        }
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("channel has gone inactive: {}", ctx.channel());
            ctx.close();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            LOG.trace("Channel read: {}", msg);
            if (webSocketServer && msg instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame frame = (BinaryWebSocketFrame) msg;
                ctx.write(frame.copy());
                return;
            } else if (msg instanceof ByteBuf) {
                ctx.write(((ByteBuf) msg).copy());
                return;
            }

            String message = "unsupported frame type: " + msg.getClass().getName();
            throw new UnsupportedOperationException(message);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.info("Exception caught on channel: {}", ctx.channel());
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close();
        }
    }

    SslHandler getSslHandler() {
        return sslHandler;
    }
}
