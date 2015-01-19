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
package org.apache.qpid.jms.test.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ServerSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Netty Server used to echo all data.
 */
public class NettyEchoServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NettyEchoServer.class);

    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
    static final int TIMEOUT = 5000;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private int serverPort;

    private final AtomicBoolean started = new AtomicBoolean();

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
            Future<?> bossFuture = bossGroup.shutdownGracefully(10, TIMEOUT, TimeUnit.MILLISECONDS);
            LOG.info("Awaiting boss group shutdown");
            boolean bossShutdown = bossFuture.await(TIMEOUT + 500);

            LOG.info("Shutting down worker group");
            Future<?> workerFuture = workerGroup.shutdownGracefully(10, TIMEOUT, TimeUnit.MILLISECONDS);
            LOG.info("Awaiting worker group shutdown");
            boolean workerShutdown = workerFuture.await(TIMEOUT + 500);

            if (!bossShutdown) {
                throw new RuntimeException("Failed to shut down bossGroup in allotted time");
            }
            if (!workerShutdown) {
                throw new RuntimeException("Failed to shut down workerGroup in allotted time");
            }
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

    private class EchoServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ctx.write(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close();
        }
    }
}
