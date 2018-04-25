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

import org.apache.qpid.jms.transports.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Simple Netty Server used to echo all data.
 */
public class NettyEchoServer extends NettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(NettyEchoServer.class);

    public NettyEchoServer(TransportOptions options, boolean secure, boolean needClientAuth) {
        super(options, secure, needClientAuth);
    }

    public NettyEchoServer(TransportOptions options, boolean secure, boolean needClientAuth, boolean webSocketServer) {
        super(options, secure, needClientAuth, webSocketServer);
    }

    @Override
    protected ChannelHandler getServerHandler() {
        return new EchoServerHandler();
    }

    private class EchoServerHandler extends SimpleChannelInboundHandler<ByteBuf>  {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            LOG.trace("Channel read: {}", msg);
            ctx.write(msg.copy());
        }
    }
}
