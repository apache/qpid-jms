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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.qpid.jms.transports.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

public enum EventLoopType {
    EPOLL, KQUEUE, NIO;

    private static final Logger LOG = LoggerFactory.getLogger(EventLoopType.class);

    public void createChannel(final Bootstrap bootstrap) {
        createChannel(this, requireNonNull(bootstrap));
    }

    public EventLoopGroup createEventLoopGroup(final int threads, final ThreadFactory ioThreadFactory) {
        return createEventLoopGroup(this, threads, ioThreadFactory);
    }

    private static EventLoopGroup createEventLoopGroup(final EventLoopType type, final int threads, final ThreadFactory ioThreadFactory) {
        switch (type) {
            case EPOLL:
                LOG.trace("Netty Transport using Epoll mode");
                return EpollSupport.createGroup(threads, ioThreadFactory);
            case KQUEUE:
                LOG.trace("Netty Transport using KQueue mode");
                return KQueueSupport.createGroup(threads, ioThreadFactory);
            case NIO:
                LOG.trace("Netty Transport using Nio mode");
                return new NioEventLoopGroup(threads, ioThreadFactory);
            default:
                throw new IllegalArgumentException("Unknown event loop type:" + type);
        }
    }

    private static void createChannel(final EventLoopType type, final Bootstrap bootstrap) {
        switch (type) {
            case EPOLL:
                EpollSupport.createChannel(bootstrap);
                break;
            case KQUEUE:
                KQueueSupport.createChannel(bootstrap);
                break;
            case NIO:
                bootstrap.channel(NioSocketChannel.class);
                break;
            default:
                throw new IllegalArgumentException("Unknown event loop type:" + type);
        }
    }

    public static EventLoopType valueOf(final TransportOptions transportOptions) {
        final boolean useKQueue = KQueueSupport.isAvailable(transportOptions);
        final boolean useEpoll = EpollSupport.isAvailable(transportOptions);
        if (useKQueue) {
            return KQUEUE;
        }

        if (useEpoll) {
            return EPOLL;
        }

        return NIO;
    }
}
