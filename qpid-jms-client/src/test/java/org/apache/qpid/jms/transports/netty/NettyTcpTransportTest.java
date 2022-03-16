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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.netty.channel.EventLoopGroup;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.proxy.TestProxy;
import org.apache.qpid.jms.test.proxy.TestProxy.ProxyType;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.apache.qpid.jms.util.Repeat;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;

/**
 * Test basic functionality of the Netty based TCP transport.
 */
@RunWith(QpidJMSTestRunner.class)
public class NettyTcpTransportTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTcpTransportTest.class);

    private static final int SEND_BYTE_COUNT = 1024;

    protected boolean transportClosed;
    protected final List<Throwable> exceptions = new ArrayList<Throwable>();
    protected final List<ByteBuf> data = new ArrayList<ByteBuf>();
    protected final AtomicInteger bytesRead = new AtomicInteger();

    protected final TransportListener testListener = new NettyTransportListener(false);

    @Test(timeout = 60 * 1000)
    public void testCloseOnNeverConnectedTransport() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        Transport transport = createTransport(serverLocation, testListener, createClientOptions());
        assertFalse(transport.isConnected());

        transport.close();

        assertTrue(!transportClosed);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testCreateWithNullOptionsThrowsIAE() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        try {
            createTransport(serverLocation, testListener, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test(timeout = 60000)
    public void testConnectWithCustomThreadFactoryConfigured() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);
            QpidJMSThreadFactory factory = new QpidJMSThreadFactory("NettyTransportTest", true);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            transport.setThreadFactory(factory);

            try {
                transport.connect(null, null);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {} as expected.", serverLocation);
                fail("Should have failed to connect to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());
            assertSame(factory, transport.getThreadFactory());

            try {
                transport.setThreadFactory(factory);
            } catch (IllegalStateException expected) {
                LOG.trace("Caught expected state exception");
            }

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectWithoutRunningServer() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            server.close();

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect(null, null);
                fail("Should have failed to connect to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {} as expected.", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectWithoutListenerFails() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, null, createClientOptions());
            try {
                transport.connect(null, null);
                fail("Should have failed to connect to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {} as expected.", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectAfterListenerSetWorks() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, null, createClientOptions());
            assertNull(transport.getTransportListener());
            transport.setTransportListener(testListener);
            assertNotNull(transport.getTransportListener());

            try {
                transport.connect(null, null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServer() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testMultipleConnectionsToServer() throws Exception {
        final int CONNECTION_COUNT = 10;

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createConnectedTransport(serverLocation, createClientOptions());
                assertTrue(transport.isConnected());

                transports.add(transport);
            }

            for (Transport transport : transports) {
                transport.close();
            }
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testMultipleConnectionsSendReceive() throws Exception {
        final int CONNECTION_COUNT = 10;
        final int FRAME_SIZE = 8;

        ByteBuf sendBuffer = Unpooled.buffer(FRAME_SIZE);
        for (int i = 0; i < 8; ++i) {
            sendBuffer.writeByte('A');
        }

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createTransport(serverLocation, testListener, createClientOptions());
                try {
                    transport.connect(null, null);
                    transport.writeAndFlush(sendBuffer.copy());
                    transports.add(transport);
                } catch (Exception e) {
                    fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
                }
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), (FRAME_SIZE * CONNECTION_COUNT));
                    return bytesRead.get() == (FRAME_SIZE * CONNECTION_COUNT);
                }
            }, 10000, 50));

            for (Transport transport : transports) {
                transport.close();
            }
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testDetectServerClose() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());

            server.close();
        }

        final Transport connectedTransport = transport;
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return !connectedTransport.isConnected();
            }
        }, 10000, 50));

        assertTrue(data.isEmpty());

        try {
            transport.close();
        } catch (Exception ex) {
            fail("Close of a disconnect transport should not generate errors");
        }
    }

    @Test(timeout = 60 * 1000)
    public void testZeroSizedSentNoErrors() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());

            transport.writeAndFlush(Unpooled.buffer(0));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testCannotDereferenceSharedClosedEventLoopGroup() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);
            final TransportOptions sharedTransportOptions = createClientOptions();
            sharedTransportOptions.setUseKQueue(false);
            sharedTransportOptions.setUseEpoll(false);
            sharedTransportOptions.setSharedEventLoopThreads(1);

            EventLoopGroupRef groupRef = null;
            Transport nioSharedTransport = createConnectedTransport(serverLocation, sharedTransportOptions);
            try {
                groupRef = getGroupRef(nioSharedTransport);
                assertNotNull(groupRef.group());
            } finally {
                nioSharedTransport.close();
            }

            try {
                groupRef.group();
                fail("Should have thrown ISE due to being closed");
            } catch (IllegalStateException expected) {
                // Ignore
            } catch (Throwable unexpected) {
                fail("Should have thrown IllegalStateException");
            }
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testSharedEventLoopGroups() throws Exception {
        final Set<Transport> transports = new HashSet<>();
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);
            final TransportOptions sharedTransportOptions = createClientOptions();
            sharedTransportOptions.setUseKQueue(false);
            sharedTransportOptions.setUseEpoll(false);
            sharedTransportOptions.setSharedEventLoopThreads(1);

            Transport sharedNioTransport1 = createConnectedTransport(serverLocation, sharedTransportOptions);
            transports.add(sharedNioTransport1);
            Transport sharedNioTransport2 = createConnectedTransport(serverLocation, sharedTransportOptions);
            transports.add(sharedNioTransport2);

            final EventLoopGroup sharedGroup = getGroupRef(sharedNioTransport1).group();
            assertSame(sharedGroup, getGroupRef(sharedNioTransport2).group());

            sharedNioTransport1.close();
            assertFalse(sharedGroup.isShutdown());
            assertFalse(sharedGroup.isTerminated());

            sharedNioTransport2.close();
            assertTrue(sharedGroup.isShutdown());
            assertTrue(sharedGroup.isTerminated());
        } finally {
            // Ensures that any not already closed, e.g due to test failure, are now closed.
            cleanUpTransports(transports);
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testSharedEventLoopGroupsOfDifferentSizes() throws Exception {
        final Set<Transport> transports = new HashSet<>();
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            final TransportOptions sharedTransportOptions1 = createClientOptions();
            sharedTransportOptions1.setUseKQueue(false);
            sharedTransportOptions1.setUseEpoll(false);
            sharedTransportOptions1.setSharedEventLoopThreads(1);
            Transport nioSharedTransport1 = createConnectedTransport(serverLocation, sharedTransportOptions1);
            transports.add(nioSharedTransport1);

            final TransportOptions sharedTransportOptions2 = createClientOptions();
            sharedTransportOptions2.setUseKQueue(false);
            sharedTransportOptions2.setUseEpoll(false);
            sharedTransportOptions2.setSharedEventLoopThreads(2);
            Transport nioSharedTransport2 = createConnectedTransport(serverLocation, sharedTransportOptions2);
            transports.add(nioSharedTransport2);

            EventLoopGroup sharedGroup1 = getGroupRef(nioSharedTransport1).group();
            EventLoopGroup sharedGroup2 = getGroupRef(nioSharedTransport2).group();
            assertNotSame(sharedGroup1, sharedGroup2);

            nioSharedTransport1.close();
            assertTrue(sharedGroup1.isShutdown());
            assertTrue(sharedGroup1.isTerminated());

            nioSharedTransport2.close();
            assertTrue(sharedGroup2.isShutdown());
            assertTrue(sharedGroup2.isTerminated());
        } finally {
            // Ensures that any not already closed, e.g due to test failure, are now closed.
            cleanUpTransports(transports);
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testUnsharedEventLoopGroups() throws Exception {
        final Set<Transport> transports = new HashSet<>();
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);
            final TransportOptions unsharedTransportOptions = createClientOptions();
            unsharedTransportOptions.setUseKQueue(false);
            unsharedTransportOptions.setUseEpoll(false);
            unsharedTransportOptions.setSharedEventLoopThreads(0);

            Transport unsharedNioTransport1 = createConnectedTransport(serverLocation, unsharedTransportOptions);
            transports.add(unsharedNioTransport1);
            Transport unsharedNioTransport2 = createConnectedTransport(serverLocation, unsharedTransportOptions);
            transports.add(unsharedNioTransport2);

            final EventLoopGroup unsharedGroup1 = getGroupRef(unsharedNioTransport1).group();
            final EventLoopGroup unsharedGroup2 = getGroupRef(unsharedNioTransport2).group();
            assertNotSame(unsharedGroup1, unsharedNioTransport2);

            unsharedNioTransport1.close();
            assertTrue(unsharedGroup1.isShutdown());
            assertTrue(unsharedGroup1.isTerminated());

            unsharedNioTransport2.close();
            assertTrue(unsharedGroup2.isShutdown());
            assertTrue(unsharedGroup2.isTerminated());
        } finally {
            // Ensures that any not already closed, e.g due to test failure, are now closed.
            cleanUpTransports(transports);
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testDataSentIsReceived() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());

            ByteBuf sendBuffer = transport.allocateSendBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer.writeByte('A');
            }

            transport.writeAndFlush(sendBuffer);

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !data.isEmpty();
                }
            }, 10000, 50));

            assertEquals(SEND_BYTE_COUNT, data.get(0).readableBytes());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testMultipleDataPacketsSentAreReceived() throws Exception {
        doMultipleDataPacketsSentAndReceive(SEND_BYTE_COUNT, 1);
    }

    @Test(timeout = 60 * 1000)
    public void testMultipleDataPacketsSentAreReceivedRepeatedly() throws Exception {
        doMultipleDataPacketsSentAndReceive(SEND_BYTE_COUNT, 10);
    }

    public void doMultipleDataPacketsSentAndReceive(final int byteCount, final int iterations) throws Exception {

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());

            ByteBuf sendBuffer = Unpooled.buffer(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                sendBuffer.writeByte('A');
            }

            for (int i = 0; i < iterations; ++i) {
                transport.writeAndFlush(sendBuffer.copy());
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return bytesRead.get() == (byteCount * iterations);
                }
            }, 10000, 50));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testSendToClosedTransportFails() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createConnectedTransport(serverLocation, createClientOptions());

            assertTrue(transport.isConnected());

            transport.close();

            ByteBuf sendBuffer = Unpooled.buffer(10);
            try {
                transport.writeAndFlush(sendBuffer);
                fail("Should throw on send of closed transport");
            } catch (IOException ex) {
            }
        }
    }

    @Test(timeout = 60000)
    public void testConnectRunsInitializationMethod() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);
            final AtomicBoolean initialized = new AtomicBoolean();

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect(() -> initialized.set(true), null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());
            assertTrue(initialized.get());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testFailureInInitializationRoutineFailsConnect() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect(() -> { throw new RuntimeException(); }, null);
                fail("Should not have connected to the server at " + serverLocation);
            } catch (Exception e) {
                LOG.info("Failed to connect to server:{} as expected", serverLocation);
            }

            assertFalse("Should not be connected", transport.isConnected());
            assertEquals("Server location is incorrect", serverLocation, transport.getRemoteLocation());

            transport.close();
        }

        assertFalse(transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Ignore("Used for checking for transport level leaks, my be unstable on CI.")
    @Test(timeout = 60 * 1000)
    public void testSendToClosedTransportFailsButDoesNotLeak() throws Exception {
        Transport transport = null;

        ResourceLeakDetector.setLevel(Level.PARANOID);

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            for (int i = 0; i < 256; ++i) {
                transport = createConnectedTransport(serverLocation, createClientOptions());

                assertTrue(transport.isConnected());

                ByteBuf sendBuffer = transport.allocateSendBuffer(10 * 1024 * 1024);
                sendBuffer.writeBytes(new byte[] {0, 1, 2, 3, 4});

                transport.close();

                try {
                    transport.writeAndFlush(sendBuffer);
                    fail("Should throw on send of closed transport");
                } catch (IOException ex) {
                }
            }

            System.gc();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithEpollEnabled() throws Exception {
        doTestEpollSupport(true);
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithEpollDisabled() throws Exception {
        doTestEpollSupport(false);
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerViaProxy() throws Exception {
        try (TestProxy testProxy = new TestProxy(ProxyType.SOCKS5);
             NettyEchoServer server = createEchoServer(createServerOptions())) {

            testProxy.start();
            server.start();

            int port = server.getServerPort();
            LOG.info("Echo server bound at: {}", port);
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions clientOptions = createClientOptions();
            SocketAddress proxyAddress = new InetSocketAddress("localhost", testProxy.getPort());
            Supplier<ProxyHandler> proxyHandlerFactory = () -> {
                return new Socks5ProxyHandler(proxyAddress);
            };
            clientOptions.setProxyHandlerSupplier(proxyHandlerFactory);

            Transport transport = createConnectedTransport(serverLocation, clientOptions);

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return server.getChannelActiveCount() == 1;
                }
            }, 10_000, 10));

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return testProxy.getSuccessCount() == 1;
                }
            }, 10_000, 10));

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();

        }

        assertTrue(!transportClosed); // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void doTestEpollSupport(boolean useEpoll) throws Exception {
        assumeTrue(Epoll.isAvailable());

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions options = createClientOptions();
            options.setUseEpoll(useEpoll);
            options.setUseKQueue(false);
            Transport transport = createConnectedTransport(serverLocation, options);

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());

            if(useEpoll) {
                assertEventLoopGroupType("Transport should be using Epoll", transport, EpollEventLoopGroup.class);
            } else {
                assertEventLoopGroupType("Transport should be using Nio", transport, NioEventLoopGroup.class);
            }

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private static EventLoopGroupRef getGroupRef(final Transport transport) throws IllegalAccessException {
        Field groupRefField = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && groupRefField == null) {
            try {
                groupRefField = transportType.getDeclaredField("groupRef");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull("Transport implementation unknown", groupRefField);

        groupRefField.setAccessible(true);
        return (EventLoopGroupRef) groupRefField.get(transport);
    }

    private static void assertEventLoopGroupType(String message, Transport transport, Class<? extends EventLoopGroup> eventLoopGroupClass) throws Exception {
        final EventLoopGroupRef groupRef = getGroupRef(transport);

        assertThat(message, groupRef.group(), instanceOf(eventLoopGroupClass));
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithKQueueEnabled() throws Exception {
        doTestKQueueSupport(true);
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithKQueueDisabled() throws Exception {
        doTestKQueueSupport(false);
    }

    private void doTestKQueueSupport(boolean useKQueue) throws Exception {
        assumeTrue(KQueue.isAvailable());

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions options = createClientOptions();
            options.setUseKQueue(useKQueue);
            options.setUseEpoll(false);
            Transport transport = createConnectedTransport(serverLocation, options);

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());
            if(useKQueue) {
                assertEventLoopGroupType("Transport should be using Kqueue", transport, KQueueEventLoopGroup.class);
            } else {
                assertEventLoopGroupType("Transport should be using Nio", transport, NioEventLoopGroup.class);
            }

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        if (listener == null) {
            return new NettyTcpTransport(serverLocation, options, false);
        } else {
            return new NettyTcpTransport(listener, serverLocation, options, false);
        }
    }

    private Transport createConnectedTransport(final URI serverLocation, final TransportOptions options) {
        Transport transport = createTransport(serverLocation, testListener, options);
        try {
            transport.connect(null, null);
            LOG.info("Connected to server:{} as expected.", serverLocation);
        } catch (Exception e) {
            fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
        }
        return transport;
    }

    private void cleanUpTransports(final Set<Transport> transports) {
        transports.forEach(transport -> {
            try {
                transport.close();
            } catch (Throwable t) {
                LOG.warn(t.getMessage());
            }
        });
    }

    protected TransportOptions createClientOptions() {
        return new TransportOptions();
    }

    protected TransportOptions createServerOptions() {
        return new TransportOptions();
    }

    protected void logTransportErrors() {
        if (!exceptions.isEmpty()) {
            for(Throwable ex : exceptions) {
                LOG.info("Transport sent exception: {}", ex, ex);
            }
        }
    }

    protected NettyEchoServer createEchoServer(TransportOptions options) {
        return createEchoServer(options, false);
    }

    protected NettyEchoServer createEchoServer(TransportOptions options, boolean needClientAuth) {
        return new NettyEchoServer(options, false, needClientAuth, false);
    }

    public class NettyTransportListener implements TransportListener {
        final boolean retainDataBufs;

        NettyTransportListener(boolean retainDataBufs) {
            this.retainDataBufs = retainDataBufs;
        }

        @Override
        public void onData(ByteBuf incoming) {
            LOG.debug("Client has new incoming data of size: {}", incoming.readableBytes());
            data.add(incoming);
            bytesRead.addAndGet(incoming.readableBytes());

            if(retainDataBufs) {
                incoming.retain();
            }
        }

        @Override
        public void onTransportClosed() {
            LOG.debug("Transport reports that it has closed.");
            transportClosed = true;
        }

        @Override
        public void onTransportError(Throwable cause) {
            LOG.info("Transport error caught: {}", cause.getMessage(), cause);
            exceptions.add(cause);
        }
    }
}