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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.proxy.TestProxy;
import org.apache.qpid.jms.test.proxy.TestProxy.ProxyType;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;

/**
 * Test the Netty based WebSocket Transport
 */
public class NettyWsTransportTest extends NettyTcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettyWsTransportTest.class);

    @Override
    protected NettyEchoServer createEchoServer(TransportOptions options, boolean needClientAuth) {
        return new NettyEchoServer(options, false, needClientAuth, true);
    }

    @Override
    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        if (listener == null) {
            return new NettyWsTransport(serverLocation, options, false);
        } else {
            return new NettyWsTransport(listener, serverLocation, options, false);
        }
    }

    @Test
    @Timeout(60)
    public void testConnectToServerUsingCorrectPath() throws Exception {
        final String WEBSOCKET_PATH = "/testpath";

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.setWebSocketPath(WEBSOCKET_PATH);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port + WEBSOCKET_PATH);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect(null, null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

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

    @Test
    @Timeout(60)
    public void testConnectToServerUsingIncorrectPath() throws Exception {
        final String WEBSOCKET_PATH = "/testpath";

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            // No configured path means it won't match the requested one.
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port + WEBSOCKET_PATH);

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

    @Test
    @Timeout(60)
    public void testConnectionsSendReceiveLargeDataWhenFrameSizeAllowsIt() throws Exception {
        final int FRAME_SIZE = 8192;

        ByteBuf sendBuffer = Unpooled.buffer(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte('A');
        }

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            // Server should pass the data through without issue with this size
            server.setMaxFrameSize(FRAME_SIZE);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                // The transport should allow for the size of data we sent.
                transport.setMaxFrameSize(FRAME_SIZE);
                transport.connect(null, null);
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), FRAME_SIZE);
                    return bytesRead.get() == FRAME_SIZE || !transport.isConnected();
                }
            }, 10000, 50));

            assertTrue(transport.isConnected(), "Connection failed while receiving.");

            transport.close();
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test
    @Timeout(20)
    public void testConnectionReceivesFragmentedData() throws Exception {
        final int FRAME_SIZE = 5317;

        ByteBuf sendBuffer = Unpooled.buffer(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte('A' + (i % 10));
        }

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.setMaxFrameSize(FRAME_SIZE);
            // Server should fragment the data as it goes through
            server.setFragmentWrites(true);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            TransportOptions createClientOptions = createClientOptions();
            createClientOptions.setTraceBytes(true);

            NettyTransportListener wsListener = new NettyTransportListener(true);

            Transport transport = createTransport(serverLocation, wsListener, createClientOptions);
            try {
                transport.setMaxFrameSize(FRAME_SIZE);
                transport.connect(null, null);
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), FRAME_SIZE);
                    return bytesRead.get() == FRAME_SIZE || !transport.isConnected();
                }
            }, 10000, 50));

            assertTrue(transport.isConnected(), "Connection failed while receiving.");

            transport.close();

            assertEquals(2, data.size(), "Expected 2 data packets due to seperate websocket frames");

            ByteBuf receivedBuffer = Unpooled.buffer(FRAME_SIZE);
            for(ByteBuf buf : data) {
               buf.readBytes(receivedBuffer, buf.readableBytes());
            }

            assertEquals(FRAME_SIZE, receivedBuffer.readableBytes(), "Unexpected data length");
            assertTrue(ByteBufUtil.equals(sendBuffer, 0, receivedBuffer, 0, FRAME_SIZE), "Unexpected data");
        } finally {
            for (ByteBuf buf : data) {
                buf.release();
            }
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test
    @Timeout(60)
    public void testConnectionsSendReceiveLargeDataFailsDueToMaxFrameSize() throws Exception {
        final int FRAME_SIZE = 1024;

        ByteBuf sendBuffer = Unpooled.buffer(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte('A');
        }

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            // Server should pass the data through, client should choke on the incoming size.
            server.setMaxFrameSize(FRAME_SIZE);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                // Transport can't receive anything bigger so it should fail the connection
                // when data arrives that is larger than this value.
                transport.setMaxFrameSize(FRAME_SIZE / 2);
                transport.connect(null, null);
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(() -> !transport.isConnected()), "Transport should have lost connection");
        }

        assertFalse(exceptions.isEmpty());
    }

    @Test
    @Timeout(60)
    public void testTransportDetectsConnectionDropWhenServerEnforcesMaxFrameSize() throws Exception {
        final int FRAME_SIZE = 1024;

        ByteBuf sendBuffer = Unpooled.buffer(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte('A');
        }

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            // Server won't accept the data as it's to large and will close the connection.
            server.setMaxFrameSize(FRAME_SIZE / 2);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            final Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                // Transport allows bigger frames in so that server is the one causing the failure.
                transport.setMaxFrameSize(FRAME_SIZE);
                transport.connect(null, null);
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        transport.writeAndFlush(sendBuffer.copy());
                    } catch (IOException e) {
                        LOG.info("Transport send caught error:", e);
                        return true;
                    }

                    return false;
                }
            }, 10000, 10), "Transport should have lost connection");

            transport.close();
        }
    }

    @Test
    @Timeout(30)
    public void testCreateWithHttpHeadersSpecified() throws Exception {
        URI BASE_URI = new URI("ws://localhost:5672?" +
                "transport.ws.httpHeader.first=FOO&" +
                "transport.ws.httpHeader.second=BAR");

        NettyWsTransportFactory factory = new NettyWsTransportFactory();

        Transport transport = factory.createTransport(BASE_URI);

        assertNotNull(transport);
        assertTrue(transport instanceof NettyWsTransport);
        assertFalse(transport.isConnected());

        TransportOptions options = transport.getTransportOptions();
        assertNotNull(options);

        assertTrue(options.getHttpHeaders().containsKey("first"));
        assertTrue(options.getHttpHeaders().containsKey("second"));

        assertEquals("FOO", options.getHttpHeaders().get("first"));
        assertEquals("BAR", options.getHttpHeaders().get("second"));
    }

    @Test
    @Timeout(60)
    public void testConfiguredHttpHeadersArriveAtServer() throws Exception {
        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions clientOptions = createClientOptions();
            clientOptions.getHttpHeaders().put("test-header1", "FOO");
            clientOptions.getHttpHeaders().put("test-header2", "BAR");

            Transport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect(null, null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());

            assertTrue(server.awaitHandshakeCompletion(2000), "HandshakeCompletion not set within given time");
            HandshakeComplete handshake = server.getHandshakeComplete();
            assertNotNull(handshake, "completion should not be null");
            HttpHeaders requestHeaders = handshake.requestHeaders();

            assertTrue(requestHeaders.contains("test-header1"));
            assertTrue(requestHeaders.contains("test-header2"));

            assertEquals("FOO", requestHeaders.get("test-header1"));
            assertEquals("BAR", requestHeaders.get("test-header2"));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    @Timeout(60)
    public void testConnectViaHttpProxy() throws Exception {
        try (TestProxy testProxy = new TestProxy(ProxyType.HTTP);
             NettyEchoServer server = createEchoServer(createServerOptions())) {
            testProxy.start();
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions clientOptions = createClientOptions();
            SocketAddress proxyAddress = new InetSocketAddress("localhost", testProxy.getPort());
            Supplier<ProxyHandler> proxyHandlerFactory = () -> {
                return new HttpProxyHandler(proxyAddress);
            };
            clientOptions.setProxyHandlerSupplier(proxyHandlerFactory);

            Transport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect(null, null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());

            transport.close();

            assertEquals(1, testProxy.getSuccessCount());
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return server.getChannelActiveCount() == 1;
                }
            }, 10_000, 10));
        }

        assertTrue(!transportClosed); // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }
}
