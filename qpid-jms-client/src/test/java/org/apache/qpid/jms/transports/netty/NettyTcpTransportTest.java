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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of the Netty based TCP transport.
 */
public class NettyTcpTransportTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTcpTransportTest.class);

    private static final int SEND_BYTE_COUNT = 1024;

    protected boolean transportClosed;
    protected final List<Throwable> exceptions = new ArrayList<Throwable>();
    protected final List<ByteBuf> data = new ArrayList<ByteBuf>();
    protected final AtomicInteger bytesRead = new AtomicInteger();

    protected final TransportListener testListener = new NettyTransportListener();

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
    public void testCreateWithNullOptionsUsesDefaults() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        Transport transport = createTransport(serverLocation, testListener, null);
        assertEquals(TransportOptions.INSTANCE, transport.getTransportOptions());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectWithoutRunningServer() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            server.close();

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
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
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, null, createClientOptions());
            try {
                transport.connect();
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
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, null, createClientOptions());
            assertNull(transport.getTransportListener());
            transport.setTransportListener(testListener);
            assertNotNull(transport.getTransportListener());

            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServer() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
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

    @Test(timeout = 60 * 1000)
    public void testMultipleConnectionsToServer() throws Exception {
        final int CONNECTION_COUNT = 10;

        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createTransport(serverLocation, testListener, createClientOptions());
                try {
                    transport.connect();
                    assertTrue(transport.isConnected());
                    LOG.info("Connected to server:{} as expected.", serverLocation);
                    transports.add(transport);
                } catch (Exception e) {
                    fail("Should have connected to the server: " + serverLocation);
                }
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

        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createTransport(serverLocation, testListener, createClientOptions());
                try {
                    transport.connect();
                    transport.send(sendBuffer.copy());
                    transports.add(transport);
                } catch (Exception e) {
                    fail("Should have connected to the server: " + serverLocation);
                }
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), (FRAME_SIZE * CONNECTION_COUNT));
                    return bytesRead.get() == (FRAME_SIZE * CONNECTION_COUNT);
                }
            }));

            for (Transport transport : transports) {
                transport.close();
            }
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testDetectServerClose() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            server.close();
        }

        final Transport connectedTransport = transport;
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !connectedTransport.isConnected();
            }
        }));

        assertTrue(data.isEmpty());

        try {
            transport.close();
        } catch (Exception ex) {
            fail("Close of a disconnect transport should not generate errors");
        }
    }

    @Test(timeout = 60 * 1000)
    public void testZeroSizedSentNoErrors() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            transport.send(Unpooled.buffer(0));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testDataSentIsReceived() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            ByteBuf sendBuffer = transport.allocateSendBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer.writeByte('A');
            }

            transport.send(sendBuffer);

            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return !data.isEmpty();
                }
            }));

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

        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            ByteBuf sendBuffer = Unpooled.buffer(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                sendBuffer.writeByte('A');
            }

            for (int i = 0; i < iterations; ++i) {
                transport.send(sendBuffer.copy());
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return bytesRead.get() == (byteCount * iterations);
                }
            }));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testSendToClosedTransportFails() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect();
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            transport.close();

            ByteBuf sendBuffer = Unpooled.buffer(10);
            try {
                transport.send(sendBuffer);
                fail("Should throw on send of closed transport");
            } catch (IOException ex) {
            }
        }
    }

    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        if (listener == null) {
            return new NettyTcpTransport(serverLocation, options);
        } else {
            return new NettyTcpTransport(listener, serverLocation, options);
        }
    }

    protected TransportOptions createClientOptions() {
        return TransportOptions.INSTANCE.clone();
    }

    protected TransportOptions createServerOptions() {
        return TransportOptions.INSTANCE.clone();
    }

    protected void logTransportErrors() {
        if (!exceptions.isEmpty()) {
            for(Throwable ex : exceptions) {
                LOG.info("Transport sent exception: {}", ex, ex);
            }
        }
    }

    private class NettyTransportListener implements TransportListener {

        @Override
        public void onData(ByteBuf incoming) {
            LOG.debug("Client has new incoming data of size: {}", incoming.readableBytes());
            data.add(incoming);
            bytesRead.addAndGet(incoming.readableBytes());
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
