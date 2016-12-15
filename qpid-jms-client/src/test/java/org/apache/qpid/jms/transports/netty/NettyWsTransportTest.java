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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Netty based WebSocket Transport
 */
public class NettyWsTransportTest extends NettyTcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettyWsTransportTest.class);

    @Override
    protected NettyEchoServer createEchoServer(TransportOptions options, boolean needClientAuth) {
        return new NettyEchoServer(options, needClientAuth, true);
    }

    @Override
    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        if (listener == null) {
            return new NettyWsTransport(serverLocation, options);
        } else {
            return new NettyWsTransport(listener, serverLocation, options);
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerUsingCorrectPath() throws Exception {
        final String WEBSOCKET_PATH = "/testpath";

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.setWebSocketPath(WEBSOCKET_PATH);
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port + WEBSOCKET_PATH);

            Transport transport = createTransport(serverLocation, testListener, createClientOptions());
            try {
                transport.connect(null);
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

    @Test(timeout = 60 * 1000)
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
                transport.connect(null);
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
}
