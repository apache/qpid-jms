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

import java.io.IOException;
import java.net.URI;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportOptions;
import org.junit.Test;

/**
 * Test the NettyTcpTransportFactory class
 */
public class NettyTcpTransportFactoryTest {

    public static final int CUSTOM_SEND_BUFFER_SIZE = 32 * 1024;
    public static final int CUSTOM_RECEIVE_BUFFER_SIZE = CUSTOM_SEND_BUFFER_SIZE;
    public static final int CUSTOM_TRAFFIC_CLASS = 1;
    public static final boolean CUSTOM_TCP_NO_DELAY = false;
    public static final boolean CUSTOM_TCP_KEEP_ALIVE = true;
    public static final int CUSTOM_SO_LINGER = Short.MIN_VALUE;
    public static final int CUSTOM_SO_TIMEOUT = 10;
    public static final int CUSTOM_CONNECT_TIMEOUT = 90000;
    private static final String CUSTOM_LOCAL_ADDRESS = "localhost";
    private static final int CUSTOM_LOCAL_PORT = 30000;

    @Test(timeout = 30000)
    public void testCreateWithDefaultOptions() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672");

        NettyTcpTransportFactory factory = new NettyTcpTransportFactory();

        Transport transport = factory.createTransport(BASE_URI);

        assertNotNull(transport);
        assertTrue(transport instanceof NettyTcpTransport);
        assertFalse(transport.isConnected());

        TransportOptions options = transport.getTransportOptions();
        assertNotNull(options);

        assertEquals(TransportOptions.DEFAULT_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TransportOptions.DEFAULT_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TransportOptions.DEFAULT_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TransportOptions.DEFAULT_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TransportOptions.DEFAULT_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TransportOptions.DEFAULT_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TransportOptions.DEFAULT_SO_LINGER, options.getSoLinger());
        assertEquals(TransportOptions.DEFAULT_SO_TIMEOUT, options.getSoTimeout());
        assertNull(options.getLocalAddress());
        assertEquals(TransportOptions.DEFAULT_LOCAL_PORT, options.getLocalPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTransportWithUnknownOption() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672?transport.someOption=true");
        NettyTcpTransportFactory factory = new NettyTcpTransportFactory();
        factory.createTransport(BASE_URI);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTransportWithBadOption() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672?transport.trafficClass=4096");
        NettyTcpTransportFactory factory = new NettyTcpTransportFactory();
        factory.createTransport(BASE_URI);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTransportWithHttpHeaders() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672?transport.httpHeaders=A");
        NettyTcpTransportFactory factory = new NettyTcpTransportFactory();
        factory.createTransport(BASE_URI);
    }

    @Test(expected = IOException.class)
    public void testCreateWithBadKey() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672?transport.trafficClass=4096");
        NettyTcpTransportFactory.create("foo", BASE_URI);
    }

    @Test(expected = IOException.class)
    public void testFindFactoryFailsWithNullKey() throws Exception {
        NettyTcpTransportFactory.findTransportFactory(null);
    }

    @Test(expected = IOException.class)
    public void testFindFactoryFailsWithInvalidKey() throws Exception {
        NettyTcpTransportFactory.findTransportFactory("ssh");
    }

    @Test(timeout = 30000)
    public void testCreateWithCustomOptions() throws Exception {
        URI BASE_URI = new URI("tcp://localhost:5672");

        URI configuredURI = new URI(BASE_URI.toString() + "?" +
            "transport.connectTimeout=" + CUSTOM_CONNECT_TIMEOUT + "&" +
            "transport.sendBufferSize=" + CUSTOM_SEND_BUFFER_SIZE + "&" +
            "transport.receiveBufferSize=" + CUSTOM_RECEIVE_BUFFER_SIZE + "&" +
            "transport.trafficClass=" + CUSTOM_TRAFFIC_CLASS + "&" +
            "transport.tcpNoDelay=" + CUSTOM_TCP_NO_DELAY + "&" +
            "transport.tcpKeepAlive=" + CUSTOM_TCP_KEEP_ALIVE + "&" +
            "transport.soLinger=" + CUSTOM_SO_LINGER + "&" +
            "transport.soTimeout=" + CUSTOM_SO_TIMEOUT + "&" +
            "transport.localAddress=" + CUSTOM_LOCAL_ADDRESS + "&" +
            "transport.localPort=" + CUSTOM_LOCAL_PORT);

        NettyTcpTransportFactory factory = new NettyTcpTransportFactory();

        Transport transport = factory.createTransport(configuredURI);

        assertNotNull(transport);
        assertTrue(transport instanceof NettyTcpTransport);
        assertFalse(transport.isConnected());

        TransportOptions options = transport.getTransportOptions();
        assertNotNull(options);

        assertEquals(CUSTOM_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(CUSTOM_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(CUSTOM_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(CUSTOM_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(CUSTOM_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(CUSTOM_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(CUSTOM_SO_LINGER, options.getSoLinger());
        assertEquals(CUSTOM_SO_TIMEOUT, options.getSoTimeout());
    }
}
