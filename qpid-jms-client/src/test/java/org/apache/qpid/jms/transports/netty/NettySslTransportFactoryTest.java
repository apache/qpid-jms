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

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.junit.Test;

/**
 * Test the NettySslTransportFactoryTest class
 */
public class NettySslTransportFactoryTest {

    public static final int CUSTOM_SEND_BUFFER_SIZE = 32 * 1024;
    public static final int CUSTOM_RECEIVE_BUFFER_SIZE = CUSTOM_SEND_BUFFER_SIZE;
    public static final int CUSTOM_TRAFFIC_CLASS = 1;
    public static final boolean CUSTOM_TCP_NO_DELAY = false;
    public static final boolean CUSTOM_TCP_KEEP_ALIVE = true;
    public static final int CUSTOM_SO_LINGER = Short.MIN_VALUE;
    public static final int CUSTOM_SO_TIMEOUT = 10;
    public static final int CUSTOM_CONNECT_TIMEOUT = 90000;
    public static final String CUSTOM_CONTEXT_PROTOCOL = "TLSv1.2";
    public static final String[] CUSTOM_ENABLED_PROTOCOLS = { "TLSv1.1", "TLSv1.2" };
    public static final String CUSTOM_ENABLED_PROTOCOLS_STRING = "TLSv1.1,TLSv1.2";
    public static final String[] CUSTOM_DISABLED_PROTOCOLS = { "SSLv3", "TLSv1.1" };
    public static final String CUSTOM_DISABLED_PROTOCOLS_STRING = "SSLv3,TLSv1.1";
    public static final String[] CUSTOM_ENABLED_CIPHER_SUITES = {"Suite-1", "Suite-2"};
    public static final String CUSTOM_ENABLED_CIPHER_SUITES_STRING = "Suite-1,Suite-2";
    public static final String[] CUSTOM_DISABLED_CIPHER_SUITES = {"Suite-3", "Suite-4"};
    public static final String CUSTOM_DISABLED_CIPHER_SUITES_STRING = "Suite-3,Suite-4";
    public static final String CUSTOM_STORE_TYPE = "jceks";
    public static final boolean CUSTOM_TRUST_ALL = true;
    public static final boolean CUSTOM_VERIFY_HOST = false;
    public static final String CUSTOM_KEY_ALIAS = "myTestAlias";

    @Test
    public void testCreateWithDefaultOptions() throws Exception {
        URI BASE_URI = new URI("ssl://localhost:5672");

        NettySslTransportFactory factory = new NettySslTransportFactory();

        Transport transport = factory.createTransport(BASE_URI);

        assertNotNull(transport);
        assertTrue(transport instanceof NettyTcpTransport);
        assertFalse(transport.isConnected());
        assertTrue(transport.isSecure());

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

        assertTrue(options instanceof TransportSslOptions);
        TransportSslOptions sslOptions = (TransportSslOptions) options;

        assertEquals(TransportSslOptions.DEFAULT_CONTEXT_PROTOCOL, sslOptions.getContextProtocol());
        assertNull(sslOptions.getEnabledProtocols());
        assertArrayEquals(TransportSslOptions.DEFAULT_DISABLED_PROTOCOLS.toArray(new String[0]), sslOptions.getDisabledProtocols());
        assertNull(sslOptions.getEnabledCipherSuites());

        assertEquals(TransportSslOptions.DEFAULT_STORE_TYPE, sslOptions.getStoreType());
        assertEquals(TransportSslOptions.DEFAULT_VERIFY_HOST, sslOptions.isVerifyHost());
        assertNull(sslOptions.getKeyAlias());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithUnknownOption() throws Exception {
        URI BASE_URI = new URI("ssl://localhost:5672?transport.someOption=true");
        NettySslTransportFactory factory = new NettySslTransportFactory();
        factory.createTransport(BASE_URI);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithBadOption() throws Exception {
        URI BASE_URI = new URI("ssl://localhost:5672?transport.trafficClass=4096");
        NettySslTransportFactory factory = new NettySslTransportFactory();
        factory.createTransport(BASE_URI);
    }

    @Test
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
            "transport.verifyHost=" + CUSTOM_VERIFY_HOST + "&" +
            "transport.storeType=" + CUSTOM_STORE_TYPE + "&" +
            "transport.trustAll=" + CUSTOM_TRUST_ALL + "&" +
            "transport.keyAlias=" + CUSTOM_KEY_ALIAS + "&" +
            "transport.contextProtocol=" + CUSTOM_CONTEXT_PROTOCOL + "&" +
            "transport.enabledProtocols=" + CUSTOM_ENABLED_PROTOCOLS_STRING + "&" +
            "transport.disabledProtocols=" + CUSTOM_DISABLED_PROTOCOLS_STRING + "&" +
            "transport.enabledCipherSuites=" + CUSTOM_ENABLED_CIPHER_SUITES_STRING + "&" +
            "transport.disabledCipherSuites=" + CUSTOM_DISABLED_CIPHER_SUITES_STRING);

        NettySslTransportFactory factory = new NettySslTransportFactory();

        Transport transport = factory.createTransport(configuredURI);

        assertNotNull(transport);
        assertTrue(transport instanceof NettyTcpTransport);
        assertFalse(transport.isConnected());
        assertTrue(transport.isSecure());

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

        assertTrue(options instanceof TransportSslOptions);
        TransportSslOptions sslOptions = (TransportSslOptions) options;

        List<String> enabledProtocols = Arrays.asList(sslOptions.getEnabledProtocols());
        List<String> customProtocols = Arrays.asList(CUSTOM_ENABLED_PROTOCOLS);
        assertThat(enabledProtocols, containsInAnyOrder(customProtocols.toArray()));

        List<String> disabledProtocols = Arrays.asList(sslOptions.getDisabledProtocols());
        List<String> customDisabledProtocols = Arrays.asList(CUSTOM_DISABLED_PROTOCOLS);
        assertThat(disabledProtocols, containsInAnyOrder(customDisabledProtocols.toArray()));

        List<String> enabledCipherSuites = Arrays.asList(sslOptions.getEnabledCipherSuites());
        List<String> customChiperSuites = Arrays.asList(CUSTOM_ENABLED_CIPHER_SUITES);
        assertThat(enabledCipherSuites, containsInAnyOrder(customChiperSuites.toArray()));

        List<String> disabledCipherSuites = Arrays.asList(sslOptions.getDisabledCipherSuites());
        List<String> customDisabledChiperSuites = Arrays.asList(CUSTOM_DISABLED_CIPHER_SUITES);
        assertThat(disabledCipherSuites, containsInAnyOrder(customDisabledChiperSuites.toArray()));

        assertEquals(CUSTOM_STORE_TYPE, sslOptions.getStoreType());
        assertEquals(CUSTOM_VERIFY_HOST, sslOptions.isVerifyHost());
        assertEquals(CUSTOM_KEY_ALIAS, sslOptions.getKeyAlias());
        assertEquals(CUSTOM_CONTEXT_PROTOCOL, sslOptions.getContextProtocol());
    }
}
