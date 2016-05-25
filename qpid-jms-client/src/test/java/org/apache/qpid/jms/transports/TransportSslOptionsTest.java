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
package org.apache.qpid.jms.transports;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

/**
 * Test for class TransportSslOptions
 */
public class TransportSslOptionsTest extends QpidJmsTestCase {

    public static final String PASSWORD = "password";
    public static final String CLIENT_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    public static final String KEYSTORE_TYPE = "jks";
    public static final String KEY_ALIAS = "myTestAlias";
    public static final String CONTEXT_PROTOCOL = "TLSv1.1";
    public static final boolean TRUST_ALL = true;
    public static final boolean VERIFY_HOST = true;

    public static final int TEST_SEND_BUFFER_SIZE = 128 * 1024;
    public static final int TEST_RECEIVE_BUFFER_SIZE = TEST_SEND_BUFFER_SIZE;
    public static final int TEST_TRAFFIC_CLASS = 1;
    public static final boolean TEST_TCP_NO_DELAY = false;
    public static final boolean TEST_TCP_KEEP_ALIVE = true;
    public static final int TEST_SO_LINGER = Short.MAX_VALUE;
    public static final int TEST_SO_TIMEOUT = 10;
    public static final int TEST_CONNECT_TIMEOUT = 90000;
    public static final int TEST_DEFAULT_SSL_PORT = 5681;

    public static final String[] ENABLED_PROTOCOLS = new String[] {"TLSv1.2"};
    public static final String[] DISABLED_PROTOCOLS = new String[] {"SSLv3", "TLSv1.2"};
    public static final String[] ENABLED_CIPHERS = new String[] {"CIPHER_A", "CIPHER_B"};
    public static final String[] DISABLED_CIPHERS = new String[] {"CIPHER_C"};

    @Test
    public void testCreate() {
        TransportSslOptions options = new TransportSslOptions();

        assertEquals(TransportSslOptions.DEFAULT_TRUST_ALL, options.isTrustAll());
        assertEquals(TransportSslOptions.DEFAULT_STORE_TYPE, options.getStoreType());

        assertEquals(TransportSslOptions.DEFAULT_CONTEXT_PROTOCOL, options.getContextProtocol());
        assertNull(options.getEnabledProtocols());
        assertArrayEquals(TransportSslOptions.DEFAULT_DISABLED_PROTOCOLS.toArray(new String[0]),
                          options.getDisabledProtocols());
        assertNull(options.getEnabledCipherSuites());
        assertNull(options.getDisabledCipherSuites());

        assertNull(options.getKeyStoreLocation());
        assertNull(options.getKeyStorePassword());
        assertNull(options.getTrustStoreLocation());
        assertNull(options.getTrustStorePassword());
        assertNull(options.getKeyAlias());
    }

    @Test
    public void testCreateAndConfigure() {
        TransportSslOptions options = createSslOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());

        assertEquals(CLIENT_KEYSTORE, options.getKeyStoreLocation());
        assertEquals(PASSWORD, options.getKeyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.getTrustStoreLocation());
        assertEquals(PASSWORD, options.getTrustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.getStoreType());
        assertEquals(KEY_ALIAS, options.getKeyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.getContextProtocol());
        assertArrayEquals(ENABLED_PROTOCOLS,options.getEnabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.getDisabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.getEnabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.getDisabledCipherSuites());
    }

    @Test
    public void testClone() {
        TransportSslOptions options = createSslOptions().clone();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TEST_DEFAULT_SSL_PORT, options.getDefaultSslPort());

        assertEquals(CLIENT_KEYSTORE, options.getKeyStoreLocation());
        assertEquals(PASSWORD, options.getKeyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.getTrustStoreLocation());
        assertEquals(PASSWORD, options.getTrustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.getStoreType());
        assertEquals(KEY_ALIAS, options.getKeyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.getContextProtocol());
        assertArrayEquals(ENABLED_PROTOCOLS,options.getEnabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.getDisabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.getEnabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.getDisabledCipherSuites());
    }

    private TransportSslOptions createSslOptions() {
        TransportSslOptions options = new TransportSslOptions();

        options.setKeyStoreLocation(CLIENT_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setTrustAll(TRUST_ALL);
        options.setVerifyHost(VERIFY_HOST);
        options.setKeyAlias(KEY_ALIAS);
        options.setContextProtocol(CONTEXT_PROTOCOL);
        options.setEnabledProtocols(ENABLED_PROTOCOLS);
        options.setDisabledProtocols(DISABLED_PROTOCOLS);
        options.setEnabledCipherSuites(ENABLED_CIPHERS);
        options.setDisabledCipherSuites(DISABLED_CIPHERS);

        options.setSendBufferSize(TEST_SEND_BUFFER_SIZE);
        options.setReceiveBufferSize(TEST_RECEIVE_BUFFER_SIZE);
        options.setTrafficClass(TEST_TRAFFIC_CLASS);
        options.setTcpNoDelay(TEST_TCP_NO_DELAY);
        options.setTcpKeepAlive(TEST_TCP_KEEP_ALIVE);
        options.setSoLinger(TEST_SO_LINGER);
        options.setSoTimeout(TEST_SO_TIMEOUT);
        options.setConnectTimeout(TEST_CONNECT_TIMEOUT);
        options.setDefaultSslPort(TEST_DEFAULT_SSL_PORT);

        return options;
    }
}
