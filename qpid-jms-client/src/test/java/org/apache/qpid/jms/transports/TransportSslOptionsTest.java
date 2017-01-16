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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for class TransportSslOptions
 */
public class TransportSslOptionsTest extends QpidJmsTestCase {

    private static final String PASSWORD = "password";
    private static final String CLIENT_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String KEYSTORE_TYPE = "jks";
    private static final String KEY_ALIAS = "myTestAlias";
    private static final String CONTEXT_PROTOCOL = "TLSv1.1";
    private static final boolean TRUST_ALL = true;
    private static final boolean VERIFY_HOST = true;

    private static final int TEST_SEND_BUFFER_SIZE = 128 * 1024;
    private static final int TEST_RECEIVE_BUFFER_SIZE = TEST_SEND_BUFFER_SIZE;
    private static final int TEST_TRAFFIC_CLASS = 1;
    private static final boolean TEST_TCP_NO_DELAY = false;
    private static final boolean TEST_TCP_KEEP_ALIVE = true;
    private static final int TEST_SO_LINGER = Short.MAX_VALUE;
    private static final int TEST_SO_TIMEOUT = 10;
    private static final int TEST_CONNECT_TIMEOUT = 90000;
    private static final int TEST_DEFAULT_SSL_PORT = 5681;

    private static final String[] ENABLED_PROTOCOLS = new String[] {"TLSv1.2"};
    private static final String[] DISABLED_PROTOCOLS = new String[] {"SSLv3", "TLSv1.2"};
    private static final String[] ENABLED_CIPHERS = new String[] {"CIPHER_A", "CIPHER_B"};
    private static final String[] DISABLED_CIPHERS = new String[] {"CIPHER_C"};

    private static final SSLContext SSL_CONTEXT = Mockito.mock(SSLContext.class);

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

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
        assertNull(options.getSslContextOverride());
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
        assertEquals(SSL_CONTEXT, options.getSslContextOverride());
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
        assertEquals(SSL_CONTEXT, options.getSslContextOverride());
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
        options.setSslContextOverride(SSL_CONTEXT);

        return options;
    }

    @Test
    public void testSslSystemPropertiesInfluenceDefaults() {
        String keystore = "keystore";
        String keystorePass = "keystorePass";
        String truststore = "truststore";
        String truststorePass = "truststorePass";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        TransportSslOptions options1 = new TransportSslOptions();

        assertEquals(keystore, options1.getKeyStoreLocation());
        assertEquals(keystorePass, options1.getKeyStorePassword());
        assertEquals(truststore, options1.getTrustStoreLocation());
        assertEquals(truststorePass, options1.getTrustStorePassword());

        keystore +="2";
        keystorePass +="2";
        truststore +="2";
        truststorePass +="2";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        TransportSslOptions options2 = new TransportSslOptions();

        assertEquals(keystore, options2.getKeyStoreLocation());
        assertEquals(keystorePass, options2.getKeyStorePassword());
        assertEquals(truststore, options2.getTrustStoreLocation());
        assertEquals(truststorePass, options2.getTrustStorePassword());

        assertNotEquals(options1.getKeyStoreLocation(), options2.getKeyStoreLocation());
        assertNotEquals(options1.getKeyStorePassword(), options2.getKeyStorePassword());
        assertNotEquals(options1.getTrustStoreLocation(), options2.getTrustStoreLocation());
        assertNotEquals(options1.getTrustStorePassword(), options2.getTrustStorePassword());
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

}
