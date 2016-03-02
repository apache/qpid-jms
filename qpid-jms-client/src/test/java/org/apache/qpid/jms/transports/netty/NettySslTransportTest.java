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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of the Netty based SSL Transport.
 */
@RunWith(QpidJMSTestRunner.class)
public class NettySslTransportTest extends NettyTcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettySslTransportTest.class);

    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String SERVER_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String SERVER_WRONG_HOST_KEYSTORE = "src/test/resources/broker-wrong-host-jks.keystore";
    public static final String CLIENT_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    public static final String CLIENT_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    public static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-jks.truststore";

    public static final String CLIENT_KEY_ALIAS = "client";
    public static final String CLIENT_DN = "O=Client,CN=client";
    public static final String CLIENT2_KEY_ALIAS = "client2";
    public static final String CLIENT2_DN = "O=Client2,CN=client2";

    public static final String KEYSTORE_TYPE = "jks";

    @Override
    @Test(timeout = 60 * 1000)
    public void testCreateWithNullOptionsUsesDefaults() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        Transport transport = createTransport(serverLocation, testListener, null);
        assertEquals(TransportSslOptions.INSTANCE, transport.getTransportOptions());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithoutTrustStoreFails() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptionsWithoutTrustStore(false));
            try {
                transport.connect();
                fail("Should not have connected to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        logTransportErrors();

        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    @Repeat(repetitions = 1)
    public void testConnectToServerUsingUntrustedKeyFails() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

            options.setTrustStoreLocation(OTHER_CA_TRUSTSTORE);
            options.setTrustStorePassword(PASSWORD);

            Transport transport = createTransport(serverLocation, testListener, options);
            try {
                transport.connect();
                fail("Should not have connected to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerClientTrustsAll() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptionsWithoutTrustStore(true));
            try {
                transport.connect();
                LOG.info("Connection established to untrusted test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectWithNeedClientAuth() throws Exception {
        TransportSslOptions serverOptions = createServerOptions();

        try (NettyEchoServer server = new NettyEchoServer(serverOptions, true)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions clientOptions = createClientOptions();

            NettySslTransport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect();
                LOG.info("Connection established to test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            // Verify there was a certificate sent to the server
            assertNotNull(server.getSslHandler().engine().getSession().getPeerCertificates());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectWithSpecificClientAuthKeyAlias() throws Exception {
        doClientAuthAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN);
        doClientAuthAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN);
    }

    private void doClientAuthAliasTestImpl(String alias, String expectedDN) throws Exception, URISyntaxException, IOException, InterruptedException {
        TransportSslOptions serverOptions = createServerOptions();

        try (NettyEchoServer server = new NettyEchoServer(serverOptions, true)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions clientOptions = createClientOptions();
            clientOptions.setKeyStoreLocation(CLIENT_MULTI_KEYSTORE);
            clientOptions.setKeyAlias(alias);

            NettySslTransport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect();
                LOG.info("Connection established to test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server: " + serverLocation);
            }

            assertTrue(transport.isConnected());

            Certificate[] peerCertificates = server.getSslHandler().engine().getSession().getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(true);
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerNoVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(false);
    }

    private void doConnectToServerVerifyHostTestImpl(boolean verifyHost) throws Exception, URISyntaxException, IOException, InterruptedException {
        TransportSslOptions serverOptions = createServerOptions();
        serverOptions.setKeyStoreLocation(SERVER_WRONG_HOST_KEYSTORE);

        try (NettyEchoServer server = new NettyEchoServer(serverOptions)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions clientOptions = createClientOptionsIsVerify(verifyHost);

            if (verifyHost) {
                assertTrue("Expected verifyHost to be true", clientOptions.isVerifyHost());
            } else {
                assertFalse("Expected verifyHost to be false", clientOptions.isVerifyHost());
            }

            Transport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect();
                if (verifyHost) {
                    fail("Should not have connected to the server: " + serverLocation);
                }
            } catch (Exception e) {
                if (verifyHost) {
                    LOG.info("Connection failed to test server: {} as expected.", serverLocation);
                } else {
                    LOG.error("Failed to connect to test server: " + serverLocation, e);
                    fail("Should have connected to the server: " + serverLocation + ", but got: " + e);
                }
            }

            if (verifyHost) {
                assertFalse(transport.isConnected());
            } else {
                assertTrue(transport.isConnected());
            }

            transport.close();
        }
    }

    @Override
    protected NettySslTransport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        return new NettySslTransport(listener, serverLocation, options);
    }

    @Override
    protected TransportSslOptions createClientOptions() {
        return createClientOptionsIsVerify(false);
    }

    protected TransportSslOptions createClientOptionsIsVerify(boolean verifyHost) {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(CLIENT_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(verifyHost);

        return options;
    }

    @Override
    protected TransportSslOptions createServerOptions() {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(SERVER_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(SERVER_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(false);

        return options;
    }

    protected TransportSslOptions createClientOptionsWithoutTrustStore(boolean trustAll) {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setStoreType(KEYSTORE_TYPE);
        options.setTrustAll(trustAll);

        return options;
    }
}
