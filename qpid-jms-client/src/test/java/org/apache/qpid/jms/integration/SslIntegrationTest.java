/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.Socket;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.junit.Test;

import io.netty.handler.ssl.OpenSsl;

public class SslIntegrationTest extends QpidJmsTestCase {

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    private static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String CLIENT_PKCS12_TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";
    private static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-jks.truststore";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_PKCS12_KEYSTORE = "src/test/resources/client-pkcs12.keystore";
    private static final String CLIENT2_JKS_KEYSTORE = "src/test/resources/client2-jks.keystore";
    private static final String CUSTOM_STORE_TYPE_PKCS12 = "pkcs12";
    private static final String PASSWORD = "password";
    private static final String WRONG_PASSWORD = "wrong-password";

    private static final String CLIENT_KEY_ALIAS = "client";
    private static final String CLIENT_DN = "O=Client,CN=client";
    private static final String CLIENT2_KEY_ALIAS = "client2";
    private static final String CLIENT2_DN = "O=Client2,CN=client2";

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionJDK() throws Exception {
        testCreateAndCloseSslConnection(false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnection(true);
    }

    private void testCreateAndCloseSslConnection(boolean openSSL) throws Exception {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, false);) {
            String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "transport.useOpenSSL=" + openSSL;
            Connection connection = testFixture.establishConnecton(testPeer, true, connOptions, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateSslConnectionWithServerSendingPreemptiveDataJDK() throws Exception {
        doTestCreateSslConnectionWithServerSendingPreemptiveData(false);
    }

    @Test(timeout = 20000)
    public void testCreateSslConnectionWithServerSendingPreemptiveDataOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateSslConnectionWithServerSendingPreemptiveData(true);
    }

    private void doTestCreateSslConnectionWithServerSendingPreemptiveData(boolean openSSL) throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        boolean sendServerSaslHeaderPreEmptively = true;
        try (TestAmqpPeer testPeer = new TestAmqpPeer(serverSslContext, false, sendServerSaslHeaderPreEmptively);) {
            // Don't use test fixture, handle the connection directly to control sasl behaviour
            testPeer.expectSaslAnonymousWithPreEmptiveServerHeader();
            testPeer.expectOpen();
            testPeer.expectBegin();

            String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                  "transport.trustStorePassword=" + PASSWORD + "&" +
                                  "transport.useOpenSSL=" + openSSL;

            JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + testPeer.getServerPort() + connOptions);
            Connection connection = factory.createConnection();
            connection.start();

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithClientAuthJDK() throws Exception {
        doTestCreateAndCloseSslConnectionWithClientAuth(false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithClientAuthOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateAndCloseSslConnectionWithClientAuth(true);
    }

    private void doTestCreateAndCloseSslConnectionWithClientAuth(boolean openSSL) throws Exception {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_MULTI_KEYSTORE + "&" +
                                 "transport.keyStorePassword=" + PASSWORD + "&" +
                                 "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "transport.useOpenSSL=" + openSSL;
            Connection connection = testFixture.establishConnecton(testPeer, true, connOptions, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            assertNotNull(((SSLSocket) socket).getSession().getPeerPrincipal());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithAliasJDK() throws Exception {
        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, false);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithAliasOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, true);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, true);
    }

    private void doConnectionWithAliasTestImpl(String alias, String expectedDN, boolean requestOpenSSL) throws Exception, JMSException, SSLPeerUnverifiedException, IOException {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_MULTI_KEYSTORE + "&" +
                                 "transport.keyStorePassword=" + PASSWORD + "&" +
                                 "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "transport.keyAlias=" + alias + "&" +
                                 "transport.useOpenSSL=" + requestOpenSSL;

            Connection connection = testFixture.establishConnecton(testPeer, true, connOptions, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            SSLSession session = ((SSLSocket) socket).getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithAliasThatDoesNotExist() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_DOES_NOT_EXIST);
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithAliasThatDoesNotRepresentKeyEntry() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_CA_CERT);
    }

    private void doCreateConnectionWithInvalidAliasTestImpl(String alias) throws Exception, IOException {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_MULTI_KEYSTORE + "&" +
                                 "transport.keyStorePassword=" + PASSWORD + "&" +
                                 "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "transport.keyAlias=" + alias;

            // DONT use a test fixture, we will drive it directly (because creating the connection will fail).
            JmsConnectionFactory factory = new JmsConnectionFactory("amqps://127.0.0.1:" + testPeer.getServerPort() + connOptions);
            try {
                factory.createConnection();
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            assertNull("Attempt should have failed locally, peer should not have accepted any TCP connection", testPeer.getClientSocket());
        }
    }

    /**
     * Checks that configuring different SSLContext instances using different client key
     * stores via {@link JmsConnectionFactory#setSslContext(SSLContext)} results
     * in different certificates being observed server side following handshake.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testCreateConnectionWithSslContextOverride() throws Exception {
        assertNotEquals(CLIENT_JKS_KEYSTORE, CLIENT2_JKS_KEYSTORE);
        assertNotEquals(CLIENT_DN, CLIENT2_DN);

        // Connect providing the Client 1 details via context override, expect Client1 DN.
        doConnectionWithSslContextOverride(CLIENT_JKS_KEYSTORE, CLIENT_DN, false);
        // Connect providing the Client 2 details via context override, expect Client2 DN instead.
        doConnectionWithSslContextOverride(CLIENT2_JKS_KEYSTORE, CLIENT2_DN, false);
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithSslContextOverrideByExtension() throws Exception {
        assertNotEquals(CLIENT_JKS_KEYSTORE, CLIENT2_JKS_KEYSTORE);
        assertNotEquals(CLIENT_DN, CLIENT2_DN);

        // Connect providing the Client 1 details via context override, expect Client1 DN.
        doConnectionWithSslContextOverride(CLIENT_JKS_KEYSTORE, CLIENT_DN, true);
        // Connect providing the Client 2 details via context override, expect Client2 DN instead.
        doConnectionWithSslContextOverride(CLIENT2_JKS_KEYSTORE, CLIENT2_DN, true);
    }

    private void doConnectionWithSslContextOverride(String clientKeyStorePath, String expectedDN, boolean useExtension) throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverContext = TransportSupport.createJdkSslContext(serverSslOptions);

        TransportOptions clientSslOptions = new TransportOptions();
        clientSslOptions.setKeyStoreLocation(clientKeyStorePath);
        clientSslOptions.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        clientSslOptions.setKeyStorePassword(PASSWORD);
        clientSslOptions.setTrustStorePassword(PASSWORD);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(serverContext, true);) {
            JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + testPeer.getServerPort());

            if (useExtension) {
                factory.setExtension(JmsConnectionExtensions.SSL_CONTEXT.toString(), (options, uri) -> {
                    try {
                        return TransportSupport.createJdkSslContext(clientSslOptions);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });
            } else {
                factory.setSslContext(TransportSupport.createJdkSslContext(clientSslOptions));
            }

            testPeer.expectSaslPlain("guest", "guest");
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection("guest", "guest");
            connection.start();

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            SSLSession session = ((SSLSocket) socket).getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            testPeer.expectClose();
            connection.close();
        }
    }

    /**
     * Checks that configuring an SSLContext instance via
     * {@link JmsConnectionFactory#setSslContext(SSLContext)} overrides URI config
     * for store location etc, resulting in a different certificate being observed
     * server side following handshake.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testCreateConnectionWithSslContextOverrideAndURIConfig() throws Exception {
        assertNotEquals(CLIENT_JKS_KEYSTORE, CLIENT2_JKS_KEYSTORE);
        assertNotEquals(CLIENT_DN, CLIENT2_DN);

        // Connect without providing a context, expect Client1 DN.
        doConnectionWithSslContextOverrideAndURIConfig(null, CLIENT_DN);

        TransportOptions clientSslOptions = new TransportOptions();
        clientSslOptions.setKeyStoreLocation(CLIENT2_JKS_KEYSTORE);
        clientSslOptions.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        clientSslOptions.setKeyStorePassword(PASSWORD);
        clientSslOptions.setTrustStorePassword(PASSWORD);

        SSLContext clientContext = TransportSupport.createJdkSslContext(clientSslOptions);

        // Connect providing the Client 2 details via context override, expect Client2 DN instead.
        doConnectionWithSslContextOverrideAndURIConfig(clientContext, CLIENT2_DN);
    }

    private void doConnectionWithSslContextOverrideAndURIConfig(SSLContext clientContext, String expectedDN) throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverContext = TransportSupport.createJdkSslContext(serverSslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(serverContext, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_JKS_KEYSTORE + "&" +
                    "transport.keyStorePassword=" + PASSWORD + "&" +
                    "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                    "transport.trustStorePassword=" + PASSWORD;

            JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + testPeer.getServerPort() + connOptions);
            factory.setSslContext(clientContext);

            testPeer.expectSaslPlain("guest", "guest");
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection("guest", "guest");
            connection.start();

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            SSLSession session = ((SSLSocket) socket).getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConfigureStoresWithSslSystemProperties() throws Exception {
        // Set properties and expect connection as Client1
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT_DN);

        // Set properties with 'wrong ca' trust store and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, OTHER_CA_TRUSTSTORE, PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong CA");
        } catch (JMSException jmse) {
            // Expected
        }

        // Set properties with wrong key store password and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, WRONG_PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong keystore password");
        } catch (JMSException jmse) {
            // Expected
        }

        // Set properties with wrong trust store password and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, WRONG_PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong truststore password");
        } catch (JMSException jmse) {
            // Expected
        }

        // Set properties and expect connection as Client2
        setSslSystemPropertiesForCurrentTest(CLIENT2_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT2_DN);
    }

    @Test(timeout = 20000)
    public void testConfigurePkcs12StoresWithSslSystemProperties() throws Exception {
        // Set properties and expect connection as Client1
        setSslSystemPropertiesForCurrentTest(CLIENT_PKCS12_KEYSTORE, CUSTOM_STORE_TYPE_PKCS12, PASSWORD, CLIENT_PKCS12_TRUSTSTORE, CUSTOM_STORE_TYPE_PKCS12, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT_DN, true);
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystoreType, String keystorePassword, String truststore, String truststoreType, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_TYPE, keystoreType);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, truststoreType);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private void doConfigureStoresWithSslSystemPropertiesTestImpl(String expectedDN) throws Exception {
        doConfigureStoresWithSslSystemPropertiesTestImpl(expectedDN, false);
    }

    private void doConfigureStoresWithSslSystemPropertiesTestImpl(String expectedDN, boolean usePkcs12Store) throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();

        if (!usePkcs12Store) {
            serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
            serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
            serverSslOptions.setKeyStorePassword(PASSWORD);
            serverSslOptions.setTrustStorePassword(PASSWORD);
            serverSslOptions.setVerifyHost(false);
        } else {
            serverSslOptions.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
            serverSslOptions.setTrustStoreLocation(BROKER_PKCS12_TRUSTSTORE);
            serverSslOptions.setKeyStoreType(CUSTOM_STORE_TYPE_PKCS12);
            serverSslOptions.setTrustStoreType(CUSTOM_STORE_TYPE_PKCS12);
            serverSslOptions.setKeyStorePassword(PASSWORD);
            serverSslOptions.setTrustStorePassword(PASSWORD);
            serverSslOptions.setVerifyHost(false);
        }

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(serverSslContext, true);) {
            Connection connection = testFixture.establishConnecton(testPeer, true, null, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            SSLSession session = ((SSLSocket) socket).getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            testPeer.expectClose();
            connection.close();
        }
    }
}
