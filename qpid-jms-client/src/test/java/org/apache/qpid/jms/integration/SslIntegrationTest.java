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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.junit.Test;

public class SslIntegrationTest extends QpidJmsTestCase {

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String PASSWORD = "password";

    private static final String CLIENT_KEY_ALIAS = "client";
    private static final String CLIENT_DN = "O=Client,CN=client";
    private static final String CLIENT2_KEY_ALIAS = "client2";
    private static final String CLIENT2_DN = "O=Client2,CN=client2";

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnection() throws Exception {
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, false);) {
            String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD;
            Connection connection = testFixture.establishConnecton(testPeer, true, connOptions, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithClientAuth() throws Exception {
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_MULTI_KEYSTORE + "&" +
                                 "transport.keyStorePassword=" + PASSWORD + "&" +
                                 "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD;
            Connection connection = testFixture.establishConnecton(testPeer, true, connOptions, null, null, true);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);
            assertNotNull(((SSLSocket) socket).getSession().getPeerPrincipal());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithAlias() throws Exception {
        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN);
    }

    private void doConnectionWithAliasTestImpl(String alias, String expectedDN) throws Exception, JMSException, SSLPeerUnverifiedException, IOException {
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            String connOptions = "?transport.keyStoreLocation=" + CLIENT_MULTI_KEYSTORE + "&" +
                                 "transport.keyStorePassword=" + PASSWORD + "&" +
                                 "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "transport.keyAlias=" + alias;
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
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setTrustStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createSslContext(sslOptions);

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
        }
    }
}
