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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.net.URLDecoder;
import java.net.URLEncoder;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SaslIntegrationTest.class);

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final Symbol CRAM_MD5 = Symbol.valueOf("CRAM-MD5");
    private static final Symbol SCRAM_SHA_1 = Symbol.valueOf("SCRAM-SHA-1");
    private static final Symbol SCRAM_SHA_256 = Symbol.valueOf("SCRAM-SHA-256");
    private static final Symbol EXTERNAL = Symbol.valueOf("EXTERNAL");

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String PASSWORD = "password";

    @Test(timeout = 20000)
    public void testSaslExternalConnection() throws Exception {
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);
        sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        sslOptions.setTrustStorePassword(PASSWORD);

        String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                             "transport.trustStorePassword=" + PASSWORD + "&" +
                             "transport.keyStoreLocation=" + CLIENT_JKS_KEYSTORE + "&" +
                             "transport.keyStorePassword=" + PASSWORD;

        SSLContext context = TransportSupport.createSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, true);) {
            // Expect an EXTERNAL connection
            testPeer.expectSaslExternal();
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + testPeer.getServerPort() + connOptions);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslPlainConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection(user, pass);
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslPlainConnectionWithURIEncodedCredentials() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection with decoded password from URL encoded value.
            String user = "user";
            String pass = " CN24tCa+Hn/av";

            // If double decoded this value results in " CN24tCa Hn/av" as the decoded plus
            // becomes a valid encoding for a space character and would be removed.
            String encodedPass = "+CN24tCa%2BHn%2Fav";

            String urlEncodedPassword = URLEncoder.encode(pass, "UTF-8");
            String urlDecodedPassword = URLDecoder.decode(pass, "UTF-8");

            // Inadvertent double decoding of the password should result in a different value
            // which would fail this test.
            assertEquals(encodedPass, urlEncodedPassword);
            assertFalse(urlEncodedPassword.equals(urlDecodedPassword));
            assertFalse(pass.equals(urlDecodedPassword));

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort() +
                "?jms.username=" + user + "&jms.password=" + encodedPass);

            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslAnonymousConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Expect an ANOYMOUS connection
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    /**
     * Add a small delay after the SASL process fails, test peer will throw if
     * any unexpected frames arrive, such as erroneous open+close.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testWaitForUnexpectedFramesAfterSaslFailure() throws Exception {
        doMechanismSelectedTestImpl(null, null, ANONYMOUS, new Symbol[] {ANONYMOUS}, true);
    }

    @Test(timeout = 20000)
    public void testAnonymousSelectedWhenNoCredentialsWereSupplied() throws Exception {
        doMechanismSelectedTestImpl(null, null, ANONYMOUS, new Symbol[] {CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test(timeout = 20000)
    public void testAnonymousSelectedWhenNoPasswordWasSupplied() throws Exception {
        doMechanismSelectedTestImpl("username", null, ANONYMOUS, new Symbol[] {CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test(timeout = 20000)
    public void testCramMd5SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", CRAM_MD5, new Symbol[] {CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test(timeout = 20000)
    public void testScramSha1SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", SCRAM_SHA_1, new Symbol[] {SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test(timeout = 20000)
    public void testScramSha256SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", SCRAM_SHA_256, new Symbol[] {SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    private void doMechanismSelectedTestImpl(String username, String password, Symbol clientSelectedMech, Symbol[] serverMechs, boolean wait) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectFailingSaslAuthentication(serverMechs, clientSelectedMech);

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?jms.clientID=myclientid");
            try {
                factory.createConnection(username, password);
                fail("Excepted exception to be thrown");
            }catch (JMSSecurityException jmsse) {
                // Expected, we deliberately failed the SASL process,
                // we only wanted to verify the correct mechanism
                // was selected, other tests verify the remainder.

                LOG.info("Caught expected security exception: {}", jmsse.getMessage());
            }

            if (wait) {
                Thread.sleep(200);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testExternalSelectedWhenLocalPrincipalPresent() throws Exception {
        doMechanismSelectedExternalTestImpl(true, EXTERNAL, new Symbol[] {EXTERNAL, SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS});
    }

    @Test(timeout = 20000)
    public void testExternalNotSelectedWhenLocalPrincipalMissing() throws Exception {
        doMechanismSelectedExternalTestImpl(false, ANONYMOUS, new Symbol[] {EXTERNAL, SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS});
    }

    private void doMechanismSelectedExternalTestImpl(boolean requireClientCert, Symbol clientSelectedMech, Symbol[] serverMechs) throws Exception {
        TransportSslOptions sslOptions = new TransportSslOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);
        if (requireClientCert) {
            sslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
            sslOptions.setTrustStorePassword(PASSWORD);
        }

        SSLContext context = TransportSupport.createSslContext(sslOptions);

        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, requireClientCert);) {
            String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                 "transport.trustStorePassword=" + PASSWORD + "&" +
                                 "jms.clientID=myclientid";
            if (requireClientCert) {
                connOptions += "&transport.keyStoreLocation=" + CLIENT_JKS_KEYSTORE + "&" +
                               "transport.keyStorePassword=" + PASSWORD;
            }

            testPeer.expectFailingSaslAuthentication(serverMechs, clientSelectedMech);

            JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + testPeer.getServerPort() + connOptions);
            try {
                factory.createConnection();
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSaslLayerDisabledConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Expect a connection with no SASL layer.
            testPeer.expectSaslLayerDisabledConnect(null);
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?amqp.saslLayer=false");
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRestrictSaslMechanismsWithSingleMech() throws Exception {
        // Check PLAIN gets picked when we don't specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", PLAIN, new Symbol[] { PLAIN, ANONYMOUS}, null);

        // Check ANONYMOUS gets picked when we do specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", ANONYMOUS, new Symbol[] { PLAIN, ANONYMOUS}, "ANONYMOUS");
    }

    @Test(timeout = 20000)
    public void testRestrictSaslMechanismsWithMultipleMechs() throws Exception {
        // Check CRAM-MD5 gets picked when we dont specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", CRAM_MD5, new Symbol[] {CRAM_MD5, PLAIN, ANONYMOUS}, null);

        // Check PLAIN gets picked when we specify a restriction with multiple mechs
        doMechanismSelectionRestrictedTestImpl("username", "password", PLAIN, new Symbol[] { CRAM_MD5, PLAIN, ANONYMOUS}, "PLAIN,ANONYMOUS");
    }

    @Test(timeout = 20000)
    public void testRestrictSaslMechanismsWithMultipleMechsNoPassword() throws Exception {
        // Check ANONYMOUS gets picked when we specify a restriction with multiple mechs but don't give a password
        doMechanismSelectionRestrictedTestImpl("username", null, ANONYMOUS, new Symbol[] { CRAM_MD5, PLAIN, ANONYMOUS}, "PLAIN,ANONYMOUS");
    }

    private void doMechanismSelectionRestrictedTestImpl(String username, String password, Symbol clientSelectedMech, Symbol[] serverMechs, String mechanismsOptionValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectFailingSaslAuthentication(serverMechs, clientSelectedMech);

            String uriOptions = "?jms.clientID=myclientid";
            if(mechanismsOptionValue != null) {
                uriOptions += "&amqp.saslMechanisms=" + mechanismsOptionValue;
            }

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);
            try {
                factory.createConnection(username, password);

                fail("Excepted exception to be thrown");
            }catch (JMSSecurityException jmsse) {
                // Expected, we deliberately failed the SASL process,
                // we only wanted to verify the correct mechanism
                // was selected, other tests verify the remainder.
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
