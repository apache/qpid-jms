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

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSSecurityException;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SaslGssApiIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SaslGssApiIntegrationTest.class);

    private static final String LOGIN_CONFIG = "SaslGssApiIntegrationTest-login.config";
    private static final String GSSAPI = "GSSAPI";
    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final String KRB5_KEYTAB = "target/SaslGssApiIntegrationTest.krb5.keytab";
    private static final String SERVICE_PRINCIPAL = "amqp/localhost";
    private static final String CLIENT_PRINCIPAL_LOGIN_CONFIG = "clientprincipal";
    private static final String CLIENT_PRINCIPAL_FACTORY_USERNAME = "factoryusername";
    private static final String CLIENT_PRINCIPAL_URI_USERNAME = "uriusername";
    private static final String CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE = "defaultscopeprincipal";

    private static MiniKdc kdc;
    private static final boolean DEBUG = false;

    @BeforeClass
    public static void setUpKerberos() throws Exception {
        Path targetDir = FileSystems.getDefault().getPath("target");
        Path tempDirectory = Files.createTempDirectory(targetDir, "junit.SaslGssApiIntegrationTest.");
        File root = tempDirectory.toFile();

        kdc = new MiniKdc(MiniKdc.createConf(), new File(root, "kdc"));
        kdc.start();

        // hard coded match, default_keytab_name in minikdc-krb5.conf template
        File userKeyTab = new File(KRB5_KEYTAB);
        kdc.createPrincipal(userKeyTab, CLIENT_PRINCIPAL_LOGIN_CONFIG, CLIENT_PRINCIPAL_FACTORY_USERNAME,
                CLIENT_PRINCIPAL_URI_USERNAME, CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE, SERVICE_PRINCIPAL);

        if (DEBUG) {
            Keytab kt = Keytab.read(userKeyTab);
            for (KeytabEntry entry : kt.getEntries()) {
                LOG.info("KeyTab Entry: PrincipalName:" + entry.getPrincipalName() + " ; KeyInfo:"+ entry.getKey().getKeyType());
            }

            java.util.logging.Logger logger = java.util.logging.Logger.getLogger("javax.security.sasl");
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
                handler.setLevel(java.util.logging.Level.FINEST);
            }
        }
    }

    @AfterClass
    public static void cleanUpKerberos() {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        Assume.assumeFalse(System.getProperty("java.vendor").contains("IBM"));

        // NOTE: we may need to isolate this test later if we use login.config in others
        setTestSystemProperty("java.security.auth.login.config",
                SaslGssApiIntegrationTest.class.getClassLoader().getResource(LOGIN_CONFIG).getPath());
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConnection() throws Exception {
        doSaslGssApiKrbConnectionTestImpl("KRB5-CLIENT", CLIENT_PRINCIPAL_LOGIN_CONFIG + "@EXAMPLE.COM");
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConnectionWithDefaultScope() throws Exception {
        doSaslGssApiKrbConnectionTestImpl(null, CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE + "@EXAMPLE.COM");
    }

    private void doSaslGssApiKrbConnectionTestImpl(String configScope, String clientAuthIdAtServer) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(SERVICE_PRINCIPAL, KRB5_KEYTAB, clientAuthIdAtServer);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?amqp.saslMechanisms=" + GSSAPI;
            if(configScope != null) {
                uriOptions += "&sasl.options.configScope=" + configScope;
            }

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);
            Connection connection = factory.createConnection("ignoredusername", null);
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConnectionWithPrincipalViaJmsUsernameUri() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(SERVICE_PRINCIPAL, KRB5_KEYTAB, CLIENT_PRINCIPAL_URI_USERNAME + "@EXAMPLE.COM");
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            // No password, not needed as using keyTab.
            String uriOptions = "?sasl.options.configScope=KRB5-CLIENT-URI-USERNAME-CALLBACK&jms.username="
                                + CLIENT_PRINCIPAL_URI_USERNAME +"&amqp.saslMechanisms=" + GSSAPI;
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);

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
    public void testSaslGssApiKrbConnectionWithPrincipalViaJmsUsernameConnFactory() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(SERVICE_PRINCIPAL, KRB5_KEYTAB, CLIENT_PRINCIPAL_FACTORY_USERNAME + "@EXAMPLE.COM");
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?sasl.options.configScope=KRB5-CLIENT-FACTORY-USERNAME-CALLBACK" + "&amqp.saslMechanisms=" + GSSAPI;
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);

            // No password, not needed as using keyTab.
            Connection connection = factory.createConnection(CLIENT_PRINCIPAL_FACTORY_USERNAME, null);

            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConfigError() throws Exception {
        final String loginConfigScope = "KRB5-CLIENT-DOES-NOT-EXIST";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslGSSAPIFail();

            String uriOptions = "?sasl.options.configScope=" + loginConfigScope + "&amqp.saslMechanisms=" + GSSAPI;
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);
            factory.createConnection();

            fail("Expect exception on no login config");
        } catch (JMSSecurityException expected) {
            assertTrue(expected.getMessage().contains(loginConfigScope));
        }
    }

    @Test(timeout = 20000)
    public void testGssapiOnlySelectedWhenPresentIfExplicitlyEnabled() throws Exception {
        doMechanismSelectedTestImpl("username", "password", PLAIN, new Symbol[] {Symbol.valueOf(GSSAPI), PLAIN, ANONYMOUS}, false);
        doMechanismSelectedTestImpl("username", "password", Symbol.valueOf(GSSAPI), new Symbol[] {Symbol.valueOf(GSSAPI), PLAIN, ANONYMOUS}, true);
    }

    private void doMechanismSelectedTestImpl(String username, String password, Symbol clientSelectedMech, Symbol[] serverMechs, boolean enableGssapiExplicitly) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectFailingSaslAuthentication(serverMechs, clientSelectedMech);

            String uriOptions = "?jms.clientID=myclientid";
            if(enableGssapiExplicitly) {
                uriOptions += "&amqp.saslMechanisms=PLAIN," + GSSAPI;
            }
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);

            try {
                factory.createConnection(username, password);
                fail("Excepted exception to be thrown");
            }catch (JMSSecurityException jmsse) {
                // Expected, we deliberately failed the SASL process,
                // we only wanted to verify the correct mechanism
                // was selected, other tests verify the remainder.

                LOG.info("Caught expected security exception: {}", jmsse.getMessage());
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
