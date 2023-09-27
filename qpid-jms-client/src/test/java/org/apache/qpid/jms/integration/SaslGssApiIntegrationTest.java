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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSSecurityException;

import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslGssApiIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SaslGssApiIntegrationTest.class);

    private static final String LOGIN_CONFIG = "SaslGssApiIntegrationTest-login.config";
    private static final String GSSAPI = "GSSAPI";
    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final String KRB5_TCP_PORT_TEMPLATE = "MINI_KDC_PORT";
    private static final String KRB5_CONFIG_TEMPLATE = "minikdc-krb5-template.conf";
    private static final String KRB5_KEYTAB = "target/SaslGssApiIntegrationTest.krb5.keytab";
    private static final String CLIENT_PRINCIPAL_LOGIN_CONFIG = "clientprincipal";
    private static final String CLIENT_PRINCIPAL_FACTORY_USERNAME = "factoryusername";
    private static final String CLIENT_PRINCIPAL_URI_USERNAME = "uriusername";
    private static final String CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE = "defaultscopeprincipal";

    private static String servicePrincipal;
    private static MiniKdc kdc;
    private static final boolean DEBUG = false;

    @BeforeAll
    public static void setUpKerberos() throws Exception {
        servicePrincipal = prepareServiceName();
        LOG.info("Using service principal: " + servicePrincipal);

        Path targetDir = FileSystems.getDefault().getPath("target");
        Path tempDirectory = Files.createTempDirectory(targetDir, "junit.SaslGssApiIntegrationTest.");
        File root = tempDirectory.toFile();

        Properties kdcConf = MiniKdc.createConf();
        kdcConf.setProperty("debug", Boolean.toString(DEBUG));

        kdc = new MiniKdc(kdcConf, new File(root, "kdc"));
        kdc.start();

        File userKeyTab = new File(KRB5_KEYTAB);
        kdc.createPrincipal(userKeyTab, CLIENT_PRINCIPAL_LOGIN_CONFIG, CLIENT_PRINCIPAL_FACTORY_USERNAME,
                CLIENT_PRINCIPAL_URI_USERNAME, CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE, servicePrincipal);

        // We need to hard code the default keyTab into the Krb5 configuration file which is not possible
        // with this version of MiniKDC so we use a template file and replace the port with the value from
        // the MiniKDC instance we just started.
        rewriteKrbConfFile(kdc);

        if (DEBUG) {
            LOG.debug("java.security.krb5.conf='{}'", System.getProperty("java.security.krb5.conf"));
            try (BufferedReader br = new BufferedReader(new FileReader(System.getProperty("java.security.krb5.conf")))) {
                br.lines().forEach(line -> LOG.debug(line));
            }

            Keytab kt = Keytab.loadKeytab(userKeyTab);
            for (PrincipalName name : kt.getPrincipals()) {
                for (KeytabEntry entry : kt.getKeytabEntries(name)) {
                    LOG.info("KeyTab Entry: PrincipalName:" + entry.getPrincipal() + " ; KeyInfo:"+ entry.getKey().getKeyType());
                }
            }

            java.util.logging.Logger logger = java.util.logging.Logger.getLogger("javax.security.sasl");
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
                handler.setLevel(java.util.logging.Level.FINEST);
            }

            logger = java.util.logging.Logger.getLogger("logincontext");
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
                handler.setLevel(java.util.logging.Level.FINEST);
            }
        }
    }

    private static String prepareServiceName() {
        InetSocketAddress addr = new InetSocketAddress("localhost", 0);
        InetAddress inetAddress = addr.getAddress();
        if (inetAddress != null) {
            if ("localhost.localdomain".equals(inetAddress.getCanonicalHostName())) {
                return "amqp/localhost.localdomain";
            }
        }

        return "amqp/localhost";
    }

    @AfterAll
    public static void cleanUpKerberos() {
        if (kdc != null) {
           kdc.stop();
        }
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        Assumptions.assumeFalse(System.getProperty("java.vendor").contains("IBM"));

        // NOTE: we may need to isolate this test later if we use login.config in others
        setTestSystemProperty("java.security.auth.login.config",
                SaslGssApiIntegrationTest.class.getClassLoader().getResource(LOGIN_CONFIG).getPath());
    }

    @Test
    @Timeout(20)
    public void testSaslGssApiKrbConnection() throws Exception {
        doSaslGssApiKrbConnectionTestImpl("KRB5-CLIENT", CLIENT_PRINCIPAL_LOGIN_CONFIG + "@EXAMPLE.COM");
    }

    @Test
    @Timeout(20)
    public void testSaslGssApiKrbConnectionWithDefaultScope() throws Exception {
        doSaslGssApiKrbConnectionTestImpl(null, CLIENT_PRINCIPAL_DEFAULT_CONFIG_SCOPE + "@EXAMPLE.COM");
    }

    private void doSaslGssApiKrbConnectionTestImpl(String configScope, String clientAuthIdAtServer) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslGSSAPI(servicePrincipal, KRB5_KEYTAB, clientAuthIdAtServer);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?amqp.saslMechanisms=" + GSSAPI;
            if (configScope != null) {
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

    @Test
    @Timeout(20)
    public void testSaslGssApiKrbConnectionWithPrincipalViaJmsUsernameUri() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslGSSAPI(servicePrincipal, KRB5_KEYTAB, CLIENT_PRINCIPAL_URI_USERNAME + "@EXAMPLE.COM");
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            // No password, not needed as using keyTab.
            String uriOptions = "?sasl.options.configScope=KRB5-CLIENT-URI-USERNAME-CALLBACK&jms.username="
                                + CLIENT_PRINCIPAL_URI_USERNAME +
                                "&amqp.saslMechanisms=" + GSSAPI;
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

    @Test
    @Timeout(20)
    public void testSaslGssApiKrbConnectionWithPrincipalViaJmsUsernameConnFactory() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslGSSAPI(servicePrincipal, KRB5_KEYTAB, CLIENT_PRINCIPAL_FACTORY_USERNAME + "@EXAMPLE.COM");
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

    @Test
    @Timeout(20)
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

    @Test
    @Timeout(20)
    public void testGssapiOnlySelectedWhenPresentIfExplicitlyEnabled() throws Exception {
        doMechanismSelectedTestImpl("username", "password", PLAIN, new Symbol[] {Symbol.valueOf(GSSAPI), PLAIN, ANONYMOUS}, false);
        doMechanismSelectedTestImpl("username", "password", Symbol.valueOf(GSSAPI), new Symbol[] {Symbol.valueOf(GSSAPI), PLAIN, ANONYMOUS}, true);
    }

    private void doMechanismSelectedTestImpl(String username, String password, Symbol clientSelectedMech, Symbol[] serverMechs, boolean enableGssapiExplicitly) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslFailingAuthentication(serverMechs, clientSelectedMech);

            String uriOptions = "?jms.clientID=myclientid";
            if (enableGssapiExplicitly) {
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

    private static void rewriteKrbConfFile(MiniKdc server) throws Exception {
        final Path template = Paths.get(SaslGssApiIntegrationTest.class.getClassLoader().getResource(KRB5_CONFIG_TEMPLATE).toURI());
        final String krb5confTemplate = new String(Files.readAllBytes(template), StandardCharsets.UTF_8);
        final String replacementPort = Integer.toString(server.getPort());

        // Replace the port template with the current actual port of the MiniKDC Server instance.
        final String krb5confUpdated = krb5confTemplate.replaceAll(KRB5_TCP_PORT_TEMPLATE, replacementPort);

        try (OutputStream outputStream = Files.newOutputStream(kdc.getKrb5conf().toPath());
             WritableByteChannel channel = Channels.newChannel(outputStream)) {

            channel.write(ByteBuffer.wrap(krb5confUpdated.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
