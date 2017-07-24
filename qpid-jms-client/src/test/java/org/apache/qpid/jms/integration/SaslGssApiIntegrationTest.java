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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSSecurityException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SaslGssApiIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SaslGssApiIntegrationTest.class);

    private static final Symbol GSSAPI = Symbol.valueOf("GSSAPI");
    private static final String serviceName = "amqp/localhost";

    private MiniKdc kdc;
    private final boolean debug = false;

    @Before
    public void setUpKerberos() throws Exception {
        Path tempDirectory = Files.createTempDirectory("junit.SaslGssApiIntegrationTest.");
        File root = tempDirectory.toFile();
        root.deleteOnExit();
        kdc = new MiniKdc(MiniKdc.createConf(), new File(root, "kdc"));
        kdc.start();

        // hard coded match, default_keytab_name in minikdc-krb5.conf template
        File userKeyTab = new File("target/SaslGssApiIntegrationTest.krb5.keytab");
        kdc.createPrincipal(userKeyTab, "client", serviceName);

        Keytab kt = Keytab.read(userKeyTab);
        for (KeytabEntry entry : kt.getEntries()) {
            LOG.info("KeyTab Kerb PrincipalNames:" + entry.getPrincipalName());
        }

        if (debug) {
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger("javax.security.sasl");
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
                handler.setLevel(java.util.logging.Level.FINEST);
            }
        }
    }

    @After
    public void stopKDC() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(serviceName);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?amqp.saslMechanisms=" + GSSAPI.toString();
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);
            Connection connection = factory.createConnection("client", null);
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSaslGssApiKrbConnectionJmsUser() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(serviceName);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?jms.username=client&amqp.saslMechanisms=" + GSSAPI.toString();
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
    public void testSaslGssApiKrb5ConfigOptionOverridePrincipal() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(serviceName);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?jms.username=getsOverridden&sasl.krb5.principal=client&amqp.saslMechanisms=" + GSSAPI.toString();
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
    public void testSaslGssApiKrbConfigConnection() throws Exception {
        setTestSystemProperty("java.security.auth.login.config",
                SaslGssApiIntegrationTest.class.getClassLoader().getResource("SaslGssApiIntegrationTest-login.config").getPath());
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPI(serviceName);
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uriOptions = "?sasl.configScope=KRB5-CLIENT&sasl.protocol=amqp&sasl.server=localhost&amqp.saslMechanisms=" + GSSAPI.toString();
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
    public void testSaslGssApiKrbConfigError() throws Exception {
        final String loginConfigScope = "KRB5-CLIENT-DOES-NOT-EXIST";
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslGSSAPIFail();

            String uriOptions = "?sasl.configScope=" + loginConfigScope + "&sasl.protocol=amqp&sasl.server=localhost&amqp.saslMechanisms=" + GSSAPI.toString();
            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + uriOptions);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.expectClose();
            connection.close();
            fail("Expect exception on no login config");
        } catch (JMSSecurityException expected) {
            assertTrue(expected.getMessage().contains(loginConfigScope));
        }
    }

}
