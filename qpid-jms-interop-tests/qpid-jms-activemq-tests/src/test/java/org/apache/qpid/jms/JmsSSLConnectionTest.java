/**
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
package org.apache.qpid.jms;

import static org.junit.Assert.assertNotNull;

import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that we can connect to a broker over SSL.
 */
public class JmsSSLConnectionTest {

    private BrokerService brokerService;

    public static final String PASSWORD = "password";
    public static final String KEYSTORE = "src/test/resources/keystore";
    public static final String KEYSTORE_TYPE = "jks";

    private URI connectionURI;

    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseJmx(true);

        TransportConnector connector = brokerService.addConnector("amqp+ssl://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = connector.getPublishableConnectURI();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public String getConnectionURI() throws Exception {
        return "amqps://" + connectionURI.getHost() + ":" + connectionURI.getPort();
    }

    @Test(timeout=30000)
    public void testCreateConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test(timeout=30000)
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }
}
