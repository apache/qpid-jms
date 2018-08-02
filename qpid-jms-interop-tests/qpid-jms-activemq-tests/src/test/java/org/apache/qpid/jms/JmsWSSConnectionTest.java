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
package org.apache.qpid.jms;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test connections can be established to remote peers via secure WebSockets
 */
public class JmsWSSConnectionTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsWSSConnectionTest.class);

    private static final String PASSWORD = "password";
    private static final String KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String TRUSTSTORE = "src/test/resources/client-jks.truststore";

    private BrokerService brokerService;
    private URI connectionURI;
    private final int DEFAULT_WS_PORT = 5679;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseJmx(false);

        // Setup broker SSL context...
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext sslContext = TransportSupport.createJdkSslContext(sslOptions);

        final SslContext brokerContext = new SslContext();
        brokerContext.setSSLContext(sslContext);

        brokerService.setSslContext(brokerContext);

        TransportConnector connector = brokerService.addConnector("wss://0.0.0.0:" + getProxyPort());
        connectionURI = connector.getPublishableConnectURI();
        LOG.debug("Using amqp+wss connection: {}", connectionURI);

        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = connector.getPublishableConnectURI();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout=30000)
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getConnectionURI(true));
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }

    protected String getConnectionURI(boolean verifyHost) throws Exception {
        String baseURI = "amqpwss://localhost:" + connectionURI.getPort() +
                "?transport.trustStoreLocation=" + TRUSTSTORE +
                "&transport.trustStorePassword=" + PASSWORD;
        if (verifyHost) {
            return baseURI;
        } else {
            return baseURI + "&transport.verifyHost=false";
        }
    }

    protected int getProxyPort() {
        int proxyPort = DEFAULT_WS_PORT;

        try (ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0)) {
            proxyPort = ss.getLocalPort();
        } catch (IOException e) {
        }

        return proxyPort;
    }
}
