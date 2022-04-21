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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import javax.net.ServerSocketFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test connections can be established to remote peers via WebSockets
 */
public class JmsWSConnectionTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsWSConnectionTest.class);

    @Rule
    public TestName testName = new TestName();

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

        TransportConnector connector = brokerService.addConnector(
                "ws://0.0.0.0:" + getProxyPort() + "?websocket.maxBinaryMessageSize=1048576");
        connectionURI = connector.getPublishableConnectURI();
        LOG.debug("Using amqp+ws connection: {}", connectionURI);

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout = 30000)
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }

    @Test(timeout = 30000)
    public void testSendLargeMessageToClientFromOpenWire() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getConnectionURI());
        JmsConnection connection = (JmsConnection) factory.createConnection();

        sendLargeMessageViaOpenWire();

        try {
            Session session = connection.createSession();
            Queue queue = session.createQueue(getQueueName());
            connection.start();

            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(1000);

            assertNotNull(message);
            assertTrue(message instanceof BytesMessage);
        } finally {
            connection.close();
        }
    }

    protected void sendLargeMessageViaOpenWire() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        doSendLargeMessageViaOpenWire(factory.createConnection());
    }

    protected void doSendLargeMessageViaOpenWire(javax.jms.Connection connection) throws Exception {
        try {
            javax.jms.Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            javax.jms.Queue queue = session.createQueue(getQueueName());
            javax.jms.MessageProducer producer = session.createProducer(queue);

            byte[] payload = new byte[1024 * 1024];
            for (int i = 0; i < payload.length; ++i) {
                payload[i] = (byte) (i % 256);
            }
            javax.jms.BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);

            producer.send(message);
        } finally {
            connection.close();
        }
    }

    protected String getQueueName() {
        return testName.getMethodName();
    }

    protected String getConnectionURI() throws Exception {
        return "amqpws://localhost:" + connectionURI.getPort() + "?transport.traceBytes=true";
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
