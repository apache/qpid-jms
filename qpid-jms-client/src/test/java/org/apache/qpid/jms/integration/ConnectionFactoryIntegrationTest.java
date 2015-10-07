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

import java.net.URI;
import java.util.UUID;

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.message.JmsMessageIDBuilder.BUILTIN;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactoryIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactoryIntegrationTest.class);

    private final class TestJmsMessageIdBuilder implements JmsMessageIDBuilder {

        @Override
        public Object createMessageID(String producerId, long messageSequence) {
            return UUID.randomUUID();
        }

        @Override
        public String toString() {
            return "TEST";
        }
    }

    @Test(timeout=20000)
    public void testCreateConnectionGoodProviderURI() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqp://127.0.0.1:" + testPeer.getServerPort()));
            Connection connection = factory.createConnection();
            assertNotNull(connection);
            connection.close();
        }
    }

    @Test(timeout=20000)
    public void testCreateConnectionGoodProviderString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            assertNotNull(connection);
            connection.close();
        }
    }

    @Test(timeout=20000)
    public void testUriOptionsAppliedToConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.localMessagePriority=true&jms.forceAsyncSend=true";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            assertTrue(factory.isLocalMessagePriority());
            assertTrue(factory.isForceAsyncSend());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertNotNull(connection);
            assertTrue(connection.isLocalMessagePriority());
            assertTrue(connection.isForceAsyncSend());
            connection.close();
        }
    }

    @Test(timeout=20000)
    public void testSetInvalidMessageIDFormatOption() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=UNKNOWN";
            try {
                new JmsConnectionFactory(uri);
                fail("Should not be able to create a factory with invalid id type option value.");
            } catch (Exception ex) {
                LOG.debug("Caught expected exception on invalid message ID format: {}", ex);
            }
        }
    }

    @Test(timeout=20000)
    public void testSetMessageIDFormatOptionAlteredCase() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            try {
                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=uuid";
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                assertEquals(JmsMessageIDBuilder.BUILTIN.UUID.name(), factory.getMessageIDType());
            } catch (Exception ex) {
                fail("Should have succeeded in creating factory");
            }

            try {
                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=Uuid";
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                assertEquals(JmsMessageIDBuilder.BUILTIN.UUID.name(), factory.getMessageIDType());
            } catch (Exception ex) {
                fail("Should have succeeded in creating factory");
            }
        }
    }

    @Test(timeout=20000)
    public void testMessageIDFormatOptionApplied() throws Exception {
        BUILTIN[] formatters = JmsMessageIDBuilder.BUILTIN.values();

        for (BUILTIN formatter : formatters) {
            LOG.info("Testing application of Message ID Format: {}", formatter.name());
            try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
                // DONT create a test fixture, we will drive everything directly.
                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=" + formatter.name();
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                assertEquals(formatter.name(), factory.getMessageIDType());

                JmsConnection connection = (JmsConnection) factory.createConnection();
                assertEquals(formatter.name(), connection.getMessageIDBuilder().toString());
                connection.close();
            }
        }
    }

    @Test(timeout=20000)
    public void testSetCustomMessageIDBuilder() throws Exception {
        TestJmsMessageIdBuilder custom = new TestJmsMessageIdBuilder();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            factory.setMessageIDBuilder(custom);
            assertEquals(custom.toString(), factory.getMessageIDType());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertEquals(custom.toString(), connection.getMessageIDBuilder().toString());
            connection.close();
        }
    }
}
