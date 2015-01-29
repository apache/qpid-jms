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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;

public class ConnectionFactoryIntegrationTest extends QpidJmsTestCase {

    @Test(timeout=5000)
    public void testCreateConnectionGoodProviderURI() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqp://127.0.0.1:" + testPeer.getServerPort()));
            Connection connection = factory.createConnection();
            assertNotNull(connection);
            connection.close();
        }
    }

    @Test(timeout=5000)
    public void testCreateConnectionGoodProviderString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            assertNotNull(connection);
            connection.close();
        }
    }

    @Test(timeout=5000)
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
}
