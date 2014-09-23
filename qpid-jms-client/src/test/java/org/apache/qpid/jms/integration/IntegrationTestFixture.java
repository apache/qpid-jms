/*
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
 */
package org.apache.qpid.jms.integration;

import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;

public class IntegrationTestFixture {
    static final int PORT = 25672;

    Connection establishConnecton(TestAmqpPeer testPeer) throws JMSException {
        testPeer.expectPlainConnect("guest", "guest", true);

        // Each connection creates a session for managing temporary destinations etc
        testPeer.expectBegin(true);

        ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + PORT);
        Connection connection = factory.createConnection("guest", "guest");

        // Set a clientId to provoke the actual AMQP connection process to occur.
        connection.setClientID("clientName");

        assertNull(testPeer.getThrowable());
        return connection;
    }
}
