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

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.QueueConnection;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test basic QueueConnection creation etc.
 */
public class JmsQueueConnectionTest extends AmqpTestSupport {

    @Test
    public void testCreateQueueConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createQueueConnection();
        assertNotNull(connection);
        assertTrue(connection instanceof QueueConnection);
    }

    @Test(timeout=30000)
    public void testCreateConnectionAsSystemAdmin() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        factory.setUsername("system");
        factory.setPassword("manager");
        connection = factory.createQueueConnection();
        assertNotNull(connection);
        connection.start();
    }

    @Test(timeout=30000, expected = JMSSecurityException.class)
    public void testCreateConnectionAsUnknwonUser() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        factory.setUsername("unknown");
        factory.setPassword("unknown");
        connection = factory.createQueueConnection();
        assertNotNull(connection);
        connection.start();
    }

    @Test(timeout=30000)
    public void testCreateConnectionCallSystemAdmin() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createQueueConnection("system", "manager");
        assertNotNull(connection);
        connection.start();
    }

    @Test(timeout=30000, expected = JMSSecurityException.class)
    public void testCreateConnectionCallUnknwonUser() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createQueueConnection("unknown", "unknown");
        assertNotNull(connection);
        connection.start();
    }
}
