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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for basic JmsConnection functionality and error handling.
 */
public class JmsConnectionTest extends AmqpTestSupport {

    @Test
    @Timeout(30)
    public void testCreateConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionAndStart() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateWithDuplicateClientIdFails() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        JmsConnection connection1 = (JmsConnection) factory.createConnection();
        connection1.setClientID("Test");
        assertNotNull(connection1);
        connection1.start();
        JmsConnection connection2 = (JmsConnection) factory.createConnection();
        try {
            connection2.setClientID("Test");
            fail("should have thrown a JMSException");
        } catch (InvalidClientIDException ex) {
            LOG.info("Remote threw ex: {}", ex);
        } catch (Exception unexpected) {
            fail("Wrong exception type thrown: " + unexpected);
        }

        connection1.close();
        connection2.close();
    }

    @Test
    public void testSetClientIdAfterStartedFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
            connection = factory.createConnection();
            connection.setClientID("Test");
            connection.start();
            connection.setClientID("NewTest");
            connection.close();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionAsSystemAdmin() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        factory.setUsername("system");
        factory.setPassword("manager");
        connection = factory.createConnection();
        assertNotNull(connection);
        connection.start();
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionCallSystemAdmin() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createConnection("system", "manager");
        assertNotNull(connection);
        connection.start();
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionAsUnknwonUser() throws Exception {
        assertThrows(JMSSecurityException.class, () -> {
            JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
            factory.setUsername("unknown");
            factory.setPassword("unknown");
            connection = factory.createConnection();
            assertNotNull(connection);
            connection.start();
            connection.close();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionCallUnknwonUser() throws Exception {
        assertThrows(JMSSecurityException.class, () -> {
            JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
            connection = factory.createConnection("unknown", "unknown");
            assertNotNull(connection);
            connection.start();
        });
    }

    @Test
    @Timeout(30)
    public void testBrokerStopWontHangConnectionClose() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Message m = session.createTextMessage("Sample text");
        producer.send(m);

        stopPrimaryBroker();

        try {
            connection.close();
        } catch (Exception ex) {
            LOG.error("Should not thrown on disconnected connection close(): {}", ex);
            fail("Should not have thrown an exception.");
        }
    }

    @Test
    @Timeout(60)
    public void testConnectionExceptionBrokerStop() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        stopPrimaryBroker();

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        connection.close();
    }
}
