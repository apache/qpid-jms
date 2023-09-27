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
package org.apache.qpid.jms.transactions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for messages produced inside a local transaction.
 */
public class JmsTransactedProducerTest extends AmqpTestSupport {

    @Test
    @Timeout(60)
    public void testCreateTxSessionAndProducer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        assertNotNull(producer);
    }

    @Test
    @Timeout(60)
    public void testTXProducerReusesMessage() throws Exception {
        final int MSG_COUNT = 10;
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Session nonTxSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = nonTxSession.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        TextMessage message = session.createTextMessage();
        for (int i = 0; i < MSG_COUNT; ++i) {
            message.setText("Sequence: " + i);
            producer.send(message);
        }

        Message msg = consumer.receive(1000);
        assertNull(msg);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        session.commit();
        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    @Test
    @Timeout(60)
    public void testTXProducerCommitsAreQueued() throws Exception {
        final int MSG_COUNT = 10;
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Session nonTxSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = nonTxSession.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage());
        }

        Message msg = consumer.receive(2000);
        assertNull(msg);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        session.commit();
        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    @Test
    @Timeout(60)
    public void testTXProducerRollbacksNotQueued() throws Exception {
        final int MSG_COUNT = 10;
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage());
        }

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        session.rollback();
        assertEquals(0, proxy.getQueueSize());
    }
}
