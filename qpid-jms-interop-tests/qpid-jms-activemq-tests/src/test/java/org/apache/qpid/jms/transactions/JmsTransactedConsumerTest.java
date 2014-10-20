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
package org.apache.qpid.jms.transactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test consumer behavior for Transacted Session Consumers.
 */
public class JmsTransactedConsumerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateConsumerFromTxSession() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        assertTrue(session.getTransacted());

        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        assertNotNull(consumer);
    }

    @Test(timeout = 60000)
    public void testConsumedInTxAreAcked() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(1);

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(5000);
        assertNotNull(message);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        session.commit();

        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testReceiveAndRollback() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(2);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(2, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(1000);
        assertNotNull(message);
        session.commit();

        assertEquals(1, proxy.getQueueSize());

        // rollback so we can get that last message again.
        message = consumer.receive(1000);
        assertNotNull(message);
        session.rollback();

        assertEquals(1, proxy.getQueueSize());

        // Consume again.. the prev message should get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        session.commit();

        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testReceiveTwoThenRollback() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(2);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(2, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(1000);
        assertNotNull(message);
        message = consumer.receive(1000);
        assertNotNull(message);
        session.rollback();

        assertEquals(2, proxy.getQueueSize());

        // Consume again.. the prev message should get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        session.commit();

        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testCloseConsumerBeforeCommit() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(2);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(2, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage) consumer.receive(5000);
        assertNotNull(message);
        consumer.close();

        assertEquals(2, proxy.getQueueSize());
        session.commit();
        assertEquals(1, proxy.getQueueSize());

        // Create a new consumer
        consumer = session.createConsumer(queue);
        message = (TextMessage) consumer.receive(1000);
        session.commit();

        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout=60000)
    public void testJMSXDeliveryCount() throws Exception {
        sendToAmqQueue(1);

        connection = createAmqpConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertEquals(true, session.getTransacted());
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        // we receive a message...it should be delivered once and not be Re-delivered.
        Message message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals(false, message.getJMSRedelivered());
        int jmsxDeliveryCount = message.getIntProperty("JMSXDeliveryCount");
        LOG.info("Incoming message has delivery count: {}", jmsxDeliveryCount);
        assertEquals(1, jmsxDeliveryCount);
        session.rollback();

        // we receive again a message
        message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals(true, message.getJMSRedelivered());
        jmsxDeliveryCount = message.getIntProperty("JMSXDeliveryCount");
        LOG.info("Redelivered message has delivery count: {}", jmsxDeliveryCount);
        assertEquals(2, jmsxDeliveryCount);
        session.rollback();

        // we receive again a message
        message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals(true, message.getJMSRedelivered());
        jmsxDeliveryCount = message.getIntProperty("JMSXDeliveryCount");
        LOG.info("Redelivered message has delivery count: {}", jmsxDeliveryCount);
        assertEquals(3, jmsxDeliveryCount);
        session.commit();
    }
}
