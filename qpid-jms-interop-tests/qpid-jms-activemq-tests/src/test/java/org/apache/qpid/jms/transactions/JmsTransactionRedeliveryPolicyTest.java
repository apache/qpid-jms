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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * test redelivery policy application in a TX session.
 */
public class JmsTransactionRedeliveryPolicyTest extends AmqpTestSupport {

    @Override
    public String getAmqpConnectionURIOptions() {
        return "jms.redeliveryPolicy.maxRedeliveries=5";
    }

    @Test
    @Timeout(30)
    public void testSyncConsumeAndRollbackWithMaxRedeliveries() throws Exception {
        final int MAX_REDELIVERIES = 5;
        final int MSG_COUNT = 5;

        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        sendMessages(connection, queue, MSG_COUNT);

        final QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        // Consume the message for the first time.
        Message incoming = null;
        for (int i = 0; i < MSG_COUNT; ++i) {
            incoming = consumer.receive(3000);
            assertNotNull(incoming);
            assertFalse(incoming.getJMSRedelivered());
            assertTrue(incoming instanceof TextMessage);
        }
        session.rollback();

        for (int i = 0; i < MAX_REDELIVERIES; ++i) {
            LOG.info("Queue size before consume is: {}", queueView.getQueueSize());
            assertEquals(MSG_COUNT, queueView.getQueueSize());

            for (int j = 0; j < MSG_COUNT; ++j) {
                incoming = consumer.receive(3000);
                assertNotNull(incoming);
                assertTrue(incoming.getJMSRedelivered());
                assertTrue(incoming instanceof TextMessage);
            }

            assertEquals(MSG_COUNT, queueView.getQueueSize());

            session.rollback();
            LOG.info("Queue size after session rollback is: {}", queueView.getQueueSize());
        }

        assertNull(consumer.receive(50));

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return queueView.getQueueSize() == 0;
            }
        }), "Message should get DLQ'd");

        QueueViewMBean dlq = getProxyToQueue("ActiveMQ.DLQ");
        assertEquals(MSG_COUNT, dlq.getQueueSize());

        session.commit();
    }

    @Test
    @Timeout(30)
    public void testAsyncConsumeAndRollbackWithMaxRedeliveries() throws Exception {
        final int MAX_REDELIVERIES = 5;
        final int MSG_COUNT = 5;

        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(getDestinationName());
        MessageConsumer consumer = session.createConsumer(queue);
        sendMessages(connection, queue, MSG_COUNT);

        final QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        // Consume the message for the first time.
        Message incoming = null;
        for (int i = 0; i < MSG_COUNT; ++i) {
            incoming = consumer.receive(3000);
            assertNotNull(incoming);
            assertFalse(incoming.getJMSRedelivered());
            assertTrue(incoming instanceof TextMessage);
        }
        session.rollback();

        for (int i = 0; i < MAX_REDELIVERIES; ++i) {
            LOG.info("Queue size before consume is: {}", queueView.getQueueSize());
            assertEquals(MSG_COUNT, queueView.getQueueSize());

            final CountDownLatch done = new CountDownLatch(MSG_COUNT);
            consumer.setMessageListener(new MaxRedeliveryListener(done, i));

            assertTrue(done.await(10, TimeUnit.SECONDS), "Not All Messages Received");
            assertEquals(MSG_COUNT, queueView.getQueueSize());

            consumer.setMessageListener(null);
            session.rollback();
            LOG.info("Queue size after session rollback is: {}", queueView.getQueueSize());
        }

        assertNull(consumer.receive(50));

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return queueView.getQueueSize() == 0;
            }
        }), "Message should get DLQ'd");

        QueueViewMBean dlq = getProxyToQueue("ActiveMQ.DLQ");
        assertEquals(MSG_COUNT, dlq.getQueueSize());

        session.commit();
    }

    private static class MaxRedeliveryListener implements MessageListener {
        private final CountDownLatch done;
        private final int listenerNumber;

        private MaxRedeliveryListener(CountDownLatch done, int listenerNumber) {
            this.done = done;
            this.listenerNumber = listenerNumber;
        }

        @Override
        public void onMessage(Message message) {
            try {
                assertTrue(message.getJMSRedelivered());
                assertTrue(message instanceof TextMessage);

                LOG.debug("Listener {} received message: {}", listenerNumber, message.getIntProperty(AmqpTestSupport.MESSAGE_NUMBER));

                done.countDown();
            } catch (JMSException e) {
                LOG.error("Caught exception in listener {}", listenerNumber, e);
            }
        }
    }

}
