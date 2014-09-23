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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that Session CLIENT_ACKNOWLEDGE works as expected.
 */
public class JmsClientAckTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsClientAckTest.class);

    private Connection connection;

    @Override
    @After
    public void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testAckedMessageAreConsumed() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLastMessageAcked() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));
        producer.send(session.createTextMessage("Hello2"));
        producer.send(session.createTextMessage("Hello3"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(3, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testUnAckedMessageAreNotConsumedOnSessionClose() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...but don't ack it.
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        session.close();

        assertEquals(1, proxy.getQueueSize());

        // Consume the message...and this time we ack it.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testAckedMessageAreConsumedByAsync() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                    LOG.warn("Unexpected exception on acknowledge: {}", e.getMessage());
                }
            }
        });

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testUnAckedAsyncMessageAreNotConsumedOnSessionClose() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                // Don't ack the message.
            }
        });

        session.close();
        assertEquals(1, proxy.getQueueSize());

        // Now we consume and ack the Message.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout=90000)
    public void testAckMarksAllConsumerMessageAsConsumed() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());

        final int MSG_COUNT = 30;
        final AtomicReference<Message> lastMessage = new AtomicReference<Message>();
        final CountDownLatch done = new CountDownLatch(MSG_COUNT);

        MessageListener myListener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
                lastMessage.set(message);
                done.countDown();
            }
        };

        MessageConsumer consumer1 = session.createConsumer(queue);
        consumer1.setMessageListener(myListener);
        MessageConsumer consumer2 = session.createConsumer(queue);
        consumer2.setMessageListener(myListener);
        MessageConsumer consumer3 = session.createConsumer(queue);
        consumer3.setMessageListener(myListener);

        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage("Hello: " + i));
        }

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        assertTrue("Failed to consume all messages.", done.await(20, TimeUnit.SECONDS));
        assertNotNull(lastMessage.get());
        assertEquals(MSG_COUNT, proxy.getInFlightCount());

        lastMessage.get().acknowledge();

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout=60000)
    public void testUnackedAreRecovered() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = consumerSession.createQueue(name.getMethodName());
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        TextMessage sent1 = producerSession.createTextMessage();
        sent1.setText("msg1");
        producer.send(sent1);
        TextMessage sent2 = producerSession.createTextMessage();
        sent1.setText("msg2");
        producer.send(sent2);
        TextMessage sent3 = producerSession.createTextMessage();
        sent1.setText("msg3");
        producer.send(sent3);

        consumer.receive(5000);
        Message rec2 = consumer.receive(5000);
        consumer.receive(5000);
        rec2.acknowledge();

        TextMessage sent4 = producerSession.createTextMessage();
        sent4.setText("msg4");
        producer.send(sent4);

        Message rec4 = consumer.receive(5000);
        assertNotNull(rec4);
        assertTrue(rec4.equals(sent4));
        consumerSession.recover();
        rec4 = consumer.receive(5000);
        assertNotNull(rec4);
        assertTrue(rec4.equals(sent4));
        assertTrue(rec4.getJMSRedelivered());
        rec4.acknowledge();
    }

    @Test(timeout=60000)
    public void testRecoverRedelivery() throws Exception {
        final CountDownLatch redelivery = new CountDownLatch(6);
        connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    LOG.info("Got message: " + message.getJMSMessageID());
                    if (message.getJMSRedelivered()) {
                        LOG.info("It's a redelivery.");
                        redelivery.countDown();
                    }
                    LOG.info("calling recover() on the session to force redelivery.");
                    session.recover();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("test"));

        assertTrue("we got 6 redeliveries", redelivery.await(20, TimeUnit.SECONDS));
    }
}
