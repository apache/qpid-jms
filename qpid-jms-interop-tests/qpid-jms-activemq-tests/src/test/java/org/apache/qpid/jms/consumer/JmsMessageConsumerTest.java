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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.JmsMessageAvailableListener;
import org.apache.qpid.jms.JmsMessageConsumer;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for basic JMS MessageConsumer functionality.
 */
public class JmsMessageConsumerTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumerTest.class);

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Test(timeout = 60000)
    public void testCreateMessageConsumer() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testSyncConsumeFromQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        sendToAmqQueue(1);

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        assertNotNull("Failed to receive any message.", consumer.receive(3000));

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testSyncConsumeFromTopic() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(topic);

        sendToAmqTopic(1);

        final TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(1, proxy.getEnqueueCount());

        assertNotNull("Failed to receive any message.", consumer.receive(3000));

        assertTrue("Published message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testMessageAvailableConsumer() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        final int MSG_COUNT = 10;
        final AtomicInteger available = new AtomicInteger();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        ((JmsMessageConsumer) consumer).setAvailableListener(new JmsMessageAvailableListener() {

            @Override
            public void onMessageAvailable(MessageConsumer consumer) {
                available.incrementAndGet();
            }
        });

        sendToAmqQueue(MSG_COUNT);

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        assertTrue("Listener not notified of correct number of messages.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return available.get() == MSG_COUNT;
            }
        }));

        // All should be immediately ready for consume.
        for (int i = 0; i < MSG_COUNT; ++i) {
            assertNotNull(consumer.receiveNoWait());
        }

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    /**
     * Test to check if consumer thread wakes up inside a receive(timeout) after
     * a message is dispatched to the consumer.  We do a long poll here to ensure
     * that a blocked receive with timeout does eventually get a Message.  We don't
     * want to test the short poll and retry case here since that's not what we are
     * testing.
     *
     * @throws Exception on error found during test run.
     */
    @Test(timeout = 60000)
    public void testConsumerReceiveBeforeMessageDispatched() throws Exception {
        final Connection connection = createAmqpConnection();
        this.connection = connection;
        connection.start();

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createQueue(name.getMethodName());

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    MessageProducer producer = session.createProducer(queue);
                    producer.send(session.createTextMessage("Hello"));
                } catch (Exception e) {
                    LOG.warn("Caught during message send: {}", e.getMessage());
                }
            }
        };
        t.start();
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(60000);
        assertNotNull(msg);
    }

    @Test(timeout = 60000)
    public void testAsynchronousMessageConsumption() throws Exception {
        final int msgCount = 4;
        final Connection connection = createAmqpConnection();
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);
        this.connection = connection;

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message m) {
                LOG.debug("Async consumer got Message: {}", m);
                counter.incrementAndGet();
                if (counter.get() == msgCount) {
                    done.countDown();
                }
            }
        });

        sendToAmqQueue(msgCount);
        assertTrue(done.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(msgCount, counter.get());
    }

    @Test(timeout = 60000)
    public void testMessagesAreAckedAMQProducer() throws Exception {
        int messagesSent = 3;
        assertTrue(brokerService.isPersistent());

        connection = createActiveMQConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer p = session.createProducer(queue);
        TextMessage message = null;
        for (int i=0; i < messagesSent; i++) {
            message = session.createTextMessage();
            String messageText = "Hello " + i + " sent at " + new java.util.Date().toString();
            message.setText(messageText);
            LOG.debug(">>>> Sent [{}]", messageText);
            p.send(message);
        }
        connection.close();

        // After the first restart we should get all messages sent above
        restartPrimaryBroker();
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(messagesSent, proxy.getQueueSize());
        int messagesReceived = readAllMessages();
        assertEquals(messagesSent, messagesReceived);

        // This time there should be no messages on this queue
        restartPrimaryBroker();
        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testMessagesAreAckedAMQPProducer() throws Exception {
        int messagesSent = 3;

        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        TextMessage message = null;
        for (int i=0; i < messagesSent; i++) {
            message = session.createTextMessage();
            String messageText = "Hello " + i + " sent at " + new java.util.Date().toString();
            message.setText(messageText);
            LOG.debug(">>>> Sent [{}]", messageText);
            producer.send(message);
        }

        connection.close();

        // After the first restart we should get all messages sent above
        restartPrimaryBroker();
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(messagesSent, proxy.getQueueSize());
        int messagesReceived = readAllMessages();
        assertEquals(messagesSent, messagesReceived);

        // This time there should be no messages on this queue
        restartPrimaryBroker();
        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
    }

    private int readAllMessages() throws Exception {
        return readAllMessages(null);
    }

    private int readAllMessages(String selector) throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(name.getMethodName());
            int messagesReceived = 0;
            MessageConsumer consumer;

            if (selector == null) {
                consumer = session.createConsumer(queue);
            } else {
                consumer = session.createConsumer(queue, selector);
            }

            Message msg = consumer.receive(2000);
            while (msg != null) {
                assertNotNull(msg);
                assertTrue(msg instanceof TextMessage);
                TextMessage textMessage = (TextMessage) msg;
                LOG.debug(">>>> Received [{}]", textMessage.getText());
                messagesReceived++;
                msg = consumer.receive(2000);
            }

            consumer.close();
            return messagesReceived;
        } finally {
            connection.close();
        }
    }

    @Test(timeout=30000)
    public void testSelectors() throws Exception{
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer p = session.createProducer(queue);

        TextMessage message = session.createTextMessage();
        message.setText("hello");
        p.send(message, DeliveryMode.PERSISTENT, 5, 0);

        message = session.createTextMessage();
        message.setText("hello + 9");
        p.send(message, DeliveryMode.PERSISTENT, 9, 0);

        p.close();

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(2, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
        Message msg = consumer.receive(5000);
        assertNotNull("No message was recieved", msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("hello + 9", ((TextMessage) msg).getText());
        assertNull(consumer.receive(1000));
    }

    @Test(timeout=45000)
    public void testSelectorsWithJMSType() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        TextMessage message = session.createTextMessage();
        message.setText("text");
        producer.send(message, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        TextMessage message2 = session.createTextMessage();
        String type = "myJMSType";
        message2.setJMSType(type);
        message2.setText("text + type");
        producer.send(message2, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        producer.close();

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertTrue("Queue did not get all expected messages", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 2;
            }
        }));

        MessageConsumer consumer = session.createConsumer(queue, "JMSType = '" + type + "'");
        Message msg = consumer.receive(5000);
        assertNotNull("No message was recieved", msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("Unexpected JMSType value", type, msg.getJMSType());
        assertEquals("Unexpected message content", "text + type", ((TextMessage) msg).getText());
    }
}
