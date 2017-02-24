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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionListener;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.QpidJmsTestSupport;
import org.junit.Test;

/**
 * Test consumer behavior for Transacted Session Consumers.
 */
public class JmsTransactedConsumerTest extends AmqpTestSupport {

    private final String MSG_NUM = "MSG_NUM";
    private final int MSG_COUNT = 1000;

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

    @Test(timeout=30000)
    public void testRollbackRececeivedMessageAndClose() throws Exception {

        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("TestMessage-0"));
        producer.close();
        session.commit();

        MessageConsumer consumer = session.createConsumer(queue);

        Message msg = consumer.receive(3000);
        assertNotNull(msg);

        session.rollback();

        connection.close();
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

        Message message = consumer.receive(3000);
        assertNotNull(message);
        session.commit();

        assertEquals(1, proxy.getQueueSize());

        // rollback so we can get that last message again.
        message = consumer.receive(3000);
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

        Message message = consumer.receive(3000);
        assertNotNull(message);
        message = consumer.receive(3000);
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
    public void testReceiveTwoThenCloseSessionToRollback() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        sendToAmqQueue(2);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(2, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive(3000);
        assertNotNull(message);
        message = consumer.receive(3000);
        assertNotNull(message);
        session.rollback();

        assertEquals(2, proxy.getQueueSize());

        // Consume again.. the prev message should get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the message again!", message);

        session.close();

        assertEquals(2, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testReceiveSomeThenRollback() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        int totalCount = 5;
        int consumeBeforeRollback = 2;
        sendToAmqQueue(totalCount);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(totalCount, proxy.getQueueSize());

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        for(int i = 1; i <= consumeBeforeRollback; i++) {
            Message message = consumer.receive(3000);
            assertNotNull(message);
            assertEquals("Unexpected message number", i, message.getIntProperty(QpidJmsTestSupport.MESSAGE_NUMBER));
        }

        session.rollback();

        assertEquals(totalCount, proxy.getQueueSize());

        // Consume again.. the previously consumed messages should get delivered
        // again after the rollback and then the remainder should follow
        List<Integer> messageNumbers = new ArrayList<Integer>();
        for(int i = 1; i <= totalCount; i++) {
            Message message = consumer.receive(3000);
            assertNotNull("Failed to receive message: " + i, message);
            int msgNum = message.getIntProperty(QpidJmsTestSupport.MESSAGE_NUMBER);
            messageNumbers.add(msgNum);
        }

        session.commit();

        assertEquals("Unexpected size of list", totalCount, messageNumbers.size());
        for(int i = 0; i < messageNumbers.size(); i++)
        {
            assertEquals("Unexpected order of messages: " + messageNumbers, Integer.valueOf(i + 1), messageNumbers.get(i));
        }

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
        message = (TextMessage) consumer.receive(5000);
        assertNotNull(message);
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

    @Test(timeout=30000)
    public void testSessionTransactedCommitWithLocalPriorityReordering() throws Exception {
        connection = createAmqpConnection();
        ((JmsConnection) connection).setLocalMessagePriority(true);
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        connection.start();

        final CountDownLatch messagesArrived = new CountDownLatch(4);
        ((JmsConnection) connection).addConnectionListener(new JmsConnectionListener() {

            @Override
            public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                messagesArrived.countDown();
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }

            @Override
            public void onSessionClosed(Session session, Throwable exception) {
            }

            @Override
            public void onConsumerClosed(MessageConsumer consumer, Throwable cause) {
            }

            @Override
            public void onProducerClosed(MessageProducer producer, Throwable cause) {
            }

            @Override
            public void onRemoteDiscovery(List<URI> remotes) {
            }
        });

        MessageProducer pr = session.createProducer(queue);
        for (int i = 1; i <= 2; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            m.setIntProperty(MSG_NUM, i);
            pr.send(m, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        }
        session.commit();

        // Receive first message.
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(5000);
        assertNotNull(msg);
        assertEquals(1, msg.getIntProperty(MSG_NUM));
        assertEquals(Message.DEFAULT_PRIORITY, msg.getJMSPriority());

        // Send a couple higher priority, expect them to 'overtake' upon arrival at the consumer.
        for (int i = 3; i <= 4; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            m.setIntProperty(MSG_NUM, i);
            pr.send(m, DeliveryMode.NON_PERSISTENT, 5 , Message.DEFAULT_TIME_TO_LIVE);
        }
        session.commit();

        // Wait for them all to arrive at the consumer
        assertTrue("Messages didnt all arrive in given time.", messagesArrived.await(5, TimeUnit.SECONDS));

        // Receive the other messages. Expect higher priority messages first.
        msg = consumer.receive(3000);
        assertNotNull(msg);
        assertEquals(3, msg.getIntProperty(MSG_NUM));
        assertEquals(5, msg.getJMSPriority());

        msg = consumer.receive(3000);
        assertNotNull(msg);
        assertEquals(4, msg.getIntProperty(MSG_NUM));
        assertEquals(5, msg.getJMSPriority());

        msg = consumer.receive(3000);
        assertNotNull(msg);
        assertEquals(2, msg.getIntProperty(MSG_NUM));
        assertEquals(Message.DEFAULT_PRIORITY, msg.getJMSPriority());

        session.commit();

        // Send a couple messages to check the session still works.
        for (int i = 5; i <= 6; i++) {
            Message m = session.createTextMessage("TestMessage" + i);
            m.setIntProperty(MSG_NUM, i);
            pr.send(m, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY , Message.DEFAULT_TIME_TO_LIVE);
        }
        session.commit();

        session.close();
    }

    @Test(timeout = 60000)
    public void testSingleConsumedMessagePerTxCase() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(getTestName());
        MessageProducer messageProducer = session.createProducer(queue);
        for (int i = 0; i < MSG_COUNT; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("test" + i);
            messageProducer.send(message, DeliveryMode.PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY, javax.jms.Message.DEFAULT_TIME_TO_LIVE);
        }

        session.close();

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        int counter = 0;
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = session.createConsumer(queue);
        do {
            TextMessage message = (TextMessage) messageConsumer.receive(1000);
            if (message != null) {
                counter++;
                LOG.info("Message n. {} with content '{}' has been recieved.", counter,message.getText());
                session.commit();
                LOG.info("Transaction has been committed.");
                assertEquals(MSG_COUNT - counter, queueView.getQueueSize());
            }
        } while (counter < MSG_COUNT);

        assertEquals(0, queueView.getQueueSize());

        session.close();
    }

    @Test(timeout = 60000)
    public void testConsumeAllMessagesInSingleTxCase() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(getTestName());
        MessageProducer messageProducer = session.createProducer(queue);
        for (int i = 0; i < MSG_COUNT; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("test" + i);
            messageProducer.send(message, DeliveryMode.PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY, javax.jms.Message.DEFAULT_TIME_TO_LIVE);
        }

        session.close();

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        int counter = 0;
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = session.createConsumer(queue);
        do {
            TextMessage message = (TextMessage) messageConsumer.receive(1000);
            if (message != null) {
                counter++;
                LOG.info("Message n. {} with content '{}' has been recieved.", counter,message.getText());
            }
        } while (counter < MSG_COUNT);

        LOG.info("Transaction has been committed.");
        session.commit();

        assertEquals(0, queueView.getQueueSize());

        session.close();
    }

    @Test(timeout = 60000)
    public void testConsumerClosesAfterItsTXCommits() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session mgmtSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = mgmtSession.createQueue(name.getMethodName());

        // Send a message that will be rolled back.
        Session senderSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = senderSession.createProducer(queue);
        producer.send(senderSession.createMessage());
        senderSession.commit();
        senderSession.close();

        // Consumer the message in a transaction and then roll it back
        Session txSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = txSession.createConsumer(queue);
        Message received = consumer.receive(1000);
        assertNotNull("Consumer didn't receive the message", received);
        txSession.rollback();
        consumer.close();

        // Create an auto acknowledge session and consumer normally.
        Session nonTxSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = nonTxSession.createConsumer(queue);
        received = consumer.receive(1000);
        assertNotNull("receiver3 didn't received the message", received);
        consumer.close();

        connection.close();
    }

    @Test(timeout = 90000)
    public void testConsumerMessagesInOrder() throws Exception {

        final int ITERATIONS = 5;
        final int MESSAGE_COUNT = 4;

        for (int i = 0; i < ITERATIONS; i++) {
            LOG.debug("##----------- Iteration {} -----------##", i);
            Connection consumingConnection = createAmqpConnection();
            Connection producingConnection = createAmqpConnection();

            Session consumerSession = consumingConnection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = consumerSession.createQueue("my_queue");
            MessageConsumer consumer = consumerSession.createConsumer(queue);

            Session producerSession = producingConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = producerSession.createProducer(queue);

            ArrayList<TextMessage> received = new ArrayList<>();

            try {
                for (int j = 0; j < MESSAGE_COUNT; j++) {
                    producer.send(producerSession.createTextMessage("msg" + (j + 1)));
                }

                producerSession.commit();

                consumingConnection.start();

                for (int k = 0; k < MESSAGE_COUNT; k++) {
                    TextMessage tm = (TextMessage) consumer.receive(5000);
                    assertNotNull(tm);
                    received.add(tm);
                }

                for (int l = 0; l < MESSAGE_COUNT; l++) {
                    TextMessage tm = received.get(l);
                    assertNotNull(tm);
                    if (!("msg" + (l + 1)).equals(tm.getText())) {
                        fail("Out of order, expected " + ("msg" + (l + 1)) + " but got: " + tm.getText());
                    }
                }
                consumerSession.commit();
            } finally {
                consumingConnection.close();
                producingConnection.close();
            }
        }
    }
}
