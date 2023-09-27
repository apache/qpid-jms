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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.QpidJmsTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that Session CLIENT_ACKNOWLEDGE works as expected.
 */
public class JmsClientAckTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsClientAckTest.class);

    @Test
    @Timeout(60)
    public void testAckedMessageAreConsumed() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(3000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");

        connection.close();
    }

    @Test
    @Timeout(60)
    public void testLastMessageAcked() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));
        producer.send(session.createTextMessage("Hello2"));
        producer.send(session.createTextMessage("Hello3"));

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(3, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(3000);
        assertNotNull(msg);
        msg = consumer.receive(3000);
        assertNotNull(msg);
        msg = consumer.receive(3000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(60)
    public void testUnAckedMessageAreNotConsumedOnSessionClose() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...but don't ack it.
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(3000);
        assertNotNull(msg);
        session.close();

        assertEquals(1, proxy.getQueueSize());

        // Consume the message...and this time we ack it.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        msg = consumer.receive(3000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(60)
    public void testAckedMessageAreConsumedByAsync() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
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

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(60)
    public void testUnAckedAsyncMessageAreNotConsumedOnSessionClose() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        final CountDownLatch consumed = new CountDownLatch(1);
        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                consumed.countDown();
            }
        });

        assertTrue(consumed.await(10, TimeUnit.SECONDS), "Should have read one message");
        session.close();
        assertEquals(1, proxy.getQueueSize());

        // Now we consume and ack the Message.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(queue);
        Message msg = consumer.receive(3000);
        assertNotNull(msg);
        msg.acknowledge();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(90)
    public void testAckMarksAllConsumerMessageAsConsumed() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);

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

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        assertTrue(done.await(20, TimeUnit.SECONDS), "Failed to consume all messages.");
        assertNotNull(lastMessage.get());
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getInFlightCount() == MSG_COUNT;
            }
        }), "Not all messages appear as in-flight.");

        lastMessage.get().acknowledge();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(60)
    public void testOnlyUnackedAreRecovered() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = consumerSession.createQueue(testMethodName);
        MessageConsumer consumer = consumerSession.createConsumer(queue);
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        TextMessage sent1 = producerSession.createTextMessage();
        sent1.setText("msg1");
        producer.send(sent1);
        TextMessage sent2 = producerSession.createTextMessage();
        sent2.setText("msg2");
        producer.send(sent2);
        TextMessage sent3 = producerSession.createTextMessage();
        sent3.setText("msg3");
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

    @Test
    @Timeout(60)
    public void testReceiveSomeThenRecover() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        int totalCount = 5;
        int consumeBeforeRecover = 2;

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);

        sendMessages(connection, queue, totalCount);

        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(totalCount, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);

        for(int i = 1; i <= consumeBeforeRecover; i++) {
            Message message = consumer.receive(3000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty(QpidJmsTestSupport.MESSAGE_NUMBER), "Unexpected message number");
        }

        session.recover();

        assertEquals(totalCount, proxy.getQueueSize());

        // Consume again.. the previously consumed messages should get delivered
        // again after the recover and then the remainder should follow
        List<Integer> messageNumbers = new ArrayList<Integer>();
        for (int i = 1; i <= totalCount; i++) {
            Message message = consumer.receive(3000);
            assertNotNull(message, "Failed to receive message: " + i);
            int msgNum = message.getIntProperty(QpidJmsTestSupport.MESSAGE_NUMBER);
            messageNumbers.add(msgNum);

            if(i == totalCount) {
                message.acknowledge();
            }
        }

        assertEquals(totalCount, messageNumbers.size(), "Unexpected size of list");
        for (int i = 0; i < messageNumbers.size(); i++) {
            assertEquals(Integer.valueOf(i + 1), messageNumbers.get(i), "Unexpected order of messages: " + messageNumbers);
        }
    }

    @Test
    @Timeout(60)
    public void testRecoverRedelivery() throws Exception {
        final CountDownLatch redelivery = new CountDownLatch(6);
        connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
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
                    if (redelivery.getCount() != 0) {
                        session.recover();
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("test"));

        assertTrue(redelivery.await(20, TimeUnit.SECONDS), "we got 6 redeliveries");
    }

    // TODO - Enable once broker prefetch is fully worked out.
    @Disabled("Fails until Broker get it's prefetch issues resolved.")
    @Test
    @Timeout(60)
    public void testConsumeBeyondInitialPrefetch() throws Exception {
        final int MESSAGE_COUNT = 2000;

        final CountDownLatch consumed = new CountDownLatch(MESSAGE_COUNT);
        connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    LOG.debug("Got message: " + message.getJMSMessageID());
                    consumed.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection.start();

        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            producer.send(session.createTextMessage("test: message[" + (i + 1) + "]"));
        }

        assertTrue(consumed.await(45, TimeUnit.SECONDS), "Failed to get all deliveries");
    }

    /**
     * Test use of session recovery while using a client-ack session and
     * a message listener. Calling recover should result in delivery of the
     * previous messages again, followed by those that would have been received
     * afterwards.
     *
     * Send three messages. Consume the first message, then recover on the second
     * message and expect to see both again, ensure the third message is not seen
     * until after this.
     *
     * @throws Exception on error during test
     */
    @Test
    @Timeout(60)
    public void testRecoverInOnMessage() throws Exception {
        connection = createAmqpConnection();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, 3);

        CountDownLatch latch = new CountDownLatch(1);
        ClientAckRecoverMsgListener listener = new ClientAckRecoverMsgListener(latch, session);
        consumer.setMessageListener(listener);

        connection.start();

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for async listener");
        assertFalse(listener.getFailed(), "Test failed in listener, consult logs");
    }

    @Test
    @Timeout(60)
    public void testUnAckedAsyncMessagesGetRedeliveredMultipleTimes() throws Exception {
        final int MESSAGE_COUNT = 30;
        final int ITERATIONS = 20;

        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        session.close();

        sendMessages(connection, queue, MESSAGE_COUNT);

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == MESSAGE_COUNT;
            }
        }), "Queue didn't receive all messages");

        for (int i = 0; i < ITERATIONS; ++i) {
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Consume the message...
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    // Don't ack the message.
                }
            });

            session.close();
        }

        assertEquals(MESSAGE_COUNT, proxy.getQueueSize());

        // Now we consume and ack the Message.
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < MESSAGE_COUNT; ++i) {
            Message msg = consumer.receive(3000);
            assertNotNull(msg);
            msg.acknowledge();
        }

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    @Test
    @Timeout(60)
    public void testRepeatedRecoveriesInAsyncListener() throws Exception {
        final int MESSAGE_COUNT = 20;
        final int ITERATIONS = 10;

        final AtomicInteger messagesConsumed = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<Exception>();

        connection = createAmqpConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);

        sendMessages(connection, queue, MESSAGE_COUNT);

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == MESSAGE_COUNT;
            }
        }), "Queue didn't receive all messages");

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            int retries = 0;

            @Override
            public void onMessage(Message message) {
                try {
                    LOG.info("Read message {}", message.getIntProperty(MESSAGE_NUMBER));
                    if (message.getIntProperty(MESSAGE_NUMBER) != messagesConsumed.get() + 1) {
                        failure.set(new IllegalArgumentException("Read message with wrong sequence"));
                    }

                    if (++retries == ITERATIONS) {
                        messagesConsumed.incrementAndGet();
                        retries = 0;
                        message.acknowledge();

                        // Check that only one message is consumed
                        boolean consumed = Wait.waitFor(new Wait.Condition() {

                            @Override
                            public boolean isSatisfied() throws Exception {
                                return proxy.getQueueSize() == MESSAGE_COUNT - messagesConsumed.get();
                            }
                        }, 10000, 20);

                        if (!consumed) {
                            failure.set(new IllegalStateException("Broker Queue Size doesn't match expectations"));
                        }
                    } else {
                        session.recover();
                    }

                } catch (Exception e) {
                    failure.set(e);
                }
            }
        });

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                LOG.info("Are we complete: error:{} nessages read:{}", failure.get(), messagesConsumed.get());
                return failure.get() != null || messagesConsumed.get() == MESSAGE_COUNT;
            }
        }), "Not all messages could be consumed, got " + messagesConsumed.get());

        assertNull(failure.get(), "Should not get any failures during this test");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }), "Queued message not consumed.");
    }

    private static class ClientAckRecoverMsgListener implements MessageListener {
        final Session session;
        final CountDownLatch latch;
        private boolean seenFirstMessage = false;
        private boolean seenFirstMessageTwice = false;
        private boolean seenSecondMessage = false;
        private boolean seenSecondMessageTwice = false;
        private boolean complete = false;
        private boolean failed = false;

        public ClientAckRecoverMsgListener(CountDownLatch latch, Session session) {
            this.latch = latch;
            this.session = session;
        }

        @Override
        public void onMessage(Message message) {
            try {
                int msgNumProperty = message.getIntProperty(MESSAGE_NUMBER);

                if(complete ){
                    LOG.info("Test already finished, ignoring delivered message: " + msgNumProperty);
                    return;
                }

                if (msgNumProperty == 1) {
                    if (!seenFirstMessage) {
                        LOG.info("Received first message.");
                        seenFirstMessage = true;
                    } else {
                        LOG.info("Received first message again.");
                        if(message.getJMSRedelivered()) {
                            LOG.info("Message was marked redelivered as expected.");
                        } else {
                            LOG.error("Message was not marked redelivered.");
                            complete(true);
                        }
                        seenFirstMessageTwice = true;
                    }
                } else if (msgNumProperty == 2) {
                    if(!seenSecondMessage){
                        seenSecondMessage = true;
                        LOG.info("Received second message. Now calling recover()");
                        session.recover();
                    } else {
                        if (!seenFirstMessageTwice) {
                            LOG.error("Received second message again before seeing first message again.");
                            complete(true);
                            return;
                        }

                        LOG.info("Received second message again as expected.");
                        seenSecondMessageTwice = true;

                        if(message.getJMSRedelivered()) {
                            LOG.info("Message was marked redelivered as expected.");
                        } else {
                            LOG.error("Message was not marked redelivered.");
                            complete(true);
                        }
                    }
                } else {
                    if (msgNumProperty != 3) {
                        LOG.error("Received unexpected message: " + msgNumProperty);
                        complete(true);
                        return;
                    }

                    if (!(seenFirstMessageTwice && seenSecondMessageTwice)) {
                        LOG.error("Third message was not received in expected sequence.");
                        complete(true);
                        return;
                    }

                    if(message.getJMSRedelivered()) {
                        LOG.error("Message was marked redelivered against expectation.");
                        complete(true);
                    } else {
                        LOG.info("Message was not marked redelivered, as expected.");
                        complete(false);
                    }
                }
            } catch (JMSException e) {
                LOG.error("Exception caught in listener", e);
                complete(true);
            }
        }

        public boolean getFailed() {
            return failed;
        }

        private void complete(boolean fail) {
            failed = fail;
            complete = true;
            latch.countDown();
        }
    }
}
