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
package org.apache.qpid.jms.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Basic tests for the FailoverProvider implementation
 */
public class JmsFailoverTest extends AmqpTestSupport {

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Test
    @Timeout(60)
    public void testFailoverConnects() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        connection.close();
    }

    @Test
    @Timeout(60)
    public void testFailoverConnectsWithMultipleURIs() throws Exception {
        URI brokerURI = new URI("failover://(amqp://127.0.0.1:5678,amqp://localhost:5777," +
                                getBrokerAmqpConnectionURI() + ")?failover.useReconnectBackOff=false");
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionAsUnknwonUser() throws Exception {
        assertThrows(JMSSecurityException.class, () -> {
            URI brokerURI = new URI(getAmqpFailoverURI() +
                "?failover.maxReconnectAttempts=5");

            JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
            factory.setUsername("unknown");
            factory.setPassword("unknown");

            connection = factory.createConnection();
            assertNotNull(connection);
            connection.start();
            connection.close();
        });
    }

    @Test
    @Timeout(60)
    public void testStartupReconnectAttempts() throws Exception {
        URI brokerURI = new URI("failover://(amqp://localhost:5677)" +
                                "?failover.startupMaxReconnectAttempts=5" +
                                "&failover.useReconnectBackOff=false");
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        Connection connection = factory.createConnection();
        try {
            connection.start();
            fail("Should have thrown an exception of type JMSException");
        } catch (JMSException jmsEx) {
        } catch (Exception unexpected) {
            fail("Should have thrown a JMSException but threw: " + unexpected.getClass().getSimpleName());
        } finally {
            connection.close();
        }
    }

    @Test
    @Timeout(60)
    public void testStartupReconnectAttemptsMultipleHosts() throws Exception {
        URI brokerURI = new URI("failover://(amqp://localhost:5678,amqp://localhost:5677)" +
                                "?failover.startupMaxReconnectAttempts=6" +
                                "&failover.useReconnectBackOff=false");
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        Connection connection = factory.createConnection();
        try {
            connection.start();
            fail("Should have thrown an exception of type JMSException");
        } catch (JMSException jmsEx) {
        } catch (Exception unexpected) {
            fail("Should have thrown a JMSException but threw: " + unexpected.getClass().getSimpleName());
        } finally {
            connection.close();
        }
    }

    @Test
    @Timeout(60)
    public void testStartFailureWithAsyncExceptionListener() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI() +
            "?failover.maxReconnectAttempts=5" +
            "&failover.useReconnectBackOff=false");

        final CountDownLatch failed = new CountDownLatch(1);
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        factory.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Connection got exception: {}", exception.getMessage());
                failed.countDown();
            }
        });
        connection = factory.createConnection();
        connection.start();

        stopPrimaryBroker();

        assertTrue(failed.await(15, TimeUnit.SECONDS), "No async exception");
    }

    @Test
    @Timeout(60)
    public void testBasicStateRestoration() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        session.createProducer(queue);
        session.createConsumer(queue);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueSubscribers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should one new Queue Subscription.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueProducers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should one new Queue Producer.");
    }

    @Test
    @Timeout(60)
    public void testDurableSubscriberRestores() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.setClientID(testMethodName);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(testMethodName);
        MessageConsumer consumer = session.createDurableSubscriber(topic, testMethodName);
        assertNotNull(consumer);

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);

        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have no inactive subscribers.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have one durable sub.");
    }

    @Test
    @Timeout(90)
    public void testBadFirstURIConnectsAndProducerWorks() throws Exception {
        URI brokerURI = new URI("failover://(amqp://localhost:5679," + getBrokerAmqpConnectionURI() + ")");

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 10;
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        final CountDownLatch failed = new CountDownLatch(1);

        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(session.createTextMessage("Message: " + i));
        }

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == MSG_COUNT;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have all messages sent.");

        assertFalse(failed.getCount() == 0);
    }

    @Test
    @Timeout(90)
    public void testNonTxProducerRecoversAfterFailover() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 20;
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        final CountDownLatch failed = new CountDownLatch(1);
        final CountDownLatch sentSome = new CountDownLatch(3);

        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        Thread producerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < MSG_COUNT; ++i) {
                        LOG.debug("Producer sening message #{}", i + 1);
                        producer.send(session.createTextMessage("Message: " + i));
                        sentSome.countDown();
                        if (sentSome.getCount() <= 0) {
                            TimeUnit.MILLISECONDS.sleep(50);
                        }
                    }
                } catch (Exception e) {
                    failed.countDown();
                }
            }
        });
        producerThread.start();

        // Wait until a couple messages get sent on first broker run.
        assertTrue(sentSome.await(6, TimeUnit.SECONDS));
        stopPrimaryBroker();
        TimeUnit.SECONDS.sleep(2);  // Gives FailoverProvider some CPU time
        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueProducers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should have a recovered producer.");

        final QueueViewMBean proxy = getProxyToQueue(testMethodName);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == MSG_COUNT;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should have all messages sent.");

        assertFalse(failed.getCount() == 0);
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testPullConsumerTimedReceiveRecovers() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI() + "?jms.prefetchPolicy.all=0");

        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch received = new CountDownLatch(1);

        connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        final MessageConsumer consumer = session.createConsumer(queue);

        Thread receiver = new Thread(new Runnable() {

            @Override
            public void run() {
                started.countDown();
                try {
                    Message message = consumer.receive(10000);
                    if (message != null) {
                        received.countDown();
                    } else {
                        LOG.info("Consumer did not get a message");
                    }
                } catch (Exception e) {
                }
            }
        });
        receiver.start();

        assertTrue(started.await(10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        TimeUnit.MILLISECONDS.sleep(50);

        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueSubscribers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should one new Queue Subscription.");

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        assertTrue(received.await(30, TimeUnit.SECONDS), "Consumer should have recovered");
    }

    @Test
    @Timeout(30)
    public void testPullConsumerReceiveRecovers() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI() + "?jms.prefetchPolicy.all=0");

        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch received = new CountDownLatch(1);

        connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(testMethodName);
        final MessageConsumer consumer = session.createConsumer(queue);

        Thread receiver = new Thread(new Runnable() {

            @Override
            public void run() {
                started.countDown();
                try {
                    Message message = consumer.receive();
                    if (message != null) {
                        received.countDown();
                    } else {
                        LOG.info("Consumer did not get a message");
                    }
                } catch (Exception e) {
                }
            }
        });
        receiver.start();

        assertTrue(started.await(10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        TimeUnit.MILLISECONDS.sleep(50);

        restartPrimaryBroker();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)), "Should have a new connection.");

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueSubscribers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Should one new Queue Subscription.");

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        assertTrue(received.await(30, TimeUnit.SECONDS), "Consumer should have recovered");
    }
}
