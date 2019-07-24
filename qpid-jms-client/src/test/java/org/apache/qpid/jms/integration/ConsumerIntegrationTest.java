/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.AmqpPeerRunnable;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.RejectedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(QpidJMSTestRunner.class)
public class ConsumerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCloseConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseConsumerTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            testPeer.expectBegin();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectDetach(true, false, true);
            testPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            try {
                consumer.close();
                fail("Should have thrown a timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught excpected exception", jmsEx);
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            CountDownLatch consumerClosed = new CountDownLatch(1);
            CountDownLatch exceptionFired = new CountDownLatch(1);

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    exceptionFired.countDown();
                }
            });
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConsumerClosed(MessageConsumer consumer, Throwable exception) {
                    consumerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 10000, 10));

            assertTrue("Consumer closed callback didn't trigger", consumerClosed.await(2000, TimeUnit.MILLISECONDS));
            assertFalse("JMS Exception listener shouldn't fire with no MessageListener", exceptionFired.await(20, TimeUnit.MILLISECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListener() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            CountDownLatch consumerClosed = new CountDownLatch(1);
            CountDownLatch exceptionFired = new CountDownLatch(1);

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionFired.countDown();
                }
            });
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConsumerClosed(MessageConsumer consumer, Throwable exception) {
                    consumerClosed.countDown();
                }
            });

            // Use a zero prefetch to allow setMessageListener to not race link close
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setAll(0);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB, 10);

            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                }
            });

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 10000, 10));

            assertTrue("Consumer closed callback didn't trigger",  consumerClosed.await(2000, TimeUnit.MILLISECONDS));
            assertTrue("JMS Exception listener should have fired with a MessageListener", exceptionFired.await(2000, TimeUnit.MILLISECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
        }
    }

    /**
     * Test that a message is received when calling receive with a timeout
     * of 0, which means wait indefinitely.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveMessageWithReceiveZeroTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(0);

            assertNotNull("A message should have been recieved", receivedMessage);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Test that an Ack is not dropped when RTE is thrown from onMessage
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testRuntimeExceptionInOnMessageReleasesInAutoAckMode() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsReleasedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    throw new RuntimeException();
                }
            });

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Test that an Ack is not dropped when RTE is thrown from onMessage
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testRuntimeExceptionInOnMessageReleasesInDupsOkAckMode() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsReleasedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    throw new RuntimeException();
                }
            });

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseDurableTopicSubscriberDetachesWithCloseFalse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);

            testPeer.expectDetach(false, true, false);
            subscriber.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testCloseDurableSubscriberWithUnackedAnUnconsumedPrefetchedMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            String topicName = "myTopic";
            String subscriptionName = "mySubscription";

            Topic topic = session.createTopic(topicName);

            int messageCount = 5;
            // Create a consumer and fill the prefetch with some messages,
            // which we will consume some of but ack none of.
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName);

            int consumeCount = 2;
            Message receivedMessage = null;

            for (int i = 1; i <= consumeCount; i++) {
                receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Expect the messages that were not delivered to be released.
            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new AcceptedMatcher());
            }

            receivedMessage.acknowledge();

            testPeer.expectDetach(false, true, false);

            for (int i = consumeCount + 1 ; i <= messageCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            testPeer.expectEnd();

            consumer.close();
            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testConsumerReceiveThrowsIfConnectionLost() throws Exception {
        doConsumerReceiveThrowsIfConnectionLostTestImpl(false);
    }

    @Test(timeout=20000)
    public void testConsumerTimedReceiveThrowsIfConnectionLost() throws Exception {
        doConsumerReceiveThrowsIfConnectionLostTestImpl(true);
    }

    private void doConsumerReceiveThrowsIfConnectionLostTestImpl(boolean useTimeout) throws JMSException, Exception, IOException {
        final CountDownLatch consumerReady = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("queue");
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.runAfterLastHandler(new AmqpPeerRunnable() {
                @Override
                public void run() {
                    try {
                        consumerReady.await(2000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOG.warn("interrupted while waiting");
                        Thread.currentThread().interrupt();
                    }
                }
            });
            testPeer.dropAfterLastHandler(10);

            final MessageConsumer consumer = session.createConsumer(queue);
            consumerReady.countDown();

            try {
                if (useTimeout) {
                    consumer.receive(100000);
                } else {
                    consumer.receive();
                }

                fail("An exception should have been thrown");
            } catch (JMSException jmse) {
                // Expected
            }
        }
    }

    @Test(timeout=20000)
    public void testConsumerReceiveNoWaitThrowsIfConnectionLost() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("queue");
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, notNullValue(UnsignedInteger.class));
            testPeer.expectLinkFlow(true, notNullValue(UnsignedInteger.class));
            testPeer.dropAfterLastHandler();

            final MessageConsumer consumer = session.createConsumer(queue);

            try {
                consumer.receiveNoWait();
                fail("An exception should have been thrown");
            } catch (JMSException jmse) {
                // Expected
            }
        }
    }

    @Test(timeout=20000)
    public void testSetMessageListenerAfterStartAndSend() throws Exception {

        final int messageCount = 4;
        final CountDownLatch latch = new CountDownLatch(messageCount);
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            MessageConsumer consumer = session.createConsumer(destination);

            for (int i = 0; i < messageCount; i++) {
                testPeer.expectDisposition(true, new AcceptedMatcher());
            }

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    LOG.debug("Async consumer got Message: {}", m);
                    latch.countDown();
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testNoReceivedMessagesWhenConnectionNotStarted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch incoming = new CountDownLatch(1);
            Connection connection = testFixture.establishConnecton(testPeer);

            // Allow wait for incoming message before we call receive.
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    incoming.countDown();
                }
            });

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 3);

            MessageConsumer consumer = session.createConsumer(destination);

            // Wait for a message to arrive then try and receive it, which should not happen
            // since the connection is not started.
            assertTrue(incoming.await(10, TimeUnit.SECONDS));
            assertNull(consumer.receive(100));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testNoReceivedNoWaitMessagesWhenConnectionNotStarted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch incoming = new CountDownLatch(1);
            Connection connection = testFixture.establishConnecton(testPeer);

            // Allow wait for incoming message before we call receive.
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    incoming.countDown();
                }
            });

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 3);

            MessageConsumer consumer = session.createConsumer(destination);

            // Wait for a message to arrive then try and receive it, which should not happen
            // since the connection is not started.
            assertTrue(incoming.await(10, TimeUnit.SECONDS));
            assertNull(consumer.receiveNoWait());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=60000)
    public void testSyncReceiveFailsWhenListenerSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            MessageConsumer consumer = session.createConsumer(destination);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    LOG.warn("Async consumer got unexpected Message: {}", m);
                }
            });

            try {
                consumer.receive();
                fail("Should have thrown an exception.");
            } catch (JMSException ex) {
            }

            try {
                consumer.receive(1000);
                fail("Should have thrown an exception.");
            } catch (JMSException ex) {
            }

            try {
                consumer.receiveNoWait();
                fail("Should have thrown an exception.");
            } catch (JMSException ex) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateProducerInOnMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue destination = session.createQueue(getTestName());
            final Queue outboud = session.createQueue(getTestName() + "-FowardDest");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach();
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            MessageConsumer consumer = session.createConsumer(destination);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    LOG.debug("Async consumer got Message: {}", message);
                    try {
                        LOG.debug("Received async message: {}", message);
                        MessageProducer producer = session.createProducer(outboud);
                        producer.send(message);
                        producer.close();
                        LOG.debug("forwarded async message: {}", message);
                    } catch (Throwable e) {
                        LOG.debug("Caught exception: {}", e);
                        throw new RuntimeException(e.getMessage());
                    }
                }
            });

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=30000)
    public void testReceiveWithTimoutDrainsOnNoMessage() throws IOException, Exception {
        doDrainOnNoMessageTestImpl(false, true);
    }

    @Test(timeout=30000)
    public void testReceiveWithTimoutDoesntDrainOnNoMessageWithLocalOnlyOption() throws IOException, Exception {
        doDrainOnNoMessageTestImpl(true, true);
    }

    @Test(timeout=30000)
    public void testReceiveNoWaitDrainsOnNoMessage() throws IOException, Exception {
        doDrainOnNoMessageTestImpl(false, false);
    }

    @Test(timeout=30000)
    public void testReceiveNoWaitDoesntDrainOnNoMessageWithLocalOnlyOption() throws IOException, Exception {
        doDrainOnNoMessageTestImpl(true, false);
    }

    private void doDrainOnNoMessageTestImpl(boolean localCheckOnly, boolean noWait) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = null;
            if (localCheckOnly) {
                if (noWait) {
                    connection = testFixture.establishConnecton(testPeer, "?jms.receiveNoWaitLocalOnly=true");
                } else {
                    connection = testFixture.establishConnecton(testPeer, "?jms.receiveLocalOnly=true");
                }
            } else {
                connection = testFixture.establishConnecton(testPeer);
            }

            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expect receiver link attach and send credit
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            if (!localCheckOnly) {
                // If not doing local-check only, expect the credit to be drained
                // and then replenished to open the window again
                testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
                testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            }

            MessageConsumer consumer = session.createConsumer(queue);
            Message msg = null;
            if (noWait) {
                msg = consumer.receiveNoWait();
            } else {
                msg = consumer.receive(1);
            }

            assertNull(msg);

            // Then close the consumer
            testPeer.expectDetach(true, true, true);

            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testReceiveWithTimoutAndNoDrainResponseFailsAfterTimeout() throws IOException, Exception {
        doDrainWithNoResponseOnNoMessageTestImpl(false, false);
    }

    @Test(timeout=30000)
    public void testReceiveNoWaitAndNoDrainResponseFailsAfterTimeout() throws IOException, Exception {
        doDrainWithNoResponseOnNoMessageTestImpl(true, false);
    }

    @Test(timeout=30000)
    public void testDurableReceiveWithTimoutAndNoDrainResponseFailsAfterTimeout() throws IOException, Exception {
        doDrainWithNoResponseOnNoMessageTestImpl(false, true);
    }

    @Test(timeout=30000)
    public void testDurableReceiveNoWaitAndNoDrainResponseFailsAfterTimeout() throws IOException, Exception {
        doDrainWithNoResponseOnNoMessageTestImpl(true, true);
    }

    private void doDrainWithNoResponseOnNoMessageTestImpl(boolean noWait, boolean durable) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = null;
            connection = testFixture.establishConnecton(testPeer, "?amqp.drainTimeout=500");

            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("topic");

            // Expect receiver link attach and send credit
            if (durable) {
                testPeer.expectDurableSubscriberAttach(topic.getTopicName(), getTestName());
            } else {
                testPeer.expectReceiverAttach();
            }

            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_TOPIC_PREFETCH)));

            // Expect drain but do not respond so that the consumer times out.
            testPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_TOPIC_PREFETCH)));

            // Consumer should close due to timed waiting for drain.
            if (durable) {
                testPeer.expectDetach(false, true, true);
            } else {
                testPeer.expectDetach(true, true, true);
            }

            final MessageConsumer consumer;
            if (durable) {
                consumer = session.createDurableSubscriber(topic, getTestName());
            } else {
                consumer = session.createConsumer(topic);
            }

            try {
                if (noWait) {
                    consumer.receiveNoWait();
                } else {
                    consumer.receive(1);
                }

                fail("Drain timeout should have aborted the receive.");
            } catch (JMSException ex) {
                LOG.info("Receive failed after drain timeout as expected: {}", ex.getMessage());
            }

            assertTrue("Consumer should close", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageSelector();
                        return false;
                    } catch (JMSException ex) {
                        return true;
                    }
                }
            }));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    /* Check the clients view of the remaining credit stays in sync with the transports
     * even in the face of the remote peer advancing the delivery count unexpectedly,
     * ensuring the client doesn't later think there is credit when there is none.
     *
     * QPIDJMS-139 / QPID-6947 / PROTON-1077
     *
     * Expect receiver link to attach and send credit, then:
     * - Have test peer sender pretend it got multiple flows, and only some of the credit had previously arrived.
     * - Unexpectedly advance delivery count and use up some of credit, set a 0 link credit.
     * - Top up the link credit with the remainder as if it had just arrived, not advancing delivery count.
     * - Send messages using up the remaining credit.
     * - Check when the consumer tops the credit back up to the initial value that the
     *   value of link-credit on the wire is actually as expected.
     */
    @Test(timeout=30000)
    public void testCreditReplenishmentWhenSenderAdvancesDeliveryCountUnexpectedly() throws Exception {
        int prefetch = 4;
        int topUp = 2;
        int messageCount = prefetch - topUp;
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + prefetch);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowThenPerformUnexpectedDeliveryCountAdvanceThenCreditTopupThenTransfers(prefetch, topUp, messageCount);

            // Expect consumer to top up the credit window to <prefetch-1> when accepting the first message, accounting
            // for the fact the second prefetched message is still in its local buffer.
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(prefetch-1)));
            testPeer.expectDisposition(true, new AcceptedMatcher(), 0, 0);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            // First timeout used is large to ensure the client sees the
            // delivery count advancement without trying to drain first.
            MessageConsumer consumer = session.createConsumer(queue);
            Message msg = consumer.receive(10000);
            assertNotNull("Should have received message 1", msg);
            msg = consumer.receive(1);
            assertNotNull("Should have received message 2", msg);

            // Then close the consumer
            testPeer.expectDetach(true, true, true);

            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testMessageListenerCallsConnectionCloseThrowsIllegalStateException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> asyncError = new AtomicReference<Exception>(null);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    try {
                        LOG.debug("Async consumer got Message: {}", m);
                        connection.close();
                    } catch (Exception ex) {
                        asyncError.set(ex);
                    }

                    latch.countDown();
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            assertNotNull(asyncError.get());
            assertTrue(asyncError.get() instanceof IllegalStateException);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testMessageListenerCallsConnectionStopThrowsIllegalStateException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> asyncError = new AtomicReference<Exception>(null);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    try {
                        LOG.debug("Async consumer got Message: {}", m);
                        connection.stop();
                    } catch (Exception ex) {
                        asyncError.set(ex);
                    }

                    latch.countDown();
                }
            });

            boolean await = latch.await(3, TimeUnit.MINUTES);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            assertNotNull("Expected IllegalStateException", asyncError.get());
            assertTrue(asyncError.get() instanceof IllegalStateException);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testMessageListenerCallsSessionCloseThrowsIllegalStateException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> asyncError = new AtomicReference<Exception>(null);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    try {
                        LOG.debug("Async consumer got Message: {}", m);
                        session.close();
                    } catch (Exception ex) {
                        asyncError.set(ex);
                    }

                    latch.countDown();
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            Exception ex = asyncError.get();

            assertNotNull("Expected an exception", ex);

            boolean expectedType = ex instanceof IllegalStateException;
            if(!expectedType) {
                LOG.error("Unexpected exception type", ex);
            }
            assertTrue("Got unexpected exception type: " + ex, expectedType);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testMessageListenerClosesItsConsumer() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch exceptionListenerFired = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.countDown();
                }
            });

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH -1)));
            testPeer.expectDisposition(true, new AcceptedMatcher());
            testPeer.expectDetach(true, true, true);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    try {
                        consumer.close();
                    } catch (Throwable t) {
                        error.set(t);
                        LOG.error("Unexpected error during close", t);
                    }

                    latch.countDown();
                    LOG.debug("Async consumer got Message: {}", m);
                }
            });

            assertTrue("Process not completed within given timeout", latch.await(3000, TimeUnit.MILLISECONDS));
            assertNull("No error expected during close", error.get());

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            assertFalse("JMS Exception listener shouldn't have fired", exceptionListenerFired.await(20, TimeUnit.MILLISECONDS));
            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout=20000)
    public void testRecoverOrderingWithAsyncConsumer() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> asyncError = new AtomicReference<Throwable>(null);

        final int recoverCount = 5;
        final int messageCount = 8;
        final int testPayloadLength = 255;
        String payload = new String(new byte[testPayloadLength], StandardCharsets.UTF_8);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();

            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(payload),
                    messageCount, false, false, Matchers.greaterThan(UnsignedInteger.valueOf(messageCount)), 1, true);

            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(new MessageListener() {
                boolean complete;
                private int messageSeen = 0;
                private int expectedIndex = 0;

                @Override
                public void onMessage(Message message) {
                    if (complete) {
                        LOG.debug("Ignoring message as test already completed (either pass or fail)");
                        return;
                    }

                    try {
                        int actualIndex = message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER);
                        LOG.debug("Got message {}", actualIndex);
                        assertEquals("Received Message Out Of Order", expectedIndex, actualIndex);

                        // don't ack the message until we receive it X times
                        if (messageSeen < recoverCount) {
                            LOG.debug("Ignoring message " + actualIndex + " and calling recover");
                            session.recover();
                            messageSeen++;
                        } else {
                            messageSeen = 0;
                            expectedIndex++;

                            // Have the peer expect the accept the disposition (1-based, hence pre-incremented).
                            testPeer.expectDisposition(true, new AcceptedMatcher(), expectedIndex, expectedIndex);

                            LOG.debug("Acknowledging message {}", actualIndex);
                            message.acknowledge();

                            //testPeer.waitForAllHandlersToComplete(2000);

                            if (expectedIndex == messageCount) {
                                complete = true;
                                latch.countDown();
                            }
                        }
                    } catch (Throwable t) {
                        complete = true;
                        asyncError.set(t);
                        latch.countDown();
                    }
                }
            });

            boolean await = latch.await(15, TimeUnit.SECONDS);
            assertTrue("Messages not received within given timeout." + latch.getCount(), await);

            Throwable ex = asyncError.get();
            assertNull("Unexpected exception", ex);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testConsumerCloseWaitsForAsyncDeliveryToComplete() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    latch.countDown();

                    LOG.debug("Async consumer got Message: {}", m);
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testSessionCloseWaitsForAsyncDeliveryToComplete() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    latch.countDown();

                    LOG.debug("Async consumer got Message: {}", m);
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            testPeer.expectEnd();
            session.close();

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testConnectionCloseWaitsForAsyncDeliveryToComplete() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            connection.start();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.expectDisposition(true, new AcceptedMatcher());

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message m) {
                    latch.countDown();

                    LOG.debug("Async consumer got Message: {}", m);
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            });

            boolean await = latch.await(3000, TimeUnit.MILLISECONDS);
            assertTrue("Messages not received within given timeout. Count remaining: " + latch.getCount(), await);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testCloseClientAckAsyncConsumerCanStillAckMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            int messageCount = 5;
            int consumeCount = 5;

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            final CountDownLatch consumedLatch = new CountDownLatch(consumeCount);
            final AtomicReference<Message> receivedMessage = new AtomicReference<>();

            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    receivedMessage.set(message);
                    consumedLatch.countDown();
                }
            });

            assertTrue("Did not consume all messages", consumedLatch.await(10, TimeUnit.SECONDS));

            // Expect the client to then drain off all credit from the link.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - messageCount)));

            // Close should be deferred as these messages were delivered but not acknowledged.
            consumer.close();

            testPeer.waitForAllHandlersToComplete(3000);

            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new AcceptedMatcher());
            }
            // Now the consumer should close.
            testPeer.expectDetach(true, true, true);

            // Ack the last read message, which should accept all previous messages as well.
            receivedMessage.get().acknowledge();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testCloseClientAckSyncConsumerCanStillAckMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            int messageCount = 5;
            int consumeCount = 3;

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            final CountDownLatch expected = new CountDownLatch(messageCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = null;

            for (int i = 1; i <= consumeCount; i++) {
                receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(5, TimeUnit.SECONDS));

            // Expect the client to then drain off all credit from the link.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - messageCount)));

            // Expect the prefetched messages to be released for dispatch elsewhere.
            for (int i = 1; i <= messageCount - consumeCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            // Close should be deferred as these messages were delivered but not acknowledged.
            consumer.close();

            testPeer.waitForAllHandlersToComplete(3000);

            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new AcceptedMatcher());
            }
            // Now the consumer should close.
            testPeer.expectDetach(true, true, true);

            // Ack the last read message, which should accept all previous messages as well.
            receivedMessage.acknowledge();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testCloseConsumersWithDeferredAckHandledLaterWhenlastConsumedMessageIsAcked() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 10;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content-for-consumer-1"),
                    1, false, false, Matchers.equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH)), 1, true);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content-for-consumer-2"),
                    1, false, false, Matchers.equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH)), 2, true);

            final CountDownLatch expected = new CountDownLatch(2);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            // These are our two consumers, the first gets a message and abandons it, the second will have
            // acknowledge called on its message which will lead to the message for the first to be acknowledged
            // and then it's link will be closed.
            MessageConsumer consumer1 = session.createConsumer(queue);
            MessageConsumer consumer2 = session.createConsumer(queue);
            Message receivedMessage1 = null;
            Message receivedMessage2 = null;

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(5, TimeUnit.SECONDS));

            // Take our two messages from the queue leaving them in a delivered state.
            receivedMessage1 = consumer1.receive(3000);
            assertNotNull(receivedMessage1);
            assertTrue(receivedMessage1 instanceof TextMessage);
            receivedMessage2 = consumer2.receive(3000);
            assertNotNull(receivedMessage2);
            assertTrue(receivedMessage2 instanceof TextMessage);

            // Expect the client to then drain off all credit from the link when "closed"
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - 1)));
            // Expect the client to then drain off all credit from the link when "closed"
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - 1)));

            // Close should be deferred as the messages were delivered but not acknowledged.
            consumer1.close();
            consumer2.close();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectDisposition(true, new AcceptedMatcher());
            testPeer.expectDisposition(true, new AcceptedMatcher());

            // Now the links should close as we tear down the deferred consumers
            testPeer.expectDetach(true, true, true);
            testPeer.expectDetach(true, true, true);

            // Acknowledge the last read message, which should accept all previous messages as well
            // and our consumers should then close their links in turn.
            receivedMessage2.acknowledge();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testConsumerWithDeferredCloseAcksAsClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            int messageCount = 5;

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            final CountDownLatch expected = new CountDownLatch(messageCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            MessageConsumer consumer = session.createConsumer(queue);

            int consumeCount = 3;
            Message receivedMessage = null;

            for (int i = 1; i <= consumeCount; i++) {
                receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(5, TimeUnit.SECONDS));

            // Expect the client to then drain off all credit from the link.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - messageCount)));

            // Expect the prefetched messages to be released for dispatch elsewhere.
            for (int i = 1; i <= messageCount - consumeCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            // Close should be deferred as these messages were delivered but not acknowledged.
            consumer.close();

            testPeer.waitForAllHandlersToComplete(3000);

            // Should not be able to consume from the consumer once closed.
            try {
                consumer.receive();
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            try {
                consumer.receive(100);
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            try {
                consumer.receiveNoWait();
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new AcceptedMatcher());
            }

            // Now the consumer should close.
            testPeer.expectDetach(true, true, true);

            // Ack the last read message, which should accept all previous messages as well.
            receivedMessage.acknowledge();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testConsumerWithDeferredCloseAcksDeliveryFailedAsSessionClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            int messageCount = 5;

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            final CountDownLatch expected = new CountDownLatch(messageCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            MessageConsumer consumer = session.createConsumer(queue);

            int consumeCount = 3;
            Message receivedMessage = null;

            for (int i = 1; i <= consumeCount; i++) {
                receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(5, TimeUnit.SECONDS));

            // Expect the client to then drain off all credit from the link.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - messageCount)));

            // Expect the prefetched messages to be released for dispatch elsewhere.
            testPeer.expectDisposition(true, new ReleasedMatcher(), 4, 4);
            testPeer.expectDisposition(true, new ReleasedMatcher(), 5, 5);

            // Close should be deferred as these messages were delivered but not acknowledged.
            consumer.close();

            testPeer.waitForAllHandlersToComplete(3000);

            // Should not be able to consume from the consumer once closed.
            try {
                consumer.receive();
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            try {
                consumer.receive(100);
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            try {
                consumer.receiveNoWait();
                fail("Should throw as this consumer is closed.");
            } catch (IllegalStateException ise) {}

            // Now the delivered messages should be acknowledged as session shuts down.
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 1, 1);
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 2, 2);
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 3, 3);
            testPeer.expectDetach(true, true, true);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testDeferredCloseTimeoutAlertsExceptionListener() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch errorLatch = new CountDownLatch(1);

            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer,
                "jms.closeTimeout=500&jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    if (exception instanceof JmsOperationTimedOutException) {
                        errorLatch.countDown();
                    }
                }
            });
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = consumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);

            // Expect the client to then drain off all credit from the link.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - 1)));

            // Close should be deferred as these messages were delivered but not acknowledged.
            consumer.close();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectDisposition(true, new AcceptedMatcher());
            // Now the consumer should close.
            testPeer.expectDetach(true, false, true);

            // Ack the read message, which should accept all previous messages as well.
            receivedMessage.acknowledge();

            assertTrue("Did not get timed out error", errorLatch.await(10, TimeUnit.SECONDS));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testSessionCloseDoesNotDeferConsumerClose() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            MessageConsumer consumer = session.createConsumer(queue);

            Message receivedMessage = consumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);

            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 1, 1);
            testPeer.expectEnd();

            session.close();

            // Consumer and Session should be closed, not acknowledge allowed
            try {
                receivedMessage.acknowledge();
                fail("Should not be allowed to call acknowledge after session closed.");
            } catch (IllegalStateException ise) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedAccepted() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(1, false);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedRejected() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(2, false);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedReleased() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(3, false);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedModifiedFailed() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(4, false);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedModifiedFailedUndeliverable() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(5, false);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedAcceptedString() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(1, true);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedRejectedString() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(2, true);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedReleasedString() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(3, true);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedModifiedFailedString() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(4, true);
    }

    @Test(timeout=20000)
    public void testRedeliveryPolicyOutcomeAppliedModifiedFailedUndeliverableString() throws Exception {
        doTestRedeliveryPolicyOutcomeApplied(5, true);
    }

    private void doTestRedeliveryPolicyOutcomeApplied(int outcome, boolean useStringOption) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            Object outcomeValue = outcome;
            if (useStringOption) {
                //  ACCEPTED = 1
                //  REJECTED = 2
                //  RELEASED = 3
                //  MODIFIED_FAILED = 4
                //  MODIFIED_FAILED_UNDELIVERABLE = 5
                switch (outcome) {
                    case 1:
                        outcomeValue = "ACCEPTED";
                        break;
                    case 2:
                        outcomeValue = "REJECTED";
                        break;
                    case 3:
                        outcomeValue = "RELEASED";
                        break;
                    case 4:
                        outcomeValue = "MODIFIED_FAILED";
                        break;
                    case 5:
                        outcomeValue = "MODIFIED_FAILED_UNDELIVERABLE";
                        break;
                    default:
                        throw new IllegalArgumentException("Test passed invalid outcome value");
                }
            }

            Connection connection = testFixture.establishConnecton(testPeer,
                "?jms.redeliveryPolicy.maxRedeliveries=1&jms.redeliveryPolicy.outcome=" + outcomeValue);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            HeaderDescribedType header = new HeaderDescribedType();
            header.setDeliveryCount(new UnsignedInteger(2));

            testPeer.expectReceiverAttach();
            // Send some messages that have exceeded the specified re-delivery count
            testPeer.expectLinkFlowRespondWithTransfer(header, null, null, null, new AmqpValueDescribedType("redelivered-content"), 1);
            // Send a message that has not exceeded the delivery count
            String expectedContent = "not-redelivered";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(expectedContent), 2);

            Matcher<?> outcomeMatcher = null;

            // Expect a disposition matching the given outcome index:
            //
            //  ACCEPTED = 1
            //  REJECTED = 2
            //  RELEASED = 3
            //  MODIFIED_FAILED = 4
            //  MODIFIED_FAILED_UNDELIVERABLE = 5
            switch (outcome) {
                case 1:
                    outcomeMatcher = new AcceptedMatcher();
                    break;
                case 2:
                    outcomeMatcher = new RejectedMatcher();
                    break;
                case 3:
                    outcomeMatcher = new ReleasedMatcher();
                    break;
                case 4:
                    ModifiedMatcher failed = new ModifiedMatcher();
                    failed.withDeliveryFailed(equalTo(true));
                    outcomeMatcher = failed;
                    break;
                case 5:
                    ModifiedMatcher undeliverable = new ModifiedMatcher();
                    undeliverable.withDeliveryFailed(equalTo(true));
                    undeliverable.withUndeliverableHere(equalTo(true));
                    outcomeMatcher = undeliverable;
                    break;
                default:
                    throw new IllegalArgumentException("Test passed invalid outcome value");
            }

            // Expect a settled disposition matching the configured redelivery policy outcome
            testPeer.expectDisposition(true, outcomeMatcher);

            // Then expect an Accepted disposition for the good message
            testPeer.expectDisposition(true, new AcceptedMatcher());

            final MessageConsumer consumer = session.createConsumer(queue);

            Message m = consumer.receive(6000);
            assertNotNull("Should have reiceved the final message", m);
            assertTrue("Should have received the final message", m instanceof TextMessage);
            assertEquals("Unexpected content", expectedContent, ((TextMessage)m).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testLinkCreditReplenishmentWithPrefetchFilled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int prefetch = 10;
            int initialMessageCount = 10;

            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + prefetch);
            connection.start();

            final CountDownLatch expected = new CountDownLatch(initialMessageCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();

            // Expect initial credit to be sent, respond with some messages using it all
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                initialMessageCount, false, false, equalTo(UnsignedInteger.valueOf(prefetch)), 1, false, false);

            MessageConsumer consumer = session.createConsumer(queue);

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(5, TimeUnit.SECONDS));

            // Now consume the first 2 messages, expect the ack processing NOT to provoke flowing more credit, as over
            // 70% of the max potential prefetch (8 outstanding local messages) remains in play.
            int consumed = 0;
            for (consumed = 1; consumed <= 2; consumed ++) {
                testPeer.expectDisposition(true, new AcceptedMatcher(), consumed, consumed);
                Message message = consumer.receiveNoWait();
                assertNotNull("Should have received a message " + consumed, message);
            }

            // Now consume 3rd message, expect the ack processing to provoke flowing more credit as it hits the
            // 70% low threshhold (credit + prefetched) for replenishing the credit to max out the prefetch window
            // again, accounting for the already-prefetched but still not yet consumed messages.
            // Also have the peer send more messages using all the remaining credit granted.
            consumed = 3;
            int newOutstandingCredit = 3;
            assertEquals("Peer cant send more messages than we will have credit for", newOutstandingCredit, prefetch - initialMessageCount + consumed);

            final CountDownLatch expected2 = new CountDownLatch(newOutstandingCredit);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected2.countDown();
                }
            });

            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                    newOutstandingCredit, false, false, equalTo(UnsignedInteger.valueOf(newOutstandingCredit)), initialMessageCount + 1, false, false);
            testPeer.expectDisposition(true, new AcceptedMatcher(), consumed, consumed);

            Message message = consumer.receiveNoWait();
            assertNotNull("Should have received a 3rd message " + consumed, message);

            // Ensure all the new messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected2.getCount(), expected2.await(5, TimeUnit.SECONDS));

            // Consume the rest of the messages, 4-13. Expect only dispositions initially until the threshold, then
            // another flow expanding the window to the limit again, then more dispositions, then another flow as the
            // threshold is crossed again when the buffered message count decreases further.
            for (consumed = 4; consumed < prefetch + newOutstandingCredit; consumed++) {
                if(consumed == 3 + newOutstandingCredit) {
                    testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(newOutstandingCredit)));
                } else if(consumed == 3 + newOutstandingCredit * 2) {
                    testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(newOutstandingCredit * 2)));
                }
                testPeer.expectDisposition(true, new AcceptedMatcher(), consumed, consumed);

                message = consumer.receiveNoWait();
                assertNotNull("Should have received a message " + consumed, message);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testLinkCreditReplenishmentWithPrefetchTrickleFeed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int prefetch = 10;

            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + prefetch);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();

            // Expect initial credit to be sent, respond with a single message.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                1, false, false, equalTo(UnsignedInteger.valueOf(prefetch)), 1, false, false);

            MessageConsumer consumer = session.createConsumer(queue);

            // Now consume a message and have the peer send another in response, repeat until consumed 15 messages
            int consumed = 0;
            for (consumed = 1; consumed <= 15; consumed ++) {
                if(consumed == 5 || consumed == 10 || consumed == 15) {
                    testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(prefetch)));
                }

                testPeer.expectDisposition(true, new AcceptedMatcher(), consumed, consumed);
                if(consumed != 15) {
                    testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, amqpValueNullContent, consumed +1);
                }

                Message message = consumer.receive(3000);
                assertNotNull("Should have received a message " + consumed, message);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
