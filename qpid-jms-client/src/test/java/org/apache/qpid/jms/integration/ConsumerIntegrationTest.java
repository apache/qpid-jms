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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
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
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseConsumerTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);

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
            final AtomicBoolean consumerClosed = new AtomicBoolean();
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConsumerClosed(MessageConsumer consumer, Exception exception) {
                    consumerClosed.set(true);
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
                public boolean isSatisified() throws Exception {
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

            assertTrue("Consumer closed callback didn't trigger", consumerClosed.get());

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
            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            testPeer.expectDetach(false, true, false);

            // Expect the messages that were not acked to be to be either
            // modified or released depending on whether the app saw them
            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)));
            }
            for (int i = consumeCount + 1 ; i <= messageCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }
            testPeer.expectEnd();

            consumer.close();
            session.close();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();
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

    @Test(timeout=20000)
    public void testCannotUseMessageListener() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());

            testPeer.expectReceiverAttach();

            MessageConsumer consumer = session.createConsumer(destination);
            MessageListener listener = new MessageListener() {

                @Override
                public void onMessage(Message message) {
                }
            };

            try {
                consumer.setMessageListener(listener);
                fail("Should not allow listener to be set when prefetch is zero.");
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
        doDrainWithNoResponseOnNoMessageTestImpl(false);
    }

    @Test(timeout=30000)
    public void testReceiveNoWaitAndNoDrainResponseFailsAfterTimeout() throws IOException, Exception {
        doDrainWithNoResponseOnNoMessageTestImpl(true);
    }

    private void doDrainWithNoResponseOnNoMessageTestImpl(boolean noWait) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = null;
            connection = testFixture.establishConnecton(testPeer, "?amqp.drainTimeout=500");

            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expect receiver link attach and send credit
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));

            // Expect drain but do not respond so that the consumer times out.
            testPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));

            // Consumer should close due to timed waiting for drain.
            testPeer.expectDetach(true, true, true);

            final MessageConsumer consumer = session.createConsumer(queue);

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
                public boolean isSatisified() throws Exception {
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

            // Expect consumer to top up the credit window to <prefetch> when accepting the messages
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(prefetch)));
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
}
