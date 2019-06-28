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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class ZeroPrefetchIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveWithMessageExpiredInFlight() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect that once receive is called, it flows a credit, give it an already-expired message.
            // Expect it to be filtered due to local expiration checking.
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType("already-expired"));

            ModifiedMatcher modifiedMatcher = new ModifiedMatcher();
            modifiedMatcher.withDeliveryFailed(equalTo(true));
            modifiedMatcher.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modifiedMatcher, 1, 1);

            // Expect the client to then flow another credit requesting a message.
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(1)));

            // Send it a live message, expect it to get accepted.
            String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            Message m = consumer.receive(5000);
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage) m).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveNoWaitDrainsWithOneCredit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            String msgContent = "content";
            // Expect that once receiveNoWait is called, it drains with 1 credit, then give it a message.
            testPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.ONE));
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(msgContent), 1);

            // Expect it to be accepted.
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            Message m = consumer.receiveNoWait();
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", msgContent, ((TextMessage) m).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testZeroPrefetchMessageListener() throws Exception {
        final CountDownLatch msgReceived = new CountDownLatch(1);
        final CountDownLatch completeOnMessage = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            MessageConsumer consumer = session.createConsumer(destination);

            testPeer.waitForAllHandlersToComplete(2000);

            MessageListener listener = new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    msgReceived.countDown();

                    try {
                        completeOnMessage.await(6, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            // Expect that once setMessageListener is called, it flows 1 credit with drain=false. Then give it a message.
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.ONE));
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            consumer.setMessageListener(listener);

            // Wait for message to arrive
            assertTrue("message not received in given time", msgReceived.await(6, TimeUnit.SECONDS));

            // Ensure the handlers are complete at the peer
            testPeer.waitForAllHandlersToComplete(2000);

            // Now allow onMessage to complete, expecting an accept and another flow.
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.ONE));
            completeOnMessage.countDown();

            // Wait for the resulting flow to be received
            testPeer.waitForAllHandlersToComplete(2000);

            // Should not flow more credit after consumer removed and a receive should drain
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.ONE));
            consumer.setMessageListener(null);

            assertNull(consumer.receiveNoWait());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout=40000)
    public void testZeroPrefetchConsumerReceiveUnblockedOnSessionClose() throws Exception {
        doTestZeroPrefetchConsumerReceiveUnblockedOnSessionClose(0);
    }

    @Test(timeout=40000)
    public void testZeroPrefetchConsumerReceiveTimedUnblockedOnSessionClose() throws Exception {
        doTestZeroPrefetchConsumerReceiveUnblockedOnSessionClose(1);
    }

    @Test(timeout=40000)
    public void testZeroPrefetchConsumerReceiveNoWaitUnblockedOnSessionClose() throws Exception {
        doTestZeroPrefetchConsumerReceiveUnblockedOnSessionClose(-1);
    }

    public void doTestZeroPrefetchConsumerReceiveUnblockedOnSessionClose(final int timeout) throws Exception {

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect that once receive is called, it drains with 1 credit, don't answer it
            if (timeout < 0) {
                testPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.ONE));
            } else {
                testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.ONE));
            }

            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (timeout < 0) {
                                consumer.receiveNoWait();
                            } else if (timeout == 0) {
                                consumer.receive();
                            } else {
                                consumer.receive(10000);
                            }
                        } catch (Throwable t) {
                            error.set(t);
                        } finally {
                            done.countDown();
                        }
                    }
                });

                testPeer.waitForAllHandlersToComplete(3000);
                testPeer.expectEnd();

                session.close();

                assertTrue("Consumer did not unblock", done.await(10, TimeUnit.SECONDS));
                assertNull("Consumer receive errored", error.get());
            } finally {
                executor.shutdownNow();
            }

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveTimedPullWithInFlightArrival() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

             String msgContent = "content";
            // Expect that once receive is called, it flows 1 credit. Give it an initial transfer frame with header only.
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.ONE));
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(new HeaderDescribedType(), null, null, null, null, 1, "delivery1", true, 0);
            // Then give it a final transfer with body only, after a delay.
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(msgContent), 1, "delivery1", false, 30);
            // Expect it to be accepted. Depending on timing (e.g in slow CI), a draining Flow might arrive first, allowing for that.
            testPeer.optionalFlow(true, false, equalTo(UnsignedInteger.ONE));
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Message m = consumer.receive(20);

                            assertNotNull("Message should have been received", m);
                            assertTrue(m instanceof TextMessage);
                            assertEquals("Unexpected message content", msgContent, ((TextMessage) m).getText());
                        } catch (Throwable t) {
                            error.set(t);
                        } finally {
                            done.countDown();
                        }
                    }
                });

                assertTrue("Consumer receive task did not complete", done.await(4, TimeUnit.SECONDS));
                assertNull("Consumer receive errored", error.get());
            } finally {
                executor.shutdownNow();
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveTimedPullWithInFlightArrivalTimesOutIfNotCompleted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0&amqp.drainTimeout=75");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect that once receive is called, it flows 1 credit. Give it an initial (ie. more=true) transfer frame with header only.
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.ONE));
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(new HeaderDescribedType(), null, null, null, null, 1, "delivery1", true, 0);
            // Expect the consumer to be closed when stop times out. Depending on timing (e.g in slow CI), a draining Flow might arrive first, allowing for that.
            testPeer.optionalFlow(true, false, equalTo(UnsignedInteger.ONE));
            testPeer.expectDetach(true, true, true);
            testPeer.expectDispositionThatIsReleasedAndSettled();

            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            consumer.receive(20);
                        } catch (Throwable t) {
                            error.set(t);
                        } finally {
                            done.countDown();
                        }
                    }
                });

                assertTrue("Consumer receive task did not complete", done.await(4, TimeUnit.SECONDS));

                Throwable t = error.get();
                assertNotNull("Consumer receive did not throw as expected", t);
                assertTrue("Consumer receive did not throw as expected", t instanceof JMSException);
            } finally {
                executor.shutdownNow();
            }

            assertTrue("Consumer should be closed", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageSelector();
                        return false;
                    } catch (JMSException ex) {
                        return true;
                    }
                }
            }, 5000, 10));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }
}
