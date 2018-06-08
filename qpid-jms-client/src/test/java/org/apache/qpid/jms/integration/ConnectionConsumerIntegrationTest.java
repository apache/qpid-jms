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
package org.apache.qpid.jms.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for expected behaviors of JMS Connection Consumer implementation.
 */
public class ConnectionConsumerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionConsumerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCreateConnectionConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsServerSessionPool sessionPool = new JmsServerSessionPool();
            Connection connection = testFixture.establishConnecton(testPeer);

            // No additional Begin calls as there's no Session created for a Connection Consumer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectionConsumerDispatchesToSessionConnectionStartedBeforeCreate() throws Exception {
        doTestConnectionConsumerDispatchesToSession(true);
    }

    @Test(timeout = 20000)
    public void testConnectionConsumerDispatchesToSessionConnectionStartedAfterCreate() throws Exception {
        doTestConnectionConsumerDispatchesToSession(false);
    }

    private void doTestConnectionConsumerDispatchesToSession(boolean startBeforeCreate) throws Exception {
        final CountDownLatch messageArrived = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            if (startBeforeCreate) {
                connection.start();
            }

            testPeer.expectBegin();

            // Create a session for our ServerSessionPool to use
            Session session = connection.createSession();
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    messageArrived.countDown();
                }
            });
            JmsServerSession serverSession = new JmsServerSession(session);
            JmsServerSessionPool sessionPool = new JmsServerSessionPool(serverSession);

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            if (!startBeforeCreate) {
                connection.start();
            }

            assertTrue("Message didn't arrive in time", messageArrived.await(10, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testPauseInOnMessageAndConsumerClosed() throws Exception {
        final CountDownLatch messageArrived = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();

            // Create a session for our ServerSessionPool to use
            Session session = connection.createSession();
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    messageArrived.countDown();

                    LOG.trace("Pausing onMessage to check for race on connection consumer close");

                    // Pause a bit to see if we race consumer close and our own
                    // message accept attempt by the delivering Session.
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                    }

                    LOG.trace("Paused onMessage to check for race on connection consumer close");
                }
            });
            JmsServerSession serverSession = new JmsServerSession(session);
            JmsServerSessionPool sessionPool = new JmsServerSessionPool(serverSession);

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            connection.start();

            assertTrue("Message didn't arrive in time", messageArrived.await(10, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testNonStartedConnectionConsumerDoesNotDispatch() throws Exception {
        final CountDownLatch messageArrived = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();

            // Create a session for our ServerSessionPool to use
            Session session = connection.createSession();
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    messageArrived.countDown();
                }
            });
            JmsServerSession serverSession = new JmsServerSession(session);
            JmsServerSessionPool sessionPool = new JmsServerSessionPool(serverSession);

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertFalse("Message Arrived unexpectedly", messageArrived.await(500, TimeUnit.MILLISECONDS));

            testPeer.expectDetach(true, true, true);
            testPeer.expectDispositionThatIsReleasedAndSettled();
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testQueuedMessagesAreDrainedToServerSession() throws Exception {
        final int MESSAGE_COUNT = 10;
        final CountDownLatch messagesDispatched = new CountDownLatch(MESSAGE_COUNT);
        final CountDownLatch messagesArrived = new CountDownLatch(MESSAGE_COUNT);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    messagesDispatched.countDown();
                }
            });

            testPeer.expectBegin();

            // Create a session for our ServerSessionPool to use
            Session session = connection.createSession();
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    messagesArrived.countDown();
                }
            });

            JmsServerSession serverSession = new JmsServerSession(session);
            JmsServerSessionPool sessionPool = new JmsServerSessionPool(serverSession);

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, MESSAGE_COUNT);

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                testPeer.expectDispositionThatIsAcceptedAndSettled();
            }

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertTrue("Message didn't arrive in time", messagesDispatched.await(10, TimeUnit.SECONDS));
            assertEquals(MESSAGE_COUNT, messagesArrived.getCount());

            connection.start();

            assertTrue("Message didn't arrive in time", messagesArrived.await(10, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConsumerRecoversAfterSessionPoolReturnsNullSession() throws Exception {
        final int MESSAGE_COUNT = 10;
        final CountDownLatch messagesDispatched = new CountDownLatch(MESSAGE_COUNT);
        final CountDownLatch messagesArrived = new CountDownLatch(MESSAGE_COUNT);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    messagesDispatched.countDown();
                }
            });

            testPeer.expectBegin();

            // Create a session for our ServerSessionPool to use
            Session session = connection.createSession();
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    messagesArrived.countDown();
                }
            });

            JmsServerSession serverSession = new JmsServerSession(session);
            JmsServerSessionPoolFirstAttemptGetsNull sessionPool = new JmsServerSessionPoolFirstAttemptGetsNull(serverSession);

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, MESSAGE_COUNT);

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                testPeer.expectDispositionThatIsAcceptedAndSettled();
            }

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertTrue("Message didn't arrive in time", messagesDispatched.await(10, TimeUnit.SECONDS));
            assertEquals(MESSAGE_COUNT, messagesArrived.getCount());

            connection.start();

            assertTrue("Message didn't arrive in time", messagesArrived.await(10, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch connectionError = new CountDownLatch(1);
            JmsServerSessionPool sessionPool = new JmsServerSessionPool();
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    connectionError.countDown();
                }
            });

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getServerSessionPool();
                    } catch (IllegalStateException jmsise) {
                        LOG.debug("Error reported from consumer.getServerSessionPool()", jmsise);
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

            assertTrue("Consumer closed callback didn't trigger", connectionError.await(5, TimeUnit.SECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testOnExceptionFiredOnSessionPoolFailure() throws Exception {
        final CountDownLatch exceptionFired = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    exceptionFired.countDown();
                }
            });

            connection.start();

            JmsFailingServerSessionPool sessionPool = new JmsFailingServerSessionPool();

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertTrue("Exception should have been fired", exceptionFired.await(5, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            testPeer.expectDispositionThatIsReleasedAndSettled();
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testOnExceptionFiredOnServerSessionFailure() throws Exception {
        final CountDownLatch exceptionFired = new CountDownLatch(1);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    exceptionFired.countDown();
                }
            });

            connection.start();

            JmsServerSessionPool sessionPool = new JmsServerSessionPool(new JmsFailingServerSession());

            // Now the Connection consumer arrives and we give it a message
            // to be dispatched to the server session.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertTrue("Exception should have been fired", exceptionFired.await(5, TimeUnit.SECONDS));

            testPeer.expectDetach(true, true, true);
            testPeer.expectDispositionThatIsReleasedAndSettled();
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //----- Internal ServerSessionPool ---------------------------------------//

    private class JmsFailingServerSessionPool implements ServerSessionPool {

        public JmsFailingServerSessionPool() {
        }

        @Override
        public ServerSession getServerSession() throws JMSException {
            throw new JMSException("Something is wrong with me");
        }
    }

    private class JmsServerSessionPool implements ServerSessionPool {

        private JmsServerSession serverSession;

        public JmsServerSessionPool() {
            this.serverSession = new JmsServerSession();
        }

        public JmsServerSessionPool(JmsServerSession serverSession) {
            this.serverSession = serverSession;
        }

        @Override
        public ServerSession getServerSession() throws JMSException {
            return serverSession;
        }
    }

    private class JmsServerSessionPoolFirstAttemptGetsNull implements ServerSessionPool {

        private volatile boolean firstAttempt = true;
        private JmsServerSession serverSession;

        public JmsServerSessionPoolFirstAttemptGetsNull(JmsServerSession serverSession) {
            this.serverSession = serverSession;
        }

        @Override
        public ServerSession getServerSession() throws JMSException {
            if (firstAttempt) {
                firstAttempt = false;
                return null;
            } else {
                return serverSession;
            }
        }
    }

    private class JmsServerSession implements ServerSession {

        private final Session session;
        private final ExecutorService runner = Executors.newSingleThreadExecutor();

        public JmsServerSession() {
            this.session = null;
        }

        public JmsServerSession(Session session) {
            this.session = session;
        }

        @Override
        public Session getSession() throws JMSException {
            return session;
        }

        @Override
        public void start() throws JMSException {
            runner.execute(() -> {
                session.run();
            });
        }
    }

    private class JmsFailingServerSession extends JmsServerSession {

        public JmsFailingServerSession() {
        }

        @Override
        public Session getSession() throws JMSException {
            throw new JMSException("Something is wrong with me");
        }
    }
}
