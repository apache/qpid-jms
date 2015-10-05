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
package org.apache.qpid.jms.provider.failover;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.util.StopWatch;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverIntegrationTest.class);

    @Test(timeout = 20000)
    public void testConnectSecurityViolation() throws Exception {

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.rejectConnect(AmqpError.UNAUTHORIZED_ACCESS, "Anonymous connections not allowed", null);

            final JmsConnection connection = establishAnonymousConnecton(testPeer);
            try {
                connection.start();
                fail("Should have thrown JMSSecurityException");
            } catch (JMSSecurityException ex) {
            } catch (Exception ex) {
                fail("Should have thrown JMSSecurityException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesImmediateTransportDropAfterConnect() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, one to fail to reconnect to, and a final one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String rejectingURI = createPeerURI(rejectingPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Rejecting peer is at: {}", rejectingURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));
            assertEquals("should not yet have connected to final peer", 1L, finalConnected.getCount());

            // Set expectations on rejecting and final peer
            rejectingPeer.expectSaslHeaderThenDrop();

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();

            // Close the original peer and wait for things to shake out.
            originalPeer.close();

            rejectingPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            //Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesTransportDropBeforeDispositionRecieived() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // Create session+producer, send a persistent message on auto-ack session for synchronous send
            originalPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            originalPeer.expectSenderAttach();

            final MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            final Message message = session.createTextMessage();

            final CountDownLatch senderCompleted = new CountDownLatch(1);
            final AtomicReference<Throwable> problem = new AtomicReference<Throwable>();

            // Have the peer expect the message but NOT send any disposition for it
            originalPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, true);

            Thread runner = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        producer.send(message);
                    } catch (Throwable t) {
                        problem.set(t);
                        LOG.error("Problem in sending thread", t);
                    }
                    finally {
                        senderCompleted.countDown();
                    }
                }
            });
            runner.start();

            // Wait for the message to have been sent and received by peer
            originalPeer.waitForAllHandlersToComplete(3000);

            // Set the secondary peer to expect connection restoration, this time send disposition accepting the message
            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectTransfer(messageMatcher, nullValue(), false, true, new Accepted(), true);

            assertEquals("Should not yet have connected to final peer", 1L, finalConnected.getCount());
            assertEquals("Sender thread should not yet have completed", 1L, senderCompleted.getCount());

            // Close the original peer to provoke reconnect, while send() is still outstanding
            originalPeer.close();

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            boolean await = senderCompleted.await(5, TimeUnit.SECONDS);
            Throwable t = problem.get();
            assertTrue("Sender thread should have completed. Problem: " + t, await);

            //Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropWithModifiedInitialReconnectDelay() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(
                "failover.initialReconnectDelay=1&failover.reconnectDelay=600&failover.maxReconnectAttempts=10",
                originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }


    @Test(timeout = 30000)
    public void testFailoverInitialReconnectDelayDoesNotApplyToInitialConnect() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();) {
            // Create a peer to connect to
            final String originalURI = createPeerURI(originalPeer);

            LOG.info("Original peer is at: {}", originalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();

            int delay = 20000;
            StopWatch watch = new StopWatch();

            JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=" + delay + "&failover.maxReconnectAttempts=1", originalPeer);
            connection.start();

            long taken = watch.taken();

            String message = "Initial connect should not have delayed for the specified initialReconnectDelay." + "Elapsed=" + taken + ", delay=" + delay;
            assertTrue(message,  taken < delay);
            assertTrue("Connection took longer than reasonable: " + taken, taken < 5000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropZeroPrefetchPullConsumerReceiveNoWait() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.ONE));
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.getPrefetchPolicy().setQueuePrefetch(0);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.ONE));
            finalPeer.expectDetach(true, true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            assertNull(consumer.receiveNoWait());

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            consumer.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropZeroPrefetchPullConsumerReceiveWithTimeout() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.getPrefetchPolicy().setQueuePrefetch(0);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectDetach(true, true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            assertNull(consumer.receive(500));
            LOG.info("Receive returned");

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            LOG.info("Closing consumer");
            consumer.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropZeroPrefetchPullConsumerReceive() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.getPrefetchPolicy().setQueuePrefetch(0);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // --- Post Failover Expectations of FinalPeer --- //

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, 1, false, false, equalTo(UnsignedInteger.ONE), 1, true);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();
            finalPeer.expectDetach(true, true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            assertNotNull(consumer.receive());
            LOG.info("Receive returned");

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            LOG.info("Closing consumer");
            consumer.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropAfterQueueBrowserDrain() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymousConnect();
            originalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            originalPeer.expectBegin();
            originalPeer.expectQueueBrowserAttach();
            originalPeer.expectLinkFlow();
            originalPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymousConnect();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectQueueBrowserAttach();
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectDetach(true, true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();

            assertNotNull(queueView);
            assertFalse(queueView.hasMoreElements());

            browser.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateConsumerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateConsumerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test(timeout = 20000)
    public void testCreateConsumerFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateConsumerFailsWhenLinkRefusedTestImpl(true);
    }

    private void doCreateConsumerFailsWhenLinkRefusedTestImpl(boolean deferAttachResponseWrite) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymousConnect();
            testPeer.expectBegin();

            Connection connection = establishAnonymousConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            //Expect a link to a topic node, which we will then refuse
            SourceMatcher targetMatcher = new SourceMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectReceiverAttach(notNullValue(), targetMatcher, true, deferAttachResponseWrite);
            //Expect the detach response to the test peer closing the consumer link after refusal.
            testPeer.expectDetach(true, false, false);

            try {
                //Create a consumer, expect it to throw exception due to the link-refusal
                session.createConsumer(dest);
                fail("Consumer creation should have failed when link was refused");
            } catch(InvalidDestinationException ide) {
                LOG.info("Test caught expected error: {}", ide.getMessage());
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private JmsConnection establishAnonymousConnecton(TestAmqpPeer... peers) throws JMSException {
        return establishAnonymousConnecton(null, peers);
    }

    private JmsConnection establishAnonymousConnecton(String failoverParams, TestAmqpPeer... peers) throws JMSException {
        if(peers.length == 0) {
            throw new IllegalArgumentException("No test peers were given, at least 1 required");
        }

        String remoteURI = "failover:(";
        boolean first = true;
        for(TestAmqpPeer peer : peers) {
            if (!first) {
                remoteURI += ",";
            }
            remoteURI += createPeerURI(peer);
            first = false;
        }

        if (failoverParams == null) {
            remoteURI += ")?failover.maxReconnectAttempts=10";
        } else {
            remoteURI += ")?" + failoverParams;
        }

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        Connection connection = factory.createConnection();

        return (JmsConnection) connection;
    }

    private String createPeerURI(TestAmqpPeer peer) {
        return "amqp://localhost:" + peer.getServerPort();
    }
}
