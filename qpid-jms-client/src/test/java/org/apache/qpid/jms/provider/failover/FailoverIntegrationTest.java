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
package org.apache.qpid.jms.provider.failover;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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

import javax.jms.CompletionListener;
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
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsResourceNotFoundException;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.util.StopWatch;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.mockito.Mockito;
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

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesConnectErrorNotFound() throws Exception {

        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch finalConnected = new CountDownLatch(1);
            final String finalURI = createPeerURI(finalPeer);
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            originalPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(2000);

            assertNotNull(message);

            // Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesDropThenRejectionCloseAfterConnect() throws Exception {
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();

            long ird = 0;
            long rd = 2000;
            long start = System.currentTimeMillis();

            final JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=" + ird + "&failover.reconnectDelay=" + rd + "&failover.maxReconnectAttempts=10", originalPeer, rejectingPeer, finalPeer);
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
            rejectingPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            // Close the original peer and wait for things to shake out.
            originalPeer.close();

            rejectingPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));
            long end = System.currentTimeMillis();

            long margin = 2000;
            assertThat("Elapsed time outwith expected range for reconnect", end - start,
                    both(greaterThanOrEqualTo(ird + rd)).and(lessThanOrEqualTo(ird + rd + margin)));

            // Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            // Close the original peer and wait for things to shake out.
            originalPeer.close();

            rejectingPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            // Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesRemotelyEndConnectionForced() throws Exception {
        try (TestAmqpPeer forcingPeer = new TestAmqpPeer();
             TestAmqpPeer backupPeer = new TestAmqpPeer();) {

            final String forcingPeerURI = createPeerURI(forcingPeer);
            final String backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at {}: Backup peer is at: {}", forcingPeerURI, backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            forcingPeer.expectSaslAnonymous();
            forcingPeer.expectOpen();
            forcingPeer.expectBegin();
            forcingPeer.remotelyCloseConnection(true, ConnectionError.CONNECTION_FORCED, "Server is going away", 10);

            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen();
            backupPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(forcingPeer, backupPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (remoteURI.toString().equals(forcingPeerURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (remoteURI.toString().equals(backupPeerURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });
            connection.start();

            forcingPeer.waitForAllHandlersToComplete(3000);

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));
            assertTrue("Should connect to backup peer", connectedToBackup.await(5, TimeUnit.SECONDS));

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
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
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
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

            // Shut it down
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            // Shut it down
            finalPeer.expectClose();
            connection.close();

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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();

            int delay = 20000;
            StopWatch watch = new StopWatch();

            JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=" + delay + "&failover.maxReconnectAttempts=1", originalPeer);
            connection.start();

            long taken = watch.taken();

            String message = "Initial connect should not have delayed for the specified initialReconnectDelay." + "Elapsed=" + taken + ", delay=" + delay;
            assertTrue(message,  taken < delay);
            assertTrue("Connection took longer than reasonable: " + taken, taken < 5000);

            // Shut it down
            originalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.ONE));
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
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

            // Shut it down
            finalPeer.expectClose();
            connection.close();

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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
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

            // Shut it down
            finalPeer.expectClose();
            connection.close();

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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
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

            // Shut it down
            finalPeer.expectClose();
            connection.close();

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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
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
            originalPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectQueueBrowserAttach();
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            finalPeer.expectDetach(true, true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();

            assertNotNull(queueView);
            assertFalse(queueView.hasMoreElements());

            browser.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
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
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
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

            // Shut it down
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTxRecreatedAfterConnectionFailsOver() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
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
            originalPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            originalPeer.expectDeclare(txnId);

            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId);

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded.
            finalPeer.expectDischarge(txnId, true);
            finalPeer.expectEnd();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            session.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTempDestinationRecreatedAfterConnectionFailsOver() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            String dynamicAddress1 = "myTempTopicAddress";
            originalPeer.expectTempTopicCreationAttach(dynamicAddress1);

            originalPeer.dropAfterLastHandler();
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

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            String dynamicAddress2 = "myTempTopicAddress2";
            finalPeer.expectTempTopicCreationAttach(dynamicAddress2);

            // Session is recreated after previous temporary destinations are recreated on failover.
            finalPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            // Delete the temporary Topic and close the session.
            finalPeer.expectDetach(true, true, true);
            finalPeer.expectEnd();

            tempTopic.delete();

            session.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverEnforcesRequestTimeoutSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);

            // Create a peer to connect to so we can get to a state where we
            // can try to send when offline.
            final String peerURI = createPeerURI(testPeer);

            LOG.info("Original peer is at: {}", peerURI);

            // Connect to the test peer
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(
                "jms.requestTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60",
                testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onConnectionInterrupted(URI remoteURI) {
                    LOG.info("Connection Interrupted: {}", remoteURI);
                    disconnected.countDown();
                }

                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    connected.countDown();
                }
            });
            connection.start();

            assertTrue("Should connect to peer", connected.await(5, TimeUnit.SECONDS));
            assertTrue("Should lose connection to peer", disconnected.await(5, TimeUnit.SECONDS));

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Should have thrown an exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught timed out exception from send:", jmsEx);
            } catch (Exception ex) {
                fail("Should have caught a timed out exception");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverEnforcesSendTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);

            // Create a peer to connect to so we can get to a state where we
            // can try to send when offline.
            final String peerURI = createPeerURI(testPeer);

            LOG.info("Original peer is at: {}", peerURI);

            // Connect to the test peer
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(
                "jms.sendTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60",
                testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onConnectionInterrupted(URI remoteURI) {
                    LOG.info("Connection Interrupted: {}", remoteURI);
                    disconnected.countDown();
                }

                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    connected.countDown();
                }
            });
            connection.start();

            assertTrue("Should connect to peer", connected.await(5, TimeUnit.SECONDS));

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            assertTrue("Should lose connection to peer", disconnected.await(5, TimeUnit.SECONDS));

            try {
                producer.send(session.createMessage());
                fail("Should have thrown an exception");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught timed out exception from send:", jmsEx);
            } catch (Exception ex) {
                fail("Should have caught a timed out exception");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverEnforcesRequestTimeoutCreateTenpDestination() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);

            // Create a peer to connect to so we can get to a state where we
            // can try to send when offline.
            final String peerURI = createPeerURI(testPeer);

            LOG.info("Original peer is at: {}", peerURI);

            // Connect to the test peer
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(
                "jms.requestTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60",
                testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onConnectionInterrupted(URI remoteURI) {
                    LOG.info("Connection Interrupted: {}", remoteURI);
                    disconnected.countDown();
                }

                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    connected.countDown();
                }
            });
            connection.start();

            assertTrue("Should connect to peer", connected.await(5, TimeUnit.SECONDS));

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue("Should lose connection to peer", disconnected.await(5, TimeUnit.SECONDS));

            try {
                session.createTemporaryQueue();
                fail("Should have thrown an exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught timed out exception from send:", jmsEx);
            } catch (Exception ex) {
                fail("Should have caught a timed out exception");
            }

            try {
                session.createTemporaryTopic();
                fail("Should have thrown an exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught timed out exception from send:", jmsEx);
            } catch (Exception ex) {
                fail("Should have caught a timed out exception");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverPassthroughOfCompletedAsyncSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = establishAnonymousConnecton(
                "failover.reconnectDelay=2000&failover.maxReconnectAttempts=5", testPeer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(2000, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFalioverPassthroughOfRejectedAsyncCompletionSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final JmsConnection connection = establishAnonymousConnecton(
                "failover.reconnectDelay=2000&failover.maxReconnectAttempts=5", testPeer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("content");
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), false, new Rejected(), true);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(2000, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(2000, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverConnectionLossFailsWaitingAsyncCompletionSends() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final JmsConnection connection = establishAnonymousConnecton(
                "failover.reconnectDelay=2000&failover.maxReconnectAttempts=60",
                testPeer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            final int MSG_COUNT = 5;

            for (int i = 0; i < MSG_COUNT; ++i) {
                testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            }

            // Accept one which shouldn't complete until after the others have failed.
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), false, new Accepted(), true);
            testPeer.dropAfterLastHandler();

            TestJmsCompletionListener listener = new TestJmsCompletionListener(MSG_COUNT + 1);
            try {
                for (int i = 0; i < MSG_COUNT; ++i) {
                    Message message = session.createTextMessage("content");
                    producer.send(message, listener);
                }

                Message message = session.createTextMessage("content");
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(2000, TimeUnit.SECONDS));
            assertEquals(MSG_COUNT, listener.errorCount);
            assertEquals(1, listener.successCount);
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateSessionAfterConnectionDrops() throws Exception {
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin(nullValue(), false);
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectEnd();
            finalPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            session.close();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateConsumerAfterConnectionDrops() throws Exception {
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectDetach(true, true, true);
            finalPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            assertNull(consumer.receive(500));
            LOG.info("Receive returned");

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            LOG.info("Closing consumer");
            consumer.close();

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateProducerAfterConnectionDrops() throws Exception {
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
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
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

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectDetach(true, true, true);
            finalPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            LOG.info("Closing consumer");
            producer.close();

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTxCommitThrowsAfterMaxReconnectsWhenNoDischargeResponseSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            final CountDownLatch testConnected = new CountDownLatch(1);
            final CountDownLatch failedConnection = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String testPeerURI = createPeerURI(testPeer);

            LOG.info("test peer is at: {}", testPeerURI);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(
                "failover.maxReconnectAttempts=10&failover.useReconnectBackOff=false", testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (testPeerURI.equals(remoteURI.toString())) {
                        testConnected.countDown();
                    }
                }

                @Override
                public void onConnectionFailure(Throwable cause) {
                    LOG.info("Connection Failed: {}", cause);
                    failedConnection.countDown();
                }
            });
            connection.start();

            assertTrue("Should connect to test peer", testConnected.await(5, TimeUnit.SECONDS));

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // The session should send a commit but we don't respond, then drop the connection
            // and check that the commit is failed due to dropped connection.
            testPeer.expectDischargeButDoNotRespond(txnId, false);
            testPeer.expectDeclareButDoNotRespond();

            testPeer.remotelyCloseConnection(true, ConnectionError.CONNECTION_FORCED, "Server is going away", 100);

            // --- Failover should handle the connection close ---------------//

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.commit();
                fail("Commit should have thrown an exception");
            } catch (JMSException jmsEx) {
                LOG.debug("Commit threw: ", jmsEx);
            }

            assertTrue("Should reported failed", failedConnection.await(5, TimeUnit.SECONDS));

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testDropAndRejectAfterwardsHonorsMax() throws Exception {
        try (TestAmqpPeer firstPeer = new TestAmqpPeer();
             TestAmqpPeer secondPeer = new TestAmqpPeer();
             TestAmqpPeer thirdPeer = new TestAmqpPeer();
             TestAmqpPeer fourthPeer = new TestAmqpPeer()) {

            final CountDownLatch testConnected = new CountDownLatch(1);
            final CountDownLatch failedConnection = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String firstPeerURI = createPeerURI(firstPeer);

            LOG.info("First peer is at: {}", firstPeerURI);
            LOG.info("Second peer is at: {}", createPeerURI(secondPeer));
            LOG.info("Third peer is at: {}", createPeerURI(thirdPeer));
            LOG.info("Fourth peer is at: {}", createPeerURI(fourthPeer));

            firstPeer.expectSaslAnonymous();
            firstPeer.expectOpen();
            firstPeer.expectBegin();
            firstPeer.remotelyCloseConnection(true, ConnectionError.CONNECTION_FORCED, "Server is going away", 100);

            secondPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);
            thirdPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);

            // This shouldn't get hit, but if it does accept the connect so we don't pass the failed
            // to connect assertion.
            fourthPeer.expectSaslAnonymous();
            fourthPeer.expectOpen();
            fourthPeer.expectBegin();
            fourthPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(
                "failover.maxReconnectAttempts=2&failover.useReconnectBackOff=false", firstPeer, secondPeer, thirdPeer, fourthPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (firstPeerURI.equals(remoteURI.toString())) {
                        testConnected.countDown();
                    }
                }

                @Override
                public void onConnectionFailure(Throwable cause) {
                    LOG.info("Connection Failed: {}", cause);
                    failedConnection.countDown();
                }
            });
            connection.start();

            assertTrue("Should connect to first peer", testConnected.await(5, TimeUnit.SECONDS));

            // --- Failover should handle the connection close ---------------//

            assertTrue("Should reported failed", failedConnection.await(5, TimeUnit.SECONDS));

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            secondPeer.waitForAllHandlersToCompleteNoAssert(2000);
            thirdPeer.waitForAllHandlersToCompleteNoAssert(2000);

            // Shut down last peer and verify no connection made to it
            fourthPeer.purgeExpectations();
            fourthPeer.close();
            assertNotNull("Peer 1 should have accepted a TCP connection", firstPeer.getClientSocket());
            assertNotNull("Peer 2 should have accepted a TCP connection", secondPeer.getClientSocket());
            assertNotNull("Peer 3 should have accepted a TCP connection", thirdPeer.getClientSocket());
            assertNull("Peer 4 should not have accepted any TCP connection", fourthPeer.getClientSocket());
        }
    }

    @Test(timeout = 20000)
    public void testStartMaxReconnectAttemptsTriggeredWhenRemotesAreRejecting() throws Exception {
        try (TestAmqpPeer firstPeer = new TestAmqpPeer();
             TestAmqpPeer secondPeer = new TestAmqpPeer();
             TestAmqpPeer thirdPeer = new TestAmqpPeer();
             TestAmqpPeer fourthPeer = new TestAmqpPeer()) {

            final CountDownLatch failedConnection = new CountDownLatch(1);

            LOG.info("First peer is at: {}", createPeerURI(firstPeer));
            LOG.info("Second peer is at: {}", createPeerURI(secondPeer));
            LOG.info("Third peer is at: {}", createPeerURI(thirdPeer));
            LOG.info("Fourth peer is at: {}", createPeerURI(fourthPeer));

            firstPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);
            secondPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);
            thirdPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);

            // This shouldn't get hit, but if it does accept the connect so we don't pass the failed
            // to connect assertion.
            fourthPeer.expectSaslAnonymous();
            fourthPeer.expectOpen();
            fourthPeer.expectBegin();
            fourthPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(
                "failover.startupMaxReconnectAttempts=3&failover.reconnectDelay=15&failover.useReconnectBackOff=false",
                firstPeer, secondPeer, thirdPeer, fourthPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onConnectionFailure(Throwable cause) {
                    LOG.info("Connection Failed: {}", cause);
                    failedConnection.countDown();
                }
            });

            try {
                connection.start();
                fail("Should not be able to connect");
            } catch (JmsResourceNotFoundException jmsrnfe) {}

            // --- Failover should handle the connection close ---------------//

            assertTrue("Should reported failed", failedConnection.await(5, TimeUnit.SECONDS));

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            firstPeer.waitForAllHandlersToCompleteNoAssert(2000);
            secondPeer.waitForAllHandlersToCompleteNoAssert(2000);
            thirdPeer.waitForAllHandlersToCompleteNoAssert(2000);

            // Shut down last peer and verify no connection made to it
            fourthPeer.purgeExpectations();
            fourthPeer.close();
            assertNotNull("Peer 1 should have accepted a TCP connection", firstPeer.getClientSocket());
            assertNotNull("Peer 2 should have accepted a TCP connection", secondPeer.getClientSocket());
            assertNotNull("Peer 3 should have accepted a TCP connection", thirdPeer.getClientSocket());
            assertNull("Peer 4 should not have accepted any TCP connection", fourthPeer.getClientSocket());
        }
    }

    @Test(timeout = 20000)
    public void testConnectionConsumerRecreatedAfterReconnect() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            ServerSessionPool sessionPool = Mockito.mock(ServerSessionPool.class);

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

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

            Queue queue = new JmsQueue("myQueue");
            connection.createConnectionConsumer(queue, null, sessionPool, 100);

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow();
            finalPeer.expectClose();

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private JmsConnection establishAnonymousConnecton(TestAmqpPeer... peers) throws JMSException {
        return establishAnonymousConnecton(null, null, peers);
    }

    private JmsConnection establishAnonymousConnecton(String failoverParams, TestAmqpPeer... peers) throws JMSException {
        return establishAnonymousConnecton(null, failoverParams, peers);
    }

    private JmsConnection establishAnonymousConnecton(String connectionParams, String failoverParams, TestAmqpPeer... peers) throws JMSException {
        if (peers.length == 0) {
            throw new IllegalArgumentException("No test peers were given, at least 1 required");
        }

        String remoteURI = "failover:(";
        boolean first = true;
        for (TestAmqpPeer peer : peers) {
            if (!first) {
                remoteURI += ",";
            }
            remoteURI += createPeerURI(peer, connectionParams);
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
        return createPeerURI(peer, null);
    }

    private String createPeerURI(TestAmqpPeer peer, String params) {
        return "amqp://localhost:" + peer.getServerPort() + (params != null ? "?" + params : "");
    }

    private class TestJmsCompletionListener implements CompletionListener {

        private final CountDownLatch completed;

        public volatile int successCount;
        public volatile int errorCount;

        public volatile Message message;
        public volatile Exception exception;

        public TestJmsCompletionListener() {
            this(1);
        }

        public TestJmsCompletionListener(int expected) {
            this.completed = new CountDownLatch(expected);
        }

        public boolean awaitCompletion(long timeout, TimeUnit units) throws InterruptedException {
            return completed.await(timeout, units);
        }

        @Override
        public void onCompletion(Message message) {
            LOG.info("JmsCompletionListener onCompletion called with message: {}", message);
            this.message = message;
            this.successCount++;

            completed.countDown();
        }

        @Override
        public void onException(Message message, Exception exception) {
            LOG.info("JmsCompletionListener onException called with message: {} error {}", message, exception);

            this.message = message;
            this.exception = exception;
            this.errorCount++;

            completed.countDown();
        }
    }
}
