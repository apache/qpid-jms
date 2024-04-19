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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.ResourceAllocationException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsResourceNotFoundException;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.ListDescribedType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Modified;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransactionalState;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.util.StopWatch;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverIntegrationTest.class);

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final UnsignedByte SASL_FAIL_AUTH = UnsignedByte.valueOf((byte) 1);
    private static final UnsignedByte SASL_SYS = UnsignedByte.valueOf((byte) 2);
    private static final UnsignedByte SASL_SYS_PERM = UnsignedByte.valueOf((byte) 3);
    private static final UnsignedByte SASL_SYS_TEMP = UnsignedByte.valueOf((byte) 4);

    @Test
    @Timeout(20)
    public void testConnectThrowsSecurityViolationOnFailureFromOpen() throws Exception {
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

    @Test
    @Timeout(20)
    public void testConnectThrowsSecurityViolationOnFailureFromSaslWithClientID() throws Exception {
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_FAIL_AUTH);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_SYS);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_SYS_PERM);
    }

    @Test
    @Timeout(20)
    public void testConnectThrowsSecurityViolationOnFailureFromSaslExplicitlyWithoutClientID() throws Exception {
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_FAIL_AUTH);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_SYS);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_SYS_PERM);
    }

    private void doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(boolean clientID, UnsignedByte saslFailureCode) throws Exception {
        String optionString;
        if (clientID) {
            optionString = "?jms.clientID=myClientID";
        } else {
            optionString = "?jms.awaitClientID=false";
        }

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslFailingExchange(new Symbol[] {PLAIN, ANONYMOUS}, PLAIN, saslFailureCode);

            ConnectionFactory factory = new JmsConnectionFactory("failover:(amqp://localhost:" + testPeer.getServerPort() + ")" + optionString);

            try {
                factory.createConnection("username", "password");
                fail("Excepted exception to be thrown");
            }catch (JMSSecurityException jmsse) {
                LOG.info("Caught expected security exception: {}", jmsse.getMessage());
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConnectThrowsSecurityViolationOnFailureFromSaslImplicitlyWithoutClientID() throws Exception {
        doConnectThrowsSecurityViolationOnFailureFromSaslImplicitlyWithoutClientIDTestImpl(SASL_FAIL_AUTH);
        doConnectThrowsSecurityViolationOnFailureFromSaslImplicitlyWithoutClientIDTestImpl(SASL_SYS);
        doConnectThrowsSecurityViolationOnFailureFromSaslImplicitlyWithoutClientIDTestImpl(SASL_SYS_PERM);
    }

    private void doConnectThrowsSecurityViolationOnFailureFromSaslImplicitlyWithoutClientIDTestImpl(UnsignedByte saslFailureCode) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslFailingExchange(new Symbol[] {PLAIN, ANONYMOUS}, PLAIN, saslFailureCode);

            ConnectionFactory factory = new JmsConnectionFactory("failover:(amqp://localhost:" + testPeer.getServerPort() + ")");
            Connection connection = factory.createConnection("username", "password");

            try {
                connection.start();
                fail("Excepted exception to be thrown");
            }catch (JMSSecurityException jmsse) {
                LOG.info("Caught expected security exception: {}", jmsse.getMessage());
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConnectHandlesSaslTempFailure() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch finalConnected = new CountDownLatch(1);
            final String finalURI = createPeerURI(finalPeer);

            originalPeer.expectSaslFailingExchange(new Symbol[] { ANONYMOUS }, ANONYMOUS, SASL_SYS_TEMP);

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            String content = "myContent";
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(content);

            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(2000);

            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);

            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals(content, ((TextMessage) message).getText());
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverStopsOnNonTemporarySaslFailure() throws Exception {
        doFailoverStopsOnNonTemporarySaslFailureTestImpl(SASL_FAIL_AUTH);
        doFailoverStopsOnNonTemporarySaslFailureTestImpl(SASL_SYS);
        doFailoverStopsOnNonTemporarySaslFailureTestImpl(SASL_SYS_PERM);
    }

    private void doFailoverStopsOnNonTemporarySaslFailureTestImpl(UnsignedByte saslFailureCode) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            final AtomicReference<Throwable> failure = new AtomicReference<>();

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String rejectingURI = createPeerURI(rejectingPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Rejecting peer is at: {}", rejectingURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting Peer--- //
            rejectingPeer.expectSaslFailingExchange(new Symbol[] { ANONYMOUS }, ANONYMOUS, saslFailureCode);

            // --- Post Failover Expectations of FinalPeer --- //
            // This shouldn't get hit, but if it does accept the connect so we don't pass the failed
            // to connect assertion.
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    failure.compareAndSet(null, exception);
                    exceptionListenerFired.countDown();
                }
            });

            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }
            });
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");

            assertTrue(exceptionListenerFired.await(3, TimeUnit.SECONDS), "The ExceptionListener should have been alerted");
            Throwable ex = failure.get();
            assertTrue(ex instanceof JMSSecurityException, "Unexpected failure exception: " + ex);

            // Verify the consumer gets marked closed
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageSelector();
                    } catch (IllegalStateException jmsise) {
                        return true;
                    }
                    return false;
                }
            }, 5000, 5), "consumer never closed.");

            // Shut down last peer and verify no connection made to it
            finalPeer.purgeExpectations();
            finalPeer.close();
            assertNotNull(originalPeer.getClientSocket(), "First peer should have accepted a TCP connection");
            assertNotNull(rejectingPeer.getClientSocket(), "Rejecting peer should have accepted a TCP connection");
            assertNull(finalPeer.getClientSocket(), "Final peer should not have accepted any TCP connection");
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesTemporarySaslFailure() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final AtomicBoolean exceptionListenerFired = new AtomicBoolean();

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String rejectingURI = createPeerURI(rejectingPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Rejecting peer is at: {}", rejectingURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting --- //
            rejectingPeer.expectSaslFailingExchange(new Symbol[] { ANONYMOUS }, ANONYMOUS, SASL_SYS_TEMP);

            // --- Post Failover Expectations of FinalPeer --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            final String expectedMessageContent = "myTextMessage";
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(expectedMessageContent));

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.set(true);
                }
            });

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

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);

            finalPeer.waitForAllHandlersToComplete(2000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // Check message arrives
            finalPeer.expectDispositionThatIsAcceptedAndSettled();

            Message msg = consumer.receive(5000);
            assertTrue(msg instanceof TextMessage, "Expected an instance of TextMessage, got: " + msg);
            assertEquals(expectedMessageContent, ((TextMessage) msg).getText(), "Unexpected msg content");

            assertFalse(exceptionListenerFired.get(), "The ExceptionListener should not have been alerted");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesConnectErrorInvalidField() throws Exception {
        doFailoverHandlesConnectErrorInvalidFieldTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesConnectErrorInvalidFieldWithContainerIdHint() throws Exception {
        // As above but also including hint that the container-id is the invalid field, i.e invalid ClientID
        doFailoverHandlesConnectErrorInvalidFieldTestImpl(true);
    }

    private void doFailoverHandlesConnectErrorInvalidFieldTestImpl(boolean includeContainerIdHint) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch finalConnected = new CountDownLatch(1);
            final String finalURI = createPeerURI(finalPeer);
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            Map<Symbol, Object> errorInfo = null;
            if (includeContainerIdHint) {
                errorInfo = new HashMap<Symbol, Object>();
                errorInfo.put(AmqpSupport.INVALID_FIELD, AmqpSupport.CONTAINER_ID);
            }

            originalPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", errorInfo);

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

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

    @Test
    @Timeout(20)
    public void testFailoverHandlesConnectErrorInvalidFieldOnReconnect() throws Exception {
        doFailoverHandlesConnectErrorInvalidFieldOnReconnectTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesConnectErrorInvalidFieldOnReconnectWithContainerIdHint() throws Exception {
        // As above but also including hint that the container-id is the invalid field, i.e invalid ClientID
        doFailoverHandlesConnectErrorInvalidFieldOnReconnectTestImpl(true);
    }

    private void doFailoverHandlesConnectErrorInvalidFieldOnReconnectTestImpl(boolean includeContainerIdHint) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch finalConnected = new CountDownLatch(1);
            final String finalURI = createPeerURI(finalPeer);
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler(10);

            Map<Symbol, Object> errorInfo = null;
            if (includeContainerIdHint) {
                errorInfo = new HashMap<Symbol, Object>();
                errorInfo.put(AmqpSupport.INVALID_FIELD, AmqpSupport.CONTAINER_ID);
            }
            rejectingPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", errorInfo);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionRestored(URI remoteURI) {
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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

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

    @Test
    @Timeout(20)
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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

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

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");
            assertEquals(1L, finalConnected.getCount(), "should not yet have connected to final peer");

            // Set expectations on rejecting and final peer
            rejectingPeer.rejectConnect(AmqpError.NOT_FOUND, "Resource could not be located", null);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            // Close the original peer and wait for things to shake out.
            originalPeer.close();

            rejectingPeer.waitForAllHandlersToComplete(2000);

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");
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

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");
            assertEquals(1L, finalConnected.getCount(), "should not yet have connected to final peer");

            // Set expectations on rejecting and final peer
            rejectingPeer.expectSaslHeaderThenDrop();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();

            // Close the original peer and wait for things to shake out.
            originalPeer.close();

            rejectingPeer.waitForAllHandlersToComplete(2000);

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            // Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(connectedToPrimary.await(5, TimeUnit.SECONDS), "Should connect to primary peer");
            assertTrue(connectedToBackup.await(5, TimeUnit.SECONDS), "Should connect to backup peer");

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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
                    } finally {
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

            assertEquals(1L, finalConnected.getCount(), "Should not yet have connected to final peer");
            assertEquals(1L, senderCompleted.getCount(), "Sender thread should not yet have completed");

            // Close the original peer to provoke reconnect, while send() is still outstanding
            originalPeer.close();

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            boolean await = senderCompleted.await(5, TimeUnit.SECONDS);
            Throwable t = problem.get();
            assertTrue(await, "Sender thread should have completed. Problem: " + t);

            // Shut it down
            finalPeer.expectClose();
            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesRemoteCloseBeforeDispositionRecieived() throws Exception {
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
            originalPeer.expectSenderAttach();

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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

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
            originalPeer.remotelyCloseConnection(true, ConnectionError.CONNECTION_FORCED, "Server is going away", 5);

            Thread runner = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        producer.send(message);
                    } catch (Throwable t) {
                        problem.set(t);
                        LOG.error("Problem in sending thread", t);
                    } finally {
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
            finalPeer.expectClose();

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            boolean await = senderCompleted.await(5, TimeUnit.SECONDS);
            Throwable t = problem.get();
            assertTrue(await, "Sender thread should have completed. Problem: " + t);

            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();

            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(30)
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
            assertTrue(taken < delay,  message);
            assertTrue(taken < 5000, "Connection took longer than reasonable: " + taken);

            // Shut it down
            originalPeer.expectClose();
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            consumer.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            LOG.info("Closing consumer");
            consumer.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            LOG.info("Closing consumer");
            consumer.close();

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

    @Test
    @Timeout(20)
    public void testFailoverHandlesDropAfterSessionCloseRequested() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer()) {

            final CountDownLatch originalConnected = new CountDownLatch(1);

            final String originalURI = createPeerURI(originalPeer);

            LOG.info("Original peer is at: {}", originalURI);

            // Connect to the first peer
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            originalPeer.expectBegin();
            originalPeer.expectEnd(false);
            originalPeer.dropAfterLastHandler();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            final CountDownLatch sessionCloseCompleted = new CountDownLatch(1);
            final AtomicBoolean sessionClosedThrew = new AtomicBoolean();
            Thread sessionCloseThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        session.close();
                        LOG.debug("Close of session returned ok");
                    } catch (JMSException jmsEx) {
                        LOG.warn("Should not throw on session close when connection drops.", jmsEx);
                        sessionClosedThrew.set(true);
                    } finally {
                        sessionCloseCompleted.countDown();
                    }
                }
            }, "Session close thread");

            sessionCloseThread.start();

            originalPeer.waitForAllHandlersToComplete(2000);

            assertTrue(sessionCloseCompleted.await(3, TimeUnit.SECONDS), "Session close should have completed by now");
            assertFalse(sessionClosedThrew.get(), "Session close should have completed normally");

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateConsumerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test
    @Timeout(20)
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
            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(topicName));
            sourceMatcher.withDynamic(equalTo(false));
            sourceMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher, true, deferAttachResponseWrite);
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

    @Test
    @Timeout(20)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(true);
    }

    private void doCreateProducerFailsWhenLinkRefusedTestImpl(boolean deferAttachResponseWrite) throws Exception {
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

            // Expect a link to a topic node, which we will then refuse
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectSenderAttach(notNullValue(), targetMatcher, true, deferAttachResponseWrite);
            // Expect the detach response to the test peer closing the producer link after refusal.
            testPeer.expectDetach(true, false, false);

            try {
                // Create a producer, expect it to throw exception due to the link-refusal
                session.createProducer(dest);
                fail("Producer creation should have failed when link was refused");
            } catch(InvalidDestinationException ide) {
                LOG.info("Test caught expected error: {}", ide.getMessage());
            }

            // Shut it down
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTxRecreatedAfterConnectionFailsOverDropsAfterCoordinatorAttach() throws Exception {
        doTxRecreatedAfterConnectionFailsOver(true);
    }

    @Test
    @Timeout(20)
    public void testTxRecreatedAfterConnectionFailsOverDropsAfterSessionBegin() throws Exception {
        doTxRecreatedAfterConnectionFailsOver(false);
    }

    private void doTxRecreatedAfterConnectionFailsOver(boolean dropAfterCoordinator) throws Exception {
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            originalPeer.expectBegin();

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            if (dropAfterCoordinator) {
                originalPeer.expectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.
                originalPeer.expectDeclare(txnId1);
            }

            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded.
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectEnd();
            finalPeer.expectClose();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            LOG.debug("About to close session following final peer connection.");
            session.close();
            LOG.debug("About to close connection following final peer connection.");
            connection.close();

            originalPeer.waitForAllHandlersToComplete(2000);
            finalPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

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

    @Test
    @Timeout(20)
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

            assertTrue(connected.await(5, TimeUnit.SECONDS), "Should connect to peer");
            assertTrue(disconnected.await(5, TimeUnit.SECONDS), "Should lose connection to peer");

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

    @Test
    @Timeout(20)
    public void testFailoverEnforcesRequestTimeoutSessionWhenBeginSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            // Create a peer to connect to so we can get to a state where we
            // can try to send when offline.
            final String peerURI = createPeerURI(testPeer);

            LOG.info("Original peer is at: {}", peerURI);

            // Connect to the test peer
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin(false);
            testPeer.dropAfterLastHandler();

            final JmsConnection connection = establishAnonymousConnecton(
                "jms.requestTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=30", testPeer);
            connection.start();

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

    @Test
    @Timeout(20)
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

            assertTrue(connected.await(5, TimeUnit.SECONDS), "Should connect to peer");

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            assertTrue(disconnected.await(5, TimeUnit.SECONDS), "Should lose connection to peer");

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

    @Test
    @Timeout(20)
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

            assertTrue(connected.await(5, TimeUnit.SECONDS), "Should connect to peer");

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue(disconnected.await(5, TimeUnit.SECONDS), "Should lose connection to peer");

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

    @Test
    @Timeout(20)
    public void testFailoverPassthroughOfCompletedSyncSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = establishAnonymousConnecton(testPeer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            //Do a warmup
            String messageContent1 = "myMessage1";
            TransferPayloadCompositeMatcher messageMatcher1 = new TransferPayloadCompositeMatcher();
            messageMatcher1.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher1.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher1.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher1.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent1));

            testPeer.expectTransfer(messageMatcher1);

            TextMessage message1 = session.createTextMessage(messageContent1);
            producer.send(message1);

            testPeer.waitForAllHandlersToComplete(1000);

            // Create and send a new message, which is accepted
            String messageContent2 = "myMessage2";
            long delay = 15;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent2));

            testPeer.expectTransfer(messageMatcher, nullValue(), false, true, new Accepted(), true, 0, delay);
            testPeer.expectClose();

            TextMessage message2 = session.createTextMessage(messageContent2);

            long start = System.currentTimeMillis();
            producer.send(message2);

            long elapsed = System.currentTimeMillis() - start;
            assertThat("Send call should have taken at least the disposition delay", elapsed, Matchers.greaterThanOrEqualTo(delay));

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverPassthroughOfRejectedSyncSend() throws Exception {
        Rejected failingState = new Rejected();
        org.apache.qpid.jms.test.testpeer.describedtypes.Error rejectError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
        rejectError.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
        rejectError.setDescription("RLE description");
        failingState.setError(rejectError);

        doFailoverPassthroughOfFailingSyncSendTestImpl(failingState, true);
    }

    @Test
    @Timeout(20)
    public void testFailoverPassthroughOfReleasedSyncSend() throws Exception {
        doFailoverPassthroughOfFailingSyncSendTestImpl(new Released(), false);
    }

    @Test
    @Timeout(20)
    public void testFailoverPassthroughOfModifiedFailedSyncSend() throws Exception {
        Modified failingState = new Modified();
        failingState.setDeliveryFailed(true);

        doFailoverPassthroughOfFailingSyncSendTestImpl(failingState, false);
    }

    private void doFailoverPassthroughOfFailingSyncSendTestImpl(ListDescribedType failingState, boolean inspectException) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = establishAnonymousConnecton(testPeer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            //Do a warmup that succeeds
            String messageContent1 = "myMessage1";
            TransferPayloadCompositeMatcher messageMatcher1 = new TransferPayloadCompositeMatcher();
            messageMatcher1.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher1.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher1.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher1.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent1));

            testPeer.expectTransfer(messageMatcher1);

            TextMessage message1 = session.createTextMessage(messageContent1);
            producer.send(message1);

            testPeer.waitForAllHandlersToComplete(1000);

            // Create and send a new message, which fails as it is not accepted
            assertFalse(failingState instanceof Accepted);

            String messageContent2 = "myMessage2";
            TransferPayloadCompositeMatcher messageMatcher2 = new TransferPayloadCompositeMatcher();
            messageMatcher2.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher2.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher2.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher2.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent2));

            long delay = 15;
            testPeer.expectTransfer(messageMatcher2, nullValue(), false, true, failingState, true, 0, delay);

            TextMessage message2 = session.createTextMessage(messageContent2);

            long start = System.currentTimeMillis();
            try {
                producer.send(message2);
                fail("Expected an exception for this send.");
            } catch (JMSException jmse) {
                //Expected
                long elapsed = System.currentTimeMillis() - start;
                assertThat("Send call should have taken at least the disposition delay", elapsed, Matchers.greaterThanOrEqualTo(delay));

                if (inspectException) {
                    assertTrue(jmse instanceof ResourceAllocationException);
                    assertTrue(jmse.getMessage().contains("RLE description"));
                }
            }

            testPeer.waitForAllHandlersToComplete(1000);

            //Do a final send that succeeds
            String messageContent3 = "myMessage3";
            TransferPayloadCompositeMatcher messageMatcher3 = new TransferPayloadCompositeMatcher();
            messageMatcher3.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher3.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher3.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher3.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent3));

            testPeer.expectTransfer(messageMatcher3);

            TextMessage message3 = session.createTextMessage(messageContent3);
            producer.send(message3);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(listener.awaitCompletion(2000, TimeUnit.SECONDS), "Did not get async callback");
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverPassthroughOfRejectedAsyncCompletionSend() throws Exception {
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
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), new Rejected(), true);

            assertNull(message.getJMSDestination(), "Should not yet have a JMSDestination");

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue(listener.awaitCompletion(2000, TimeUnit.SECONDS), "Did not get async callback");
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

            assertTrue(listener.awaitCompletion(2000, TimeUnit.SECONDS), "Did not get async callback");
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
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
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), new Accepted(), true);
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

            assertTrue(listener.awaitCompletion(2000, TimeUnit.SECONDS), "Did not get async callback");
            assertEquals(MSG_COUNT, listener.errorCount);
            assertEquals(1, listener.successCount);
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectEnd();
            finalPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            session.close();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            LOG.info("Closing consumer");
            consumer.close();

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

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

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            LOG.info("Closing consumer");
            producer.close();

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTxCommitThrowsWhenNoDischargeResponseSentAndConnectionDrops() throws Exception {
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
                "failover.maxReconnectAttempts=3&failover.useReconnectBackOff=false", testPeer);
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

            assertTrue(testConnected.await(6, TimeUnit.SECONDS), "Should connect to test peer");

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

            assertTrue(failedConnection.await(5, TimeUnit.SECONDS), "Should reported failed");

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(testConnected.await(5, TimeUnit.SECONDS), "Should connect to first peer");

            // --- Failover should handle the connection close ---------------//

            assertTrue(failedConnection.await(5, TimeUnit.SECONDS), "Should reported failed");

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            secondPeer.waitForAllHandlersToCompleteNoAssert(2000);
            thirdPeer.waitForAllHandlersToCompleteNoAssert(2000);

            // Shut down last peer and verify no connection made to it
            fourthPeer.purgeExpectations();
            fourthPeer.close();
            assertNotNull(firstPeer.getClientSocket(), "Peer 1 should have accepted a TCP connection");
            assertNotNull(secondPeer.getClientSocket(), "Peer 2 should have accepted a TCP connection");
            assertNotNull(thirdPeer.getClientSocket(), "Peer 3 should have accepted a TCP connection");
            assertNull(fourthPeer.getClientSocket(), "Peer 4 should not have accepted any TCP connection");
        }
    }

    @Test
    @Timeout(20)
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

            assertTrue(failedConnection.await(5, TimeUnit.SECONDS), "Should reported failed");

            try {
                connection.close();
            } catch (JMSException jmsEx) {}

            firstPeer.waitForAllHandlersToCompleteNoAssert(2000);
            secondPeer.waitForAllHandlersToCompleteNoAssert(2000);
            thirdPeer.waitForAllHandlersToCompleteNoAssert(2000);

            // Shut down last peer and verify no connection made to it
            fourthPeer.purgeExpectations();
            fourthPeer.close();
            assertNotNull(firstPeer.getClientSocket(), "Peer 1 should have accepted a TCP connection");
            assertNotNull(secondPeer.getClientSocket(), "Peer 2 should have accepted a TCP connection");
            assertNotNull(thirdPeer.getClientSocket(), "Peer 3 should have accepted a TCP connection");
            assertNull(fourthPeer.getClientSocket(), "Peer 4 should not have accepted any TCP connection");
        }
    }

    @Test
    @Timeout(20)
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

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of FinalPeer --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow();

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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListener() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListener";

        doRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListenerTestImpl(errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testRemotelyCloseConsumerWithMessageListenerWithoutErrorFiresJMSExceptionListener() throws Exception {
        // As above but with the peer not including any error condition in its consumer close
        doRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListenerTestImpl(null, null);
    }

    private void doRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListenerTestImpl(Symbol errorCondition, String errorDescription) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            CountDownLatch consumerClosed = new CountDownLatch(1);
            CountDownLatch exceptionListenerFired = new CountDownLatch(1);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            final JmsConnection connection = establishAnonymousConnecton("failover.maxReconnectAttempts=1", testPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.countDown();
                }
            });
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConsumerClosed(MessageConsumer consumer, Throwable exception) {
                    consumerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectBegin();
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session2.createQueue("myQueue");

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectEnd();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, errorCondition, errorDescription, 10);

            final MessageConsumer consumer = session2.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                }
            });

            // Close first session to allow the receiver remote close timing to be deterministic
            session1.close();

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            if (errorCondition != null) {
                                return message.contains(errorCondition.toString()) && message.contains(errorDescription);
                            } else {
                                return message.contains("Unknown error from remote peer");
                            }
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 5000, 10), "consumer never closed.");

            assertTrue(consumerClosed.await(2000, TimeUnit.MILLISECONDS),  "Consumer closed callback didn't trigger");
            assertTrue(exceptionListenerFired.await(2000, TimeUnit.MILLISECONDS), "JMS Exception listener should have fired with a MessageListener");

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();

            // Shut the connection down
            testPeer.expectClose();
            connection.close();
            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerFailsConnectionAndRetries() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerFailsConnectionAndRetries";

        doTestFailoverCannotRecreateConsumerFailsConnectionAndRetries(errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerFailsConnectionAndRetriesNoErrorConditionGiven() throws Exception {
        doTestFailoverCannotRecreateConsumerFailsConnectionAndRetries(null, null);
    }

    private void doTestFailoverCannotRecreateConsumerFailsConnectionAndRetries(Symbol errorCondition, String errorMessage) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final AtomicBoolean exceptionListenerFired = new AtomicBoolean();

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String rejectingURI = createPeerURI(rejectingPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Rejecting peer is at: {}", rejectingURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting --- //
            rejectingPeer.expectSaslAnonymous();
            rejectingPeer.expectOpen();
            rejectingPeer.expectBegin();
            rejectingPeer.expectBegin();
            rejectingPeer.expectReceiverAttach(notNullValue(), notNullValue(), false, true, false, false, errorCondition, errorMessage);
            // --- Client will clean up connection and then reconnect to next peer --- //
            rejectingPeer.expectDetach(true, false, false);
            rejectingPeer.expectClose();

            // --- Post Failover Expectations of FinalPeer --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            final String expectedMessageContent = "myTextMessage";
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(expectedMessageContent));
            finalPeer.expectDispositionThatIsAcceptedAndSettled();

            AtomicReference<Message> msgRef = new AtomicReference<>();
            final CountDownLatch msgReceived = new CountDownLatch(1);

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.set(true);
                }
            });

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

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    msgRef.set(message);
                    msgReceived.countDown();
                }
            });

            finalPeer.waitForAllHandlersToComplete(1000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // Check message arrives
            assertTrue(msgReceived.await(3, TimeUnit.SECONDS), "The onMessage listener should have fired");
            Message msg = msgRef.get();
            assertTrue(msg instanceof TextMessage, "Expected an instance of TextMessage, got: " + msg);
            assertEquals(expectedMessageContent, ((TextMessage) msg).getText(), "Unexpected msg content");

            // Check that consumer isn't closed
            try {
                consumer.getMessageListener();
            } catch (JMSException ex) {
                fail("Consumer should be in open state and not throw here.");
            }

            assertFalse(exceptionListenerFired.get(), "The ExceptionListener should not have been alerted");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateProducerFailsConnectionAndRetries() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateProducerFailsConnectionAndRetries";

        doTestFailoverCannotRecreateProducerFailsConnectionAndRetries(errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateProducerFailsConnectionAndRetriesNoErrorConditionGiven() throws Exception {
        doTestFailoverCannotRecreateProducerFailsConnectionAndRetries(null, null);
    }

    private void doTestFailoverCannotRecreateProducerFailsConnectionAndRetries(Symbol errorCondition, String errorMessage) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final AtomicBoolean exceptionListenerFired = new AtomicBoolean();

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String rejectingURI = createPeerURI(rejectingPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Rejecting peer is at: {}", rejectingURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting --- //
            rejectingPeer.expectSaslAnonymous();
            rejectingPeer.expectOpen();
            rejectingPeer.expectBegin();
            rejectingPeer.expectBegin();
            rejectingPeer.expectSenderAttach(notNullValue(), notNullValue(), true, false, false, -1, errorCondition, errorMessage);
            // --- Client will clean up connection and then reconnect to next peer --- //
            rejectingPeer.expectDetach(true, false, false);
            rejectingPeer.expectClose();

            // --- Post Failover Expectations of FinalPeer --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, rejectingPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.set(true);
                }
            });

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

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            finalPeer.waitForAllHandlersToComplete(1000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // Check that producer isn't closed
            try {
                producer.getDestination();
            } catch (JMSException ex) {
                fail("Producer should be in open state and not throw here.");
            }

            // Send a message
            String messageContent = "myMessage";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(messageContent));
            finalPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage(messageContent);
            producer.send(message);

            assertFalse(exceptionListenerFired.get(), "The ExceptionListener should not have been alerted");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled";

        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(true, errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoMessageListener() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoMessageListener";

        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(false, errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoErrorConditionGiven() throws Exception {
        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(true, null, null);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoErrorConditionGivenNoMessageListener() throws Exception {
        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(false, null, null);
    }

    private void doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(boolean addListener, Symbol errorCondition, String errorDescription) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final CountDownLatch exceptionListenerFired = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlow();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach(notNullValue(), notNullValue(), false, true, false, false, errorCondition, errorDescription);
            finalPeer.expectDetach(true, false, false);

            final int prefetch;
            if (addListener) {
                prefetch = 0;
            } else {
                prefetch = 1;
            }

            final JmsConnection connection = establishAnonymousConnecton(
                "jms.prefetchPolicy.all="+ prefetch + "&jms.closeLinksThatFailOnReconnect=true", originalPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.countDown();
                }
            });

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

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);
            if (addListener) {
                consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                    }
                });
            }

            finalPeer.waitForAllHandlersToComplete(1000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // Verify the consumer gets marked closed
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            if (errorCondition != null) {
                                return message.contains(errorCondition.toString()) &&
                                        message.contains(errorDescription);
                            } else {
                                return message.contains("Link creation was refused");
                            }
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 5000, 10), "consumer never closed.");

            // Verify the exception listener behaviour
            if (addListener) {
                assertTrue(exceptionListenerFired.await(2, TimeUnit.SECONDS), "JMS Exception listener should have fired with a MessageListener");
            } else {
                assertFalse(exceptionListenerFired.await(10, TimeUnit.MILLISECONDS), "The ExceptionListener should not have been alerted");
            }

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateProducerWithCloseFailedLinksEnabled() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateProducerWithCloseFailedLinksEnabled";

        doTestFailoverCannotRecreateWithCloseFailedLinksEnabled(errorCondition, errorDescription);
    }

    @Test
    @Timeout(20)
    public void testFailoverCannotRecreateProducerWithCloseFailedLinksEnabledNoErrorConditionGiven() throws Exception {
        doTestFailoverCannotRecreateWithCloseFailedLinksEnabled(null, null);
    }

    private void doTestFailoverCannotRecreateWithCloseFailedLinksEnabled(Symbol errorCondition, String errorDescription) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final AtomicBoolean exceptionListenerFired = new AtomicBoolean();

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            // Expect connection to the first peer (and have it drop)
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of Rejecting --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach(notNullValue(), notNullValue(), true, false, false, -1, errorCondition, errorDescription);
            finalPeer.expectDetach(true, false, false);

            final JmsConnection connection = establishAnonymousConnecton("jms.closeLinksThatFailOnReconnect=true", originalPeer, finalPeer);
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.trace("JMS ExceptionListener: ", exception);
                    exceptionListenerFired.set(true);
                }
            });

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

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            finalPeer.waitForAllHandlersToComplete(1000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // Verify the producer gets marked closed
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        producer.getDestination();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            if (errorCondition != null) {
                                return message.contains(errorCondition.toString()) &&
                                        message.contains(errorDescription);
                            } else {
                                return message.contains("Link creation was refused");
                            }
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 5000, 10), "producer never closed.");

            assertFalse(exceptionListenerFired.get(), "The ExceptionListener should not have been alerted");

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testInDoubtTransactionFromFailoverCompletesAsyncCompletions() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId1));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId1);
            txState.setOutcome(new Accepted());

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectSenderAttach();
            originalPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            originalPeer.dropAfterLastHandler(10);

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectSenderAttach();
            // Attempt to commit the in-doubt TX will result in rollback and a new TX will be started.
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectDeclare(txnId1);
            // this rollback comes from the session being closed on connection close.
            finalPeer.expectDischarge(txnId1, true);
            finalPeer.expectClose();

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

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener1 = new TestJmsCompletionListener();
            TestJmsCompletionListener listener2 = new TestJmsCompletionListener();

            try {
                producer.send(message, listener1);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            // This should fire after reconnect without an error, if it fires with an error at
            // any time then something is wrong.
            assertTrue(listener1.awaitCompletion(5, TimeUnit.SECONDS), "Did not get async callback for send #1");
            assertNull(listener1.exception, "Completion of send #1 should not have been on error");
            assertNotNull(listener1.message);
            assertTrue(listener1.message instanceof TextMessage);

            try {
                producer.send(message, listener2);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            assertTrue(listener2.awaitCompletion(5, TimeUnit.SECONDS), "Did not get async callback for send #2");
            assertNull(listener2.exception, "Completion of send #2 should not have been on error");
            assertNotNull(listener2.message);
            assertTrue(listener2.message instanceof TextMessage);

            try {
                session.commit();
                fail("Transaction should have been rolled back");
            } catch (TransactionRolledBackException txrbex) {}

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendAndConnectionDropsRecoveredAsInDoubtTransaction() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            // Send should occurs within transaction #1
            TransactionalStateMatcher txn1StateMatcher = new TransactionalStateMatcher();
            txn1StateMatcher.withTxnId(equalTo(txnId1));
            txn1StateMatcher.withOutcome(nullValue());

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectSenderAttach();
            // Send is synchronous so we don't respond in order to stall the MessageProducer
            // in the send call to block recovery from initiating a new transaction which
            // will then cause the current transaction to become in-doubt and commit should
            // throw a transaction rolled back exception.
            originalPeer.expectTransfer(messageMatcher, txn1StateMatcher, false, false, null, false);
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            // Send will be blocked waiting to fire so it will not be filtered by the local
            // transaction context in-doubt checks since there was no other work pending the
            // transaction will be fully recovered so the fixed producer must ensure that no
            // send occurs outside the transaction boundaries.
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.setForceSyncSend(true);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed to send.");
            }

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            try {
                session.commit();
                fail("Transaction should throw rolled back error as an operation is pending on recover.");
            } catch (TransactionRolledBackException txrbex) {
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSecondSendAndConnectionDropsResendsButTransactionRollsBackAsInDoubt() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            // Send should occurs within transaction #1
            TransactionalStateMatcher txn1StateMatcher = new TransactionalStateMatcher();
            txn1StateMatcher.withTxnId(equalTo(txnId1));
            txn1StateMatcher.withOutcome(nullValue());

            // Disposition should occurs within transaction #1 before failover
            TransactionalState txn1Disposition = new TransactionalState();
            txn1Disposition.setTxnId(txnId2);
            txn1Disposition.setOutcome(new Accepted());

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectSenderAttach();
            originalPeer.expectTransfer(messageMatcher, txn1StateMatcher, txn1Disposition, true);
            // Send is synchronous so we don't respond in order to stall the MessageProducer
            // in the send call to block recovery from initiating a new transaction which
            // will then cause the current transaction to become in-doubt and commit should
            // throw a transaction rolled back exception.
            originalPeer.expectTransfer(messageMatcher, txn1StateMatcher, false, false, null, false);
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            // Send will be blocked waiting to fire so it will not be filtered
            // by the local transaction context in-doubt checks, however since there
            // was pending transactional work the transaction will be rolled back
            // on commit and then a new transaction will be activated.  The producer
            // will filter the held send as there is no active transaction.
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.setForceSyncSend(true);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed to send.");
            }

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed to send.");
            }

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            try {
                session.commit();
                fail("Transaction should have been been in-doubt and a rolled back error thrown.");
            } catch (TransactionRolledBackException txrbex) {
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionalAcknowledgeAfterRecoveredWhileSendBlocked() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType("myContent");

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            // The initial send should occur in transaction #1
            TransactionalStateMatcher transfer1StateMatcher = new TransactionalStateMatcher();
            transfer1StateMatcher.withTxnId(equalTo(txnId1));
            transfer1StateMatcher.withOutcome(nullValue());

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectSenderAttach();
            // Send is synchronous so we don't respond in order to stall the MessageProducer
            // in the send call to block recovery from initiating a new transaction which
            // will then cause the current transaction to become in-doubt and commit should
            // throw a transaction rolled back exception.
            originalPeer.expectTransfer(messageMatcher, transfer1StateMatcher, false, false, null, false);
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            // Send will be blocked waiting to fire so it will not be filtered
            // by the local transaction context in-doubt checks, however since there
            // was pending transactional work the transaction will be rolled back.
            // The AmqpFixedProducer should filter the send after reconnect as there
            // won't be an active transaction coordinator until we start a new TXN.
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.setForceSyncSend(true);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the send after connection dropped.");
            }

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(5000));

            try {
                session.commit();
                fail("Transaction should have been rolled back");
            } catch (TransactionRolledBackException txrbex) {
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(30)
    public void testReceiveAndSendInTransactionFailsCommitWhenConnectionDropsDuringSend() throws Exception {
        final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
        final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

        final TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
        messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
        messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

        // The initial send before failover should arrive in transaction #1
        final TransactionalStateMatcher transfer1StateMatcher = new TransactionalStateMatcher();
        transfer1StateMatcher.withTxnId(equalTo(txnId1));
        transfer1StateMatcher.withOutcome(nullValue());

        // Transactional Acknowledge should happen before the failover then no others should arrive.
        final TransactionalStateMatcher dispositionStateMatcher = new TransactionalStateMatcher();
        dispositionStateMatcher.withTxnId(equalTo(txnId1));
        dispositionStateMatcher.withOutcome(new AcceptedMatcher());

        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
            TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);
            final CountDownLatch transactionRollback = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectReceiverAttach();
            originalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            originalPeer.expectDisposition(true, dispositionStateMatcher);
            originalPeer.expectSenderAttach();
            originalPeer.expectTransfer(messageMatcher, transfer1StateMatcher, false, false, null, false);
            originalPeer.dropAfterLastHandler();

            // Following failover the blocked send will be retried but the transaction will have
            // been marked as in-dbout by the context and no new transactional work will be done
            // until the commit is called and it throws a transaction rolled back error.
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.setForceSyncSend(true);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener((message) -> {
                try {
                    session.createProducer(queue).send(session.createTextMessage("sample"));
                    session.commit();
                } catch (TransactionRolledBackException txnRbEx) {
                    transactionRollback.countDown();
                } catch (JMSException jmsEx) {
                    throw new RuntimeException("Behaving badly since commit already did", jmsEx);
                }
            });

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            finalPeer.waitForAllHandlersToComplete(1000);
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectClose();

            assertTrue(transactionRollback.await(5, TimeUnit.SECONDS), "Should have encounted a Transaction Rollback Error");

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionDeclareWithNoResponseRecoveredAsInDoubtAndCommitFails() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType("myContent");

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            // The initial send should occur in transaction #1
            TransactionalStateMatcher transferStateMatcher = new TransactionalStateMatcher();
            transferStateMatcher.withTxnId(equalTo(txnId1));
            transferStateMatcher.withOutcome(nullValue());

            // Accept the initial send after failover in transaction #1
            TransactionalState transferTxnOutcome = new TransactionalState();
            transferTxnOutcome.setTxnId(txnId1);
            transferTxnOutcome.setOutcome(new Accepted());

            // Receive call after failover should occur in transaction #1
            TransactionalStateMatcher txnDispositionStateMatcher = new TransactionalStateMatcher();
            txnDispositionStateMatcher.withTxnId(equalTo(txnId1));
            txnDispositionStateMatcher.withOutcome(new AcceptedMatcher());

            // Drop the connection after the declare giving a chance for recovery to attempt
            // to rebuild while the context is still reacting to the failed begin.
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclareButDoNotRespond();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId1);
            finalPeer.expectSenderAttach();
            // Send will be blocked waiting to fire so it will not be filtered
            // by the local transaction context in-doubt checks, however since there
            // was pending transactional work the transaction will be rolled back.
            finalPeer.expectTransfer(messageMatcher, transferStateMatcher, transferTxnOutcome, true);
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDisposition(true, txnDispositionStateMatcher);
            finalPeer.expectDischarge(txnId1, false);
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.setForceSyncSend(true);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(5000));

            try {
                session.commit();
            } catch (TransactionRolledBackException txrbex) {
                fail("Transaction should not have been rolled back");
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testTransactionCommitWithNoResponseRecoveredAsInDoubtAndPerformsNoWork() throws Exception {
        doTestTransactionRetirementWithNoResponseRecoveredAsInDoubtAndCommitRollsBack(true);
    }

    @Test
    @Timeout(20)
    public void testTransactionRollbackWithNoResponseRecoveredAsInDoubtAndPerformsNoWork() throws Exception {
        doTestTransactionRetirementWithNoResponseRecoveredAsInDoubtAndCommitRollsBack(false);
    }

    private void doTestTransactionRetirementWithNoResponseRecoveredAsInDoubtAndCommitRollsBack(boolean commit) throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType("myContent");

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            // The replayed send should occur in transaction #2
            TransactionalStateMatcher transferStateMatcher = new TransactionalStateMatcher();
            transferStateMatcher.withTxnId(equalTo(txnId2));
            transferStateMatcher.withOutcome(nullValue());

            // Receive call after failover should occur in transaction #2
            TransactionalStateMatcher txnDispositionStateMatcher = new TransactionalStateMatcher();
            txnDispositionStateMatcher.withTxnId(equalTo(txnId2));
            txnDispositionStateMatcher.withOutcome(new AcceptedMatcher());

            // Re-send after failover should occur in TXN #2
            TransactionalState transferTxnOutcome = new TransactionalState();
            transferTxnOutcome.setTxnId(txnId2);
            transferTxnOutcome.setOutcome(new Accepted());

            // Drop the connection after the declare giving a chance for recovery to attempt
            // to rebuild while the context is still reacting to the failed begin.
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectDischargeButDoNotRespond(txnId1, !commit);
            originalPeer.expectDeclareButDoNotRespond();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectSenderAttach();
            // Send will be blocked waiting to fire so it will not be filtered
            // by the local transaction context in-doubt checks, however since there
            // was pending transactional work the transaction will be rolled back.
            finalPeer.expectTransfer(messageMatcher, transferStateMatcher, transferTxnOutcome, true);
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDisposition(true, txnDispositionStateMatcher);
            finalPeer.expectDischarge(txnId2, false);
            finalPeer.expectDeclare(txnId1);
            finalPeer.expectDischarge(txnId1, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            if (commit) {
                try {
                    session.commit();
                    fail("Should have failed on the commit when connection dropped");
                } catch (TransactionRolledBackException txnRbEx) {
                    // Expected
                }
            } else {
                try {
                    session.rollback();
                } catch (JMSException jmsEx) {
                    fail("Should not have failed on the rollback when connection dropped");
                }
            }

            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(5000));

            try {
                session.commit();
            } catch (TransactionRolledBackException txrbex) {
                fail("Transaction should not have been rolled back");
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendWhileOfflinePreventsRecoveredTransactionFromCommitting() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final Binary txnId1 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            final Binary txnId2 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            // Drop the connection after the declare giving a chance for recovery to attempt
            // to rebuild while the context is still reacting to the failed begin.
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            originalPeer.expectDeclare(txnId1);
            originalPeer.expectSenderAttach();
            originalPeer.expectDischargeButDoNotRespond(txnId1, false);
            originalPeer.expectDeclareButDoNotRespond();
            originalPeer.dropAfterLastHandler();

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId2);
            finalPeer.expectSenderAttach();
            finalPeer.expectDischarge(txnId2, true);
            finalPeer.expectDeclare(txnId1);
            finalPeer.expectDischarge(txnId1, true);
            finalPeer.expectClose();

            // Need to allow time for the asynchronous send to fire after connection drop in order to ensure
            // that the send is no-op'd when the TXN is in-doubt
            final JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=1000", originalPeer, finalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalPeer.getServerPort() == remoteURI.getPort()) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalPeer.getServerPort() == remoteURI.getPort()) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue(originalConnected.await(5, TimeUnit.SECONDS), "Should connect to original peer");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage("myMessage");

            try {
                session.commit();
                fail("Should have failed on the commit when connection dropped");
            } catch (TransactionRolledBackException txnRbEx) {
                // Expected
            }

            // Following the failed commit the Transaction should be in-doubt and the send
            // should be skipped since the TXN is in-doubt because we should not have connected
            // to the final peer yet so a recovery wouldn't have happened and the transaction
            // state couldn't be marked good since currently there should be no active transaction.

            try {
                producer.send(message);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            assertTrue(finalConnected.await(5, TimeUnit.SECONDS), "Should connect to final peer");

            try {
                session.commit();
                fail("Transaction should have been rolled back since a send was skipped.");
            } catch (TransactionRolledBackException txrbex) {
                // Expected
            }

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverDoesNotFailPendingAsyncCompletionSend() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            // Ensure our send blocks in the provider waiting for credit so that on failover
            // the message will actually get sent from the Failover bits once we grant some
            // credit for the recovered sender.
            originalPeer.expectSenderAttachWithoutGrantingCredit();
            originalPeer.dropAfterLastHandler(10);  // Wait for sender to get into wait state

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=25", originalPeer, finalPeer);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            try {
                producer.send(message, listener);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            // This should fire after reconnect without an error, if it fires with an error at
            // any time then something is wrong.
            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS), "Did not get async callback");
            assertNull(listener.exception, "Completion should not have been on error");
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverDoesFailPendingAsyncCompletionSend() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            originalPeer.dropAfterLastHandler(15);  // Wait for sender to get into wait state

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();

            final JmsConnection connection = establishAnonymousConnecton("failover.initialReconnectDelay=25", originalPeer, finalPeer);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            try {
                producer.send(message, listener);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            // This should fire after reconnect without an error, if it fires with an error at
            // any time then something is wrong.
            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS), "Did not get async callback");
            assertNotNull(listener.exception, "Completion should have been due to error");
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            finalPeer.waitForAllHandlersToComplete(5000);
            finalPeer.expectClose();

            connection.close();

            finalPeer.waitForAllHandlersToComplete(5000);
        }
    }

    @Test
    @Timeout(20)
    public void testFailoverHandlesAnonymousFallbackWaitingForClose() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            // DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            // Ensure that sender detach is not answered so that next send must wait for close
            originalPeer.expectDetach(true, false, false);
            originalPeer.dropAfterLastHandler(20);  // Wait for sender to get into wait state

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            finalPeer.expectDetach(true, true, true);
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(
                    "failover.initialReconnectDelay=25" +
                    "&failover.nested.amqp.anonymousFallbackCacheSize=0" +
                    "&failover.nested.amqp.anonymousFallbackCacheTimeout=0",
                    originalPeer, finalPeer);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(null);

            // Send 2 messages
            String text = "myMessage";

            TextMessage message = session.createTextMessage(text);

            producer.send(queue, message);
            producer.send(queue, message);

            producer.close();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, false);
    }

    @Test
    @Timeout(20)
    public void testPassthroughCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, true);
    }

    @Test
    @Timeout(20)
    public void testPassthroughCreateTemporaryTopicFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(true, false);
    }

    @Test
    @Timeout(20)
    public void testPassthroughCreateTemporaryTopicFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(true, true);
    }

    private void doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(boolean topic, boolean deferAttachResponseWrite) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();

            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            try {
                if (topic) {
                    testPeer.expectAndRefuseTempTopicCreationAttach(AmqpError.UNAUTHORIZED_ACCESS, "Not Authorized to create temp topics.", false);
                    //Expect the detach response to the test peer after refusal.
                    testPeer.expectDetach(true, false, false);

                    session.createTemporaryTopic();
                } else {
                    testPeer.expectAndRefuseTempQueueCreationAttach(AmqpError.UNAUTHORIZED_ACCESS, "Not Authorized to create temp queues.", false);
                    //Expect the detach response to the test peer after refusal.
                    testPeer.expectDetach(true, false, false);

                    session.createTemporaryQueue();
                }
                fail("Should have thrown security exception");
            } catch (JMSSecurityException jmsse) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughRemotelyCloseProducer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch producerClosed = new CountDownLatch(1);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();

            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onProducerClosed(MessageProducer producer, Throwable exception) {
                    producerClosed.countDown();
                }
            });
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end it afterwards.
            testPeer.expectSenderAttach();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            // Verify the producer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        producer.getDestination();
                    } catch (Exception ex) {
                        if (ex instanceof IllegalStateException && ex.getCause() != null) {
                            String message = ex.getCause().getMessage();
                            if (message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                message.contains(BREAD_CRUMB)) {
                                return true;
                            }
                        }

                        LOG.debug("Caught unexpected exception: {}", ex);
                    }

                    return false;
                }
            }, 10000, 10), "producer never closed.");

            assertTrue(producerClosed.await(10, TimeUnit.SECONDS), "Producer closed callback didn't trigger");

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughOfSendFailsWhenDelayedDeliveryIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            final String testPeerURI = createPeerURI(testPeer);
            LOG.info("Original peer is at: {}", testPeerURI);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();

            // DO NOT add capability to indicate server support for DELAYED-DELIVERY so that
            // send fails and we can see if the error passes through the failover provider
            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.start();

            Matcher<Symbol[]> desiredCapabilitiesMatcher = arrayContaining(new Symbol[] { DELAYED_DELIVERY });
            Symbol[] offeredCapabilities = null;
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryDelay(5000);

            // Producer should fail to send when message has delivery delay since remote
            // did not report that it supports that option.
            Message message = session.createMessage();
            try {
                producer.send(message);
                fail("Send should fail");
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughOfSendTimesOutWhenNoDispostionArrives() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final String testPeerURI = createPeerURI(testPeer);
            LOG.info("Original peer is at: {}", testPeerURI);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();

            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.setSendTimeout(500);
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response for which should cause the
            // send operation to time out.
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);

            try {
                producer.send(message);
                fail("Send should time out.");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught expected error: {}", jmsEx.getMessage());
            } catch (Throwable error) {
                fail("Send should time out, but got: " + error.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughOfRollbackErrorCoordinatorClosedOnCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final String testPeerURI = createPeerURI(testPeer);
            LOG.info("Original peer is at: {}", testPeerURI);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.start();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});

            testPeer.expectDeclare(txnId1);
            testPeer.remotelyCloseLastCoordinatorLinkOnDischarge(txnId1, false, true, txnId2);
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId2);
            testPeer.expectDischarge(txnId2, true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.commit();
                fail("Transaction should have rolled back");
            } catch (TransactionRolledBackException ex) {
                LOG.info("Caught expected TransactionRolledBackException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testPassthroughOfSessionCreateFailsOnDeclareTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final String testPeerURI = createPeerURI(testPeer);
            LOG.info("Original peer is at: {}", testPeerURI);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclareButDoNotRespond();
            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            JmsConnection connection = establishAnonymousConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConnectionPropertiesExtensionAppliedOnEachReconnect() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);

            final String property1 = "property1";
            final String property2 = "property2";

            final UUID value1 = UUID.randomUUID();
            final UUID value2 = UUID.randomUUID();

            Matcher<?> connPropsMatcher1 = allOf(
                    hasEntry(Symbol.valueOf(property1), value1),
                    not(hasEntry(Symbol.valueOf(property2), value2)));

            Matcher<?> connPropsMatcher2 = allOf(
                    not(hasEntry(Symbol.valueOf(property1), value1)),
                    hasEntry(Symbol.valueOf(property2), value2));

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen(connPropsMatcher1, null, false);
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler(10);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen(connPropsMatcher2, null, false);
            finalPeer.expectBegin();

            final URI remoteURI = new URI("failover:(" + originalURI + "," + finalURI + ")");

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                if (originalConnected.getCount() == 1) {
                    properties.put(property1, value1);
                } else {
                    properties.put(property2, value2);
                }

                return properties;
            });

            JmsConnection connection = (JmsConnection) factory.createConnection();

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

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            finalPeer.waitForAllHandlersToComplete(2000);

            assertTrue(originalConnected.await(3, TimeUnit.SECONDS), "Should connect to original peer");
            assertTrue(finalConnected.await(3, TimeUnit.SECONDS), "Should connect to final peer");

            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);        }
    }

    @Test
    @Timeout(20)
    public void testSessionCreationRecoversAfterDropWithNoBeginResponse() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final String content = "myContent";
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(content);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin(false);
            originalPeer.dropAfterLastHandler(20);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, finalPeer);

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(2000);

            connection.close();

            originalPeer.waitForAllHandlersToCompleteNoAssert(1000);
            finalPeer.waitForAllHandlersToComplete(1000);

            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals(content, ((TextMessage) message).getText());
        }
    }

    @Test
    @Timeout(20)
    public void testMultipleSessionCreationRecoversAfterDropWithNoBeginResponseAndFailedRecoveryAttempt() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer intermediatePeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final String content = "myContent";
            final DescribedType amqpValueNullContent = new AmqpValueDescribedType(content);

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectBegin(false);
            originalPeer.dropAfterLastHandler(20);

            intermediatePeer.expectSaslAnonymous();
            intermediatePeer.expectOpen();
            intermediatePeer.expectBegin();
            intermediatePeer.expectBegin(false);
            intermediatePeer.dropAfterLastHandler();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            finalPeer.expectDispositionThatIsAcceptedAndSettled();
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, intermediatePeer, finalPeer);

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue1 = session1.createQueue("myQueue");
            MessageConsumer consumer1 = session1.createConsumer(queue1);
            Message message1 = consumer1.receive(2000);

            Queue queue2 = session2.createQueue("myQueue");
            MessageConsumer consumer2 = session2.createConsumer(queue2);
            Message message2 = consumer2.receive(2000);

            connection.close();

            originalPeer.waitForAllHandlersToComplete(1000);
            intermediatePeer.waitForAllHandlersToComplete(1000);
            finalPeer.waitForAllHandlersToComplete(1000);

            assertNotNull(message1);
            assertTrue(message1 instanceof TextMessage);
            assertEquals(content, ((TextMessage) message1).getText());
            assertNotNull(message2);
            assertTrue(message2 instanceof TextMessage);
            assertEquals(content, ((TextMessage) message2).getText());
        }
    }

    @Test
    @Timeout(20)
    public void testMultipleSenderCreationRecoversAfterDropWithNoAttachResponseAndFailedRecoveryAttempt() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer intermediatePeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.expectSenderAttachButDoNotRespond();
            originalPeer.dropAfterLastHandler(20);

            intermediatePeer.expectSaslAnonymous();
            intermediatePeer.expectOpen();
            intermediatePeer.expectBegin();
            intermediatePeer.expectBegin();
            intermediatePeer.expectSenderAttachButDoNotRespond();
            intermediatePeer.dropAfterLastHandler();

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectSenderAttach();
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, intermediatePeer, finalPeer);

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer1 = session.createProducer(queue);
            MessageProducer producer2 = session.createProducer(queue);

            assertNotNull(producer1);
            assertNotNull(producer2);

            assertEquals(queue, producer1.getDestination());
            assertEquals(queue, producer2.getDestination());

            connection.close();

            originalPeer.waitForAllHandlersToComplete(1000);
            intermediatePeer.waitForAllHandlersToComplete(1000);
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSenderAndReceiverCreationRecoversAfterDropWithNoAttachResponseAndFailedRecoveryAttempt() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer intermediatePeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.expectReceiverAttachButDoNotRespond();
            originalPeer.dropAfterLastHandler(20);

            intermediatePeer.expectSaslAnonymous();
            intermediatePeer.expectOpen();
            intermediatePeer.expectBegin();
            intermediatePeer.expectBegin();
            intermediatePeer.expectSenderAttachButDoNotRespond();
            intermediatePeer.dropAfterLastHandler(10);

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow();
            finalPeer.expectClose();

            final JmsConnection connection = establishAnonymousConnecton(originalPeer, intermediatePeer, finalPeer);

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should not have thrown an Exception: " + ex);
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            assertNotNull(producer);
            assertNotNull(consumer);

            assertEquals(queue, producer.getDestination());
            assertNull(consumer.getMessageListener());

            connection.close();

            originalPeer.waitForAllHandlersToComplete(1000);
            intermediatePeer.waitForAllHandlersToComplete(1000);
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
