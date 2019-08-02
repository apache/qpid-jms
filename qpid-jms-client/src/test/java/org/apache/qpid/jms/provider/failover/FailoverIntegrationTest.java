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
import static org.hamcrest.Matchers.arrayContaining;
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
import java.util.HashMap;
import java.util.Map;
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
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.jms.util.StopWatch;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(QpidJMSTestRunner.class)
public class FailoverIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverIntegrationTest.class);

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final UnsignedByte SASL_FAIL_AUTH = UnsignedByte.valueOf((byte) 1);
    private static final UnsignedByte SASL_SYS = UnsignedByte.valueOf((byte) 2);
    private static final UnsignedByte SASL_SYS_PERM = UnsignedByte.valueOf((byte) 3);
    private static final UnsignedByte SASL_SYS_TEMP = UnsignedByte.valueOf((byte) 4);

    @Test(timeout = 20000)
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

    @Test(timeout = 20000)
    public void testConnectThrowsSecurityViolationOnFailureFromSaslWithClientID() throws Exception {
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_FAIL_AUTH);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_SYS);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(true, SASL_SYS_PERM);
    }

    @Test(timeout = 20000)
    public void testConnectThrowsSecurityViolationOnFailureFromSaslExplicitlyWithoutClientID() throws Exception {
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_FAIL_AUTH);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_SYS);
        doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(false, SASL_SYS_PERM);
    }

    private void doConnectThrowsSecurityViolationOnFailureFromSaslWithOrExplicitlyWithoutClientIDTestImpl(boolean clientID, UnsignedByte saslFailureCode) throws Exception {
        String optionString;
        if(clientID) {
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

    @Test(timeout = 20000)
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

    @Test(timeout = 20000)
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

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

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

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));

            assertTrue("The ExceptionListener should have been alerted", exceptionListenerFired.await(3, TimeUnit.SECONDS));
            Throwable ex = failure.get();
            assertTrue("Unexpected failure exception: " + ex, ex instanceof JMSSecurityException);

            // Verify the consumer gets marked closed
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageSelector();
                    } catch (IllegalStateException jmsise) {
                        return true;
                    }
                    return false;
                }
            }, 5000, 5));

            // Shut down last peer and verify no connection made to it
            finalPeer.purgeExpectations();
            finalPeer.close();
            assertNotNull("First peer should have accepted a TCP connection", originalPeer.getClientSocket());
            assertNotNull("Rejecting peer should have accepted a TCP connection", rejectingPeer.getClientSocket());
            assertNull("Final peer should not have accepted any TCP connection", finalPeer.getClientSocket());
        }
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));
            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

            // Check message arrives
            finalPeer.expectDispositionThatIsAcceptedAndSettled();

            Message msg = consumer.receive(5000);
            assertTrue("Expected an instance of TextMessage, got: " + msg, msg instanceof TextMessage);
            assertEquals("Unexpected msg content", expectedMessageContent, ((TextMessage) msg).getText());

            assertFalse("The ExceptionListener should not have been alerted", exceptionListenerFired.get());

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverHandlesConnectErrorInvalidField() throws Exception {
        doFailoverHandlesConnectErrorInvalidFieldTestImpl(false);
    }

    @Test(timeout = 20000)
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
    public void testFailoverHandlesConnectErrorInvalidFieldOnReconnect() throws Exception {
        doFailoverHandlesConnectErrorInvalidFieldOnReconnectTestImpl(false);
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

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

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            boolean await = senderCompleted.await(5, TimeUnit.SECONDS);
            Throwable t = problem.get();
            assertTrue("Sender thread should have completed. Problem: " + t, await);

            connection.close();
            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
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

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

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

            assertTrue("Session close should have completed by now", sessionCloseCompleted.await(3, TimeUnit.SECONDS));
            assertFalse("Session close should have completed normally", sessionClosedThrew.get());

            connection.close();
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

    @Test(timeout = 20000)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test(timeout = 20000)
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
            MatcherAssert.assertThat("Send call should have taken at least the disposition delay", elapsed, Matchers.greaterThanOrEqualTo(delay));

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverPassthroughOfRejectedSyncSend() throws Exception {
        Rejected failingState = new Rejected();
        org.apache.qpid.jms.test.testpeer.describedtypes.Error rejectError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
        rejectError.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
        rejectError.setDescription("RLE description");
        failingState.setError(rejectError);

        doFailoverPassthroughOfFailingSyncSendTestImpl(failingState, true);
    }

    @Test(timeout = 20000)
    public void testFailoverPassthroughOfReleasedSyncSend() throws Exception {
        doFailoverPassthroughOfFailingSyncSendTestImpl(new Released(), false);
    }

    @Test(timeout = 20000)
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
                MatcherAssert.assertThat("Send call should have taken at least the disposition delay", elapsed, Matchers.greaterThanOrEqualTo(delay));

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

    @Repeat(repetitions = 1)
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

            assertTrue("Should connect to test peer", testConnected.await(6, TimeUnit.SECONDS));

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

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListener() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListener";

        doRemotelyCloseConsumerWithMessageListenerFiresJMSExceptionListenerTestImpl(errorCondition, errorDescription);
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
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
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            if(errorCondition != null) {
                                return message.contains(errorCondition.toString()) &&
                                        message.contains(errorDescription);
                            } else {
                                return message.contains("Unknown error from remote peer");
                            }
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 5000, 10));

            assertTrue("Consumer closed callback didn't trigger",  consumerClosed.await(2000, TimeUnit.MILLISECONDS));
            assertTrue("JMS Exception listener should have fired with a MessageListener", exceptionListenerFired.await(2000, TimeUnit.MILLISECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();

            // Shut the connection down
            testPeer.expectClose();
            connection.close();
            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateConsumerFailsConnectionAndRetries() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerFailsConnectionAndRetries";

        doTestFailoverCannotRecreateConsumerFailsConnectionAndRetries(errorCondition, errorDescription);
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));
            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

            // Check message arrives
            assertTrue("The onMessage listener should have fired", msgReceived.await(3, TimeUnit.SECONDS));
            Message msg = msgRef.get();
            assertTrue("Expected an instance of TextMessage, got: " + msg, msg instanceof TextMessage);
            assertEquals("Unexpected msg content", expectedMessageContent, ((TextMessage) msg).getText());

            // Check that consumer isn't closed
            try {
                consumer.getMessageListener();
            } catch (JMSException ex) {
                fail("Consumer should be in open state and not throw here.");
            }

            assertFalse("The ExceptionListener should not have been alerted", exceptionListenerFired.get());

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateProducerFailsConnectionAndRetries() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateProducerFailsConnectionAndRetries";

        doTestFailoverCannotRecreateProducerFailsConnectionAndRetries(errorCondition, errorDescription);
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));
            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

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

            assertFalse("The ExceptionListener should not have been alerted", exceptionListenerFired.get());

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled";

        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(true, errorCondition, errorDescription);
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoMessageListener() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoMessageListener";

        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(false, errorCondition, errorDescription);
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateConsumerWithCloseFailedLinksEnabledNoErrorConditionGiven() throws Exception {
        doTestFailoverCannotRecreateConsumerWithCloseFailedLinksEnabled(true, null, null);
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));
            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

            // Verify the consumer gets marked closed
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
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
            }, 5000, 10));

            // Verify the exception listener behaviour
            if (addListener) {
                assertTrue("JMS Exception listener should have fired with a MessageListener", exceptionListenerFired.await(2, TimeUnit.SECONDS));
            } else {
                assertFalse("The ExceptionListener should not have been alerted", exceptionListenerFired.await(10, TimeUnit.MILLISECONDS));
            }

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testFailoverCannotRecreateProducerWithCloseFailedLinksEnabled() throws Exception {
        Symbol errorCondition = AmqpError.RESOURCE_DELETED;
        String errorDescription = "testFailoverCannotRecreateProducerWithCloseFailedLinksEnabled";

        doTestFailoverCannotRecreateWithCloseFailedLinksEnabled(errorCondition, errorDescription);
    }

    @Test(timeout = 20000)
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

            assertTrue("Should connect to original peer", originalConnected.await(3, TimeUnit.SECONDS));
            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

            // Verify the producer gets marked closed
            assertTrue("producer never closed.", Wait.waitFor(new Wait.Condition() {
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
            }, 5000, 10));

            assertFalse("The ExceptionListener should not have been alerted", exceptionListenerFired.get());

            // Shut it down
            finalPeer.expectClose();
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
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

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.expectCoordinatorAttach();
            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            originalPeer.expectDeclare(txnId);
            originalPeer.expectSenderAttach();
            originalPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            originalPeer.dropAfterLastHandler(10);

            // --- Post Failover Expectations of sender --- //
            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectCoordinatorAttach();
            finalPeer.expectDeclare(txnId);
            finalPeer.expectSenderAttach();
            // Attempt to commit the in-doubt TX will result in rollback and a new TX will be started.
            finalPeer.expectDischarge(txnId, true);
            finalPeer.expectDeclare(txnId);
            // this rollback comes from the session being closed on connection close.
            finalPeer.expectDischarge(txnId, true);
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

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));

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

            assertTrue("Should connect to final peer", finalConnected.await(3, TimeUnit.SECONDS));

            // This should fire after reconnect without an error, if it fires with an error at
            // any time then something is wrong.
            assertTrue("Did not get async callback for send #1", listener1.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull("Completion of send #1 should not have been on error", listener1.exception);
            assertNotNull(listener1.message);
            assertTrue(listener1.message instanceof TextMessage);

            try {
                producer.send(message, listener2);
            } catch (JMSException jmsEx) {
                fail("Should not have failed the async completion send.");
            }

            assertTrue("Did not get async callback for send #2", listener2.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull("Completion of send #2 should not have been on error", listener2.exception);
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

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
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
            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull("Completion should not have been on error", listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testPassthroughCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, false);
    }

    @Test(timeout = 20000)
    public void testPassthroughCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, true);
    }

    @Test(timeout = 20000)
    public void testPassthroughCreateTemporaryTopicFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(true, false);
    }

    @Test(timeout = 20000)
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

    @Test(timeout = 20000)
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
            assertTrue("producer never closed.", Wait.waitFor(new Wait.Condition() {
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
            }, 10000, 10));

            assertTrue("Producer closed callback didn't trigger", producerClosed.await(10, TimeUnit.SECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
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

    @Test(timeout = 20000)
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

    @Test(timeout=20000)
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

    @Test(timeout=20000)
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
