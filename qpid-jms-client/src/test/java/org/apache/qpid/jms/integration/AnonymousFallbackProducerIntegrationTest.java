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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the Anonymous Fallback producer implementation.
 *
 * DO NOT add capability to indicate server support for ANONYMOUS-RELAY for any of these tests
 */
@RunWith(QpidJMSTestRunner.class)
public class AnonymousFallbackProducerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(AnonymousFallbackProducerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCloseSenderWithNoActiveFallbackProducers() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);

            producer.close();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseProducerDuringSyncSendNoCache() throws Exception {
        doTestRemotelyCloseProducerDuringSyncSend(0);
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseProducerDuringSyncSendOneCached() throws Exception {
        doTestRemotelyCloseProducerDuringSyncSend(1);
    }

    private void doTestRemotelyCloseProducerDuringSyncSend(int cacheSize) throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Use a long timeout to ensure no early evictions in this test.
            Connection connection = testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + cacheSize + "&amqp.anonymousFallbackCacheTimeout=60000");

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, give it credit.
            testPeer.expectSenderAttach();

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));

            // Expect a message to be sent, but don't send a disposition in
            // response, simply remotely close the producer instead.
            testPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, false);
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(null);

            Message message = session.createTextMessage(text);

            try {
                producer.send(queue, message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                LOG.trace("JMSException thrown from send: ", jmse);
                // Expected but requires some context to be correct.
                assertTrue(jmse instanceof ResourceAllocationException);
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseProducerWithSendWaitingForCreditNoCache() throws Exception {
        doTestRemotelyCloseProducerWithSendWaitingForCredit(0);
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseProducerWithSendWaitingForCreditOneCached() throws Exception {
        doTestRemotelyCloseProducerWithSendWaitingForCredit(1);
    }

    private void doTestRemotelyCloseProducerWithSendWaitingForCredit(int cacheSize) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Use a long timeout to ensure no early evictions in this test.
            Connection connection = testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + cacheSize + "&amqp.anonymousFallbackCacheTimeout=60000");

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, don't give it credit.
            testPeer.expectSenderAttachWithoutGrantingCredit();

            // Producer has no credit so the send should block waiting for it, then fail when the remote close occurs
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, "Producer closed", 50);
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(null);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(queue, message);
                fail("Expected exception to be thrown due to close of producer");
            } catch (ResourceAllocationException rae) {
                // Expected if remote close beat the send to the provider
            } catch (IllegalStateException ise) {
                // Can happen if send fires before remote close if processed.
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSyncSendNoCache() throws Exception {
        doTestRemotelyCloseConnectionDuringSyncSend(0);
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSyncSendOneCached() throws Exception {
        doTestRemotelyCloseConnectionDuringSyncSend(1);
    }

    private void doTestRemotelyCloseConnectionDuringSyncSend(int cacheSize) throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Use a long timeout to ensure no early evictions in this test.
            Connection connection = testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + cacheSize + "&amqp.anonymousFallbackCacheTimeout=60000");

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, give it credit.
            testPeer.expectSenderAttach();

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));

            // Expect a message to be sent, but don't send a disposition in
            // response, simply remotely close the connection instead.
            testPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, false);
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(null);

            Message message = session.createTextMessage(text);

            try {
                producer.send(queue, message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected exception with specific context
                assertTrue(jmse instanceof ResourceAllocationException);
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsZeroAndTimeoutNoCache() throws Exception {
        doTestSendWhenLinkCreditIsZeroAndTimeout(0);
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsZeroAndTimeoutCacheOne() throws Exception {
        doTestSendWhenLinkCreditIsZeroAndTimeout(1);
    }

    private void doTestSendWhenLinkCreditIsZeroAndTimeout(int cacheSize) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Use a long timeout to ensure no early evictions in this test.
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + cacheSize + "&amqp.anonymousFallbackCacheTimeout=60000");
            connection.setSendTimeout(500);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");

            // Expect the producer to attach. Don't send any credit so that the client will
            // block on a send and we can test our timeouts.
            testPeer.expectSenderAttachWithoutGrantingCredit();
            if (cacheSize == 0) {
                testPeer.expectDetach(true, true, true);
            }
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(null);

            try {
                producer.send(queue, message);
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

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testSyncSendFailureHandled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher, nullValue(), new Rejected(), true);
            testPeer.expectDetach(true, true, true);

            Message message = session.createMessage();
            try {
                producer.send(dest, message);
                fail("Send should fail");
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
            }

            // Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testAsyncSendFailureHandled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final CountDownLatch sendFailureReportedToListener = new CountDownLatch(1);
            final AtomicReference<Throwable> sendFailureError = new AtomicReference<>();

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?jms.forceAsyncSend=true&amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");

            connection.setExceptionListener((error) -> {
                sendFailureError.compareAndSet(null, error);
                sendFailureReportedToListener.countDown();
            });

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            final String BREAD_CRUMB = "SEND FAILURE EXPECTED";

            org.apache.qpid.jms.test.testpeer.describedtypes.Error rejectError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
            rejectError.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
            rejectError.setDescription(BREAD_CRUMB);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher, nullValue(), new Rejected().setError(rejectError), true);
            testPeer.expectDetach(true, true, true);

            // Producer should act as synchronous regardless of asynchronous send setting.
            Message message = session.createMessage();
            try {
                producer.send(dest, message);
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
                fail("Send should not fail as it should have fired asynchronously");
            }

            // Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            assertTrue("Send failure not reported to exception handler", sendFailureReportedToListener.await(5, TimeUnit.SECONDS));
            assertNotNull(sendFailureError.get());
            assertTrue(sendFailureError.get() instanceof ResourceAllocationException);
            assertTrue(sendFailureError.get().getMessage().contains(BREAD_CRUMB));

            producer.send(dest, message);

            // Send here is asynchronous so we need to wait for disposition to arrive and detach to happen
            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testAsyncCompletionListenerSendFailureHandled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            String content = "testContent";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(content));

            TestJmsCompletionListener completionListener = new TestJmsCompletionListener();
            Message message = session.createTextMessage(content);

            final String BREAD_CRUMB = "SEND FAILURE EXPECTED";

            org.apache.qpid.jms.test.testpeer.describedtypes.Error rejectError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
            rejectError.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
            rejectError.setDescription(BREAD_CRUMB);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher, nullValue(), new Rejected().setError(rejectError), true);
            testPeer.expectDetach(true, true, true);

            // The fallback producer acts as synchronous regardless of the completion listener,
            // so exceptions are thrown from send. Only onComplete uses the listener.
            try {
                producer.send(dest, message, completionListener);
            } catch (JMSException jmsEx) {
                LOG.debug("Caught unexpected error from failed send.");
                fail("Send should not fail for asychrnous completion sends");
            }

            // Repeat the send (but accept this time) and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            assertTrue("Send failure not reported to exception handler", completionListener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(completionListener.exception);
            assertTrue(completionListener.exception instanceof ResourceAllocationException);
            assertTrue(completionListener.exception.getMessage().contains(BREAD_CRUMB));

            TestJmsCompletionListener completionListener2 = new TestJmsCompletionListener();

            producer.send(dest, message, completionListener2);

            assertTrue("Did not get completion callback", completionListener2.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(completionListener2.exception);
            Message receivedMessage2 = completionListener2.message;
            assertNotNull(receivedMessage2);
            assertTrue(receivedMessage2 instanceof TextMessage);
            assertEquals(content, ((TextMessage) receivedMessage2).getText());

            // Asynchronous send requires a wait otherwise we can close before the detach which we are testing for.
            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testAsyncCompletionListenerSendWhenNoCacheConfigured() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            String content = "testContent";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(content));

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            TestJmsCompletionListener completionListener = new TestJmsCompletionListener();
            Message message = session.createTextMessage(content);

            producer.send(dest, message, completionListener);

            assertTrue("Did not get completion callback", completionListener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(completionListener.exception);
            Message receivedMessage = completionListener.message;
            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals(content, ((TextMessage) receivedMessage).getText());

            // Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            TestJmsCompletionListener completionListener2 = new TestJmsCompletionListener();

            producer.send(dest, message, completionListener2);

            assertTrue("Did not get completion callback", completionListener2.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(completionListener2.exception);
            Message receivedMessage2 = completionListener2.message;
            assertNotNull(receivedMessage2);
            assertTrue(receivedMessage2 instanceof TextMessage);
            assertEquals(content, ((TextMessage) receivedMessage2).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyEndFallbackProducerCompletesAsyncSends() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch producerClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onProducerClosed(MessageProducer producer, Throwable exception) {
                    producerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end the session afterwards.
            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            final int MSG_COUNT = 3;

            for (int i = 0; i < MSG_COUNT; ++i) {
                testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            }

            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB, 50);

            TestJmsCompletionListener listener = new TestJmsCompletionListener(MSG_COUNT);
            try {
                for (int i = 0; i < MSG_COUNT; ++i) {
                    Message message = session.createTextMessage("content");
                    producer.send(message, listener);
                }
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            testPeer.waitForAllHandlersToComplete(2000);

            // Verify the sends gets marked as having failed
            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertEquals(MSG_COUNT, listener.errorCount);

            // Verify the producer gets marked closed
            assertTrue("Producer closed callback didn't trigger", producerClosed.await(5, TimeUnit.SECONDS));
            try {
                producer.getDeliveryMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String errorMessage = jmsise.getCause().getMessage();
                assertTrue(errorMessage.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(errorMessage.contains(BREAD_CRUMB));
            }

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseSessionAndAttemptAsyncCompletionSendThrowsAndLeavesMessageReadable() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                    "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=60000");

            final CountDownLatch sessionClosed = new CountDownLatch(1);

            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onSessionClosed(Session session, Throwable cause) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(null);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo("myQueue"));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            String content = "testContent";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(content));

            // Perform a send and observe an attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);
            testPeer.remotelyEndLastOpenedSession(true);

            Message message1 = session.createTextMessage(content);
            Message message2 = session.createTextMessage(content);
            assertNull("Should not yet have a JMSDestination", message1.getJMSDestination());
            assertNull("Should not yet have a JMSDestination", message2.getJMSDestination());
            producer.send(queue, message1);

            testPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Session should have been closed", sessionClosed.await(2, TimeUnit.SECONDS));

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(queue, message2, listener);
                fail("Expected exception to be thrown for this send.");
            } catch (JMSException e) {
                LOG.trace("Caught expected exception: {}", e.getMessage());
            }

            assertFalse("Should not get async callback", listener.awaitCompletion(5, TimeUnit.MILLISECONDS));

            // Message should be readable but not carry a destination as it wasn't actually sent anywhere
            assertNull("Should not have a readable JMSDestination", message2.getJMSDestination());
            assertEquals("Message body not as expected", content, ((TextMessage) message2).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testFallbackProducerRecoversFromRefusalOfSenderOpenOnNextSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=1&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            Topic dest = session.createTopic(topicName);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            Message message = session.createMessage();

            // Refuse the attach which should result in fallback producer detaching the refused
            // link attach and the send should then fail.
            testPeer.expectSenderAttach(targetMatcher, true, false);
            testPeer.expectDetach(true, false, false);

            try {
                producer.send(dest, message);
                fail("Send should have failed because sender link was refused.");
            } catch (JMSException ex) {
                LOG.trace("Caught expected exception: ", ex);
            }

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 60000)
    public void testRepeatedSendToSameAddressWhenCacheSizeOfOneKeepsFallbackProducerInCache() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int MESSAGE_COUNT = 25;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=1&amqp.anonymousFallbackCacheTimeout=200");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectSenderAttach(targetMatcher, false, true);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            Topic dest = session.createTopic(topicName);
            Message message = session.createMessage();

            // Setup our expectations
            for (int i = 1; i <= MESSAGE_COUNT; ++i) {
                testPeer.expectTransfer(messageMatcher);
            }

            testPeer.expectDetach(true, true, true);

            // First round of sends should open and cache sender links
            for (int i = 1; i <= MESSAGE_COUNT; ++i) {
                producer.send(dest, message);
            }

            LOG.debug("Finished with send cycle, producer should now timeout");

            // The eviction timer should reduce the cache to zero after we go idle
            testPeer.waitForAllHandlersToComplete(3000);

            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testSendToMultipleDestinationsOpensNewSendersWhenCaching() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int CACHE_SIZE = 5;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + CACHE_SIZE + "&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // First round of sends should open and cache sender links
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                // Expect a new message sent by the above producer to cause creation of a new
                // sender link to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // This round of sends should reuse the cached links
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // Cached senders should all close when the producer is closed.
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                testPeer.expectDetach(true, true, true);
            }

            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 30000)
    public void testCachedFallbackProducersAreTimedOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int CACHE_SIZE = 5;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + CACHE_SIZE + "&amqp.anonymousFallbackCacheTimeout=300");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // First round of sends should open and cache sender links
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                // Expect a new message sent by the above producer to cause creation of a new
                // sender link to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
                MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
                TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
                messageMatcher.setHeadersMatcher(headersMatcher);
                messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

                Message message = session.createMessage();

                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // Cached senders should all close when the cache timeout is reached and they are expired
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                testPeer.expectDetach(true, true, true);
            }

            // On a slow CI machine we could fail here due to the timeouts not having run.
            testPeer.waitForAllHandlersToComplete(6000);

            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testCachedFallbackProducerEvictedBySendToUncachedAddress() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int CACHE_SIZE = 2;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + CACHE_SIZE + "&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // First round of sends should open and cache sender links
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                // Expect a new message sent by the above producer to cause creation of a new
                // sender link to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // Second round using different addresses for the sends should evict old links and open new ones
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + UUID.randomUUID().toString());

                // Expect a new message sent by the above producer to cause creation of a new
                // sender link to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectDetach(true, true, true);
                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // The current cached senders should all close when the producer is closed.
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                testPeer.expectDetach(true, true, true);
            }

            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testCachedFallbackProducerEvictedBySendToUncachedAddressHandlesDelayedResponse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=1&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            Topic dest1 = session.createTopic(topicName + 1);
            Topic dest2 = session.createTopic(topicName + 2);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(dest1.getTopicName()));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            Message message = session.createMessage();

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);

            producer.send(dest1, message);

            // Expect new send to a different destination to detach the previous cached link
            // and once the response arrives the send should complete normally.
            targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(dest2.getTopicName()));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            message = session.createMessage();

            testPeer.expectDetach(true, false, false);
            // Workaround to allow a deferred detach at a later time.
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(false, true, AmqpError.RESOURCE_DELETED, "error", 20);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest2, message);
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseCachedFallbackProducerFreesSlotInCache() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int CACHE_SIZE = 3;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + CACHE_SIZE + "&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // First round of sends should open and cache sender links
            for (int i = 1; i < CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                // Expect a new message sent to the above created destination to result in a new
                // sender link attached to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            Topic dest = session.createTopic(topicName + CACHE_SIZE);

            // Expect a new message sent to the above created destination to result in a new
            // sender link attached to the given destination.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(dest.getTopicName()));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            Message message = session.createMessage();

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true);

            producer.send(dest, message);

            // Must ensure that the next send only fires after the remote detach has occurred otherwise
            // it will definitely evict another producer from the cache.
            testPeer.waitForAllHandlersToComplete(1000);

            dest = session.createTopic(topicName + UUID.randomUUID().toString());

            // Expect a new message sent to the above created destination to result in a new
            // sender link attached to the given destination.  Existing cached producers should
            // remain in the cache as a slot should now be open.
            targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(dest.getTopicName()));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            message = session.createMessage();

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);

            producer.send(dest, message);

            // The current cached senders should all close when the producer is closed.
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                testPeer.expectDetach(true, true, true);
            }

            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testFailureOfOneCacheProducerCloseOnPropagatedToMainProducerClose() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            final int CACHE_SIZE = 3;

            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=" + CACHE_SIZE + "&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // First round of sends should open and cache sender links
            for (int i = 1; i <= CACHE_SIZE; ++i) {
                Topic dest = session.createTopic(topicName + i);

                // Expect a new message sent by the above producer to cause creation of a new
                // sender link to the given destination.
                TargetMatcher targetMatcher = new TargetMatcher();
                targetMatcher.withAddress(equalTo(dest.getTopicName()));
                targetMatcher.withDynamic(equalTo(false));
                targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

                Message message = session.createMessage();

                testPeer.expectSenderAttach(targetMatcher, false, false);
                testPeer.expectTransfer(messageMatcher);

                producer.send(dest, message);
            }

            // The current cached senders should all close when the producer is closed.
            for (int i = 1; i < CACHE_SIZE; ++i) {
                testPeer.expectDetach(true, true, true);
            }

            // Last one carries error but since we asked for close it should be ignored as we got what we wanted.
            testPeer.expectDetach(true, true, true, AmqpError.RESOURCE_LOCKED, "Some error on detach");

            try {
                producer.close();
            } catch (JMSException ex) {
                LOG.trace("Caught unexpected error: ", ex);
                fail("Should not have thrown an error as close was requested so errors are ignored.");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        private final CountDownLatch completed;

        @SuppressWarnings("unused")
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
