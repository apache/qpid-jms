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

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.message.foreign.ForeignJmsMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.ListDescribedType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.Modified;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCloseSender() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.close();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseSenderTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectDetach(true, false, true);
            testPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            try {
                producer.close();
                fail("Should have thrown a timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught excpected exception", jmsEx);
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSentTextMessageCanBeModified() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            producer.send(message);

            assertEquals(text, message.getText());
            message.setText(text + text);
            assertEquals(text + text, message.getText());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testDefaultDeliveryModeProducesDurableMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            producer.send(message);
            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testProducerOverridesMessageDeliveryMode() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message, explicitly setting the deliveryMode on the
            // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
            // that the producer ignores this value and sends the message as PERSISTENT(/durable)
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();
            message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());

            producer.send(message);

            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent the JMSDestination header is set to
     * the Destination used by the producer.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSDestination() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            producer.send(message);

            assertEquals("Should have had JMSDestination set", queue, message.getJMSDestination());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSTimestamp() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            long currentTime = System.currentTimeMillis();
            Date creationLower = new Date(currentTime);
            Date creationUpper = new Date(currentTime + 3000);
            Matcher<Date> inRange = both(greaterThanOrEqualTo(creationLower)).and(lessThanOrEqualTo(creationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withCreationTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that after sending a message with the disableMessageTimestamp hint set, the
     * message object has a 0 JMSTimestamp value, and no creation-time field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageTimestampHint() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withCreationTime(nullValue()); // Check there is no creation-time value;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertEquals("JMSTimestamp should not yet be set", 0, message.getJMSTimestamp());

            producer.setDisableMessageTimestamp(true);
            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);

            assertEquals("JMSTimestamp should still not be set", 0, message.getJMSTimestamp());
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSExpirationRelatedAbsoluteExpiryAndTtlFields() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            long currentTime = System.currentTimeMillis();
            long ttl = 100_000;

            Date expirationLower = new Date(currentTime + ttl);
            Date expirationUpper = new Date(currentTime + ttl + 5000);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            Matcher<Date> inRange = both(greaterThanOrEqualTo(expirationLower)).and(lessThanOrEqualTo(expirationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            headersMatcher.withTtl(equalTo(UnsignedInteger.valueOf(ttl)));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withAbsoluteExpiryTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageWithJMS_AMQP_TTLSetPositive() throws Exception {
        sendingMessageWithJMS_AMQP_TTLSetTestImpl(100_000, 20_000);
    }

    @Test(timeout = 20000)
    public void testSendingMessageWithJMS_AMQP_TTLSetZero() throws Exception {
        sendingMessageWithJMS_AMQP_TTLSetTestImpl(50_000, 0);
    }

    public void sendingMessageWithJMS_AMQP_TTLSetTestImpl(long jmsTtl, long amqpTtl) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            long currentTime = System.currentTimeMillis();
            Date expirationLower = new Date(currentTime + jmsTtl);
            Date expirationUpper = new Date(currentTime + jmsTtl + 3000);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            Matcher<Date> inRange = both(greaterThanOrEqualTo(expirationLower)).and(lessThanOrEqualTo(expirationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            // verify the ttl field matches the JMS_AMQP_TTL value, rather than the standard JMS send TTL value.
            if (amqpTtl == 0) {
                headersMatcher.withTtl(nullValue());
            } else {
                headersMatcher.withTtl(equalTo(UnsignedInteger.valueOf(amqpTtl)));
            }
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withAbsoluteExpiryTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);
            message.setLongProperty(AmqpMessageSupport.JMS_AMQP_TTL, amqpTtl);

            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, jmsTtl);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Test that when a message is sent with default priority of 4, the emitted AMQP message has no value in the header
     * priority field, since the default for that field is already 4.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testDefaultPriorityProducesMessagesWithoutPriorityField() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withPriority(equalTo(null));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message);

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent with a non-default priority, the emitted AMQP message has that value in the
     * header priority field, and the JMS message has had JMSPriority set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsJMSPriority() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            byte priority = 5;

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withPriority(equalTo(UnsignedByte.valueOf(priority)));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message, DeliveryMode.PERSISTENT, priority, Message.DEFAULT_TIME_TO_LIVE);

            assertEquals(priority, message.getJMSPriority());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that upon sending a message, the sender sets the JMSMessageID on the Message object,
     * and that the value is included in the AMQP message sent by the client.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSMessageID() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a string, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);
            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected string message id to be sent", receivedMessageId instanceof String);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith((String)receivedMessageId));
        }
    }

    // TODO - Remove when the deprecated methods are removed.
    @Test(timeout=20000)
    public void testSendingMessageWithUUIDStringMessageFormatLegacy() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=UUID_STRING";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            Connection connection = factory.createConnection();
            testPeer.expectSaslAnonymousConnect();
            testPeer.expectBegin();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a String, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);

            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected UUID message id to be sent", receivedMessageId instanceof String);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith(receivedMessageId.toString()));
        }
    }

    @Test(timeout=20000)
    public void testSendingMessageWithUUIDStringMessageFormat() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=UUID_STRING";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            Connection connection = factory.createConnection();
            testPeer.expectSaslAnonymousConnect();
            testPeer.expectBegin();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a String, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);

            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected UUID message id to be sent", receivedMessageId instanceof String);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith(receivedMessageId.toString()));
        }
    }

    // TODO - Remove when the deprecated methods are removed.
    @Test(timeout=20000)
    public void testSendingMessageWithUUIDMessageFormatLegacy() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDType=UUID";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            Connection connection = factory.createConnection();
            testPeer.expectSaslAnonymousConnect();
            testPeer.expectBegin();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(UUID.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a UUID, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);

            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected UUID message id to be sent", receivedMessageId instanceof UUID);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith(receivedMessageId.toString()));
        }
    }

    @Test(timeout=20000)
    public void testSendingMessageWithUUIDMessageFormat() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=UUID";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            Connection connection = factory.createConnection();
            testPeer.expectSaslAnonymousConnect();
            testPeer.expectBegin();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(UUID.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a UUID, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);

            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected UUID message id to be sent", receivedMessageId instanceof UUID);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith(receivedMessageId.toString()));
        }
    }

    /**
     * Test that after sending a message with the disableMessageID hint set, the message
     * object has a null JMSMessageID value, and no message-id field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageIDHint() throws Exception {
        doSendingMessageWithDisableMessageIDHintTestImpl(false);
    }

    /**
     * Test that after sending a message with the disableMessageID hint set, which already had
     * a JMSMessageID value, that the message object then has a null JMSMessageID value, and no
     * message-id field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageIDHintAndExistingMessageID() throws Exception {
        doSendingMessageWithDisableMessageIDHintTestImpl(true);
    }

    private void doSendingMessageWithDisableMessageIDHintTestImpl(boolean existingId) throws JMSException, InterruptedException, Exception, IOException {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withMessageId(nullValue()); // Check there is no message-id value;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            if (existingId) {
                // [erroneously] set a JMSMessageID value
                String existingMessageId = "ID:this-should-be-overwritten-in-send";
                message.setJMSMessageID(existingMessageId);
                assertEquals("JMSMessageID should now be set", existingMessageId, message.getJMSMessageID());
            }

            producer.setDisableMessageID(true);
            producer.send(message);

            assertNull("JMSMessageID should be null", message.getJMSMessageID());

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseProducer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final AtomicBoolean producerClosed = new AtomicBoolean();
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onProducerClosed(MessageProducer producer, Exception exception) {
                    producerClosed.set(true);
                }
            });

            testPeer.expectBegin();
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
                public boolean isSatisified() throws Exception {
                    try {
                        producer.getDestination();
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

            assertTrue("Producer closed callback didn't trigger", producerClosed.get());

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseProducerDuringSyncSend() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

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
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage(text);

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSyncSend() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

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
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage(text);

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsDelayed() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.traceFrames=true&amqp.traceBytes=true");
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach. Delay sending credit when it does.
            testPeer.expectSenderAttach(100);

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);

            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsZeroAndTimeout() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");

            // Expect the producer to attach. Don't send any credit so that the client will
            // block on a send and we can test our timeouts.
            testPeer.expectSenderAttachWithoutGrantingCredit();
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

    @Test(timeout = 20000)
    public void testSendTimesOutWhenNoDispostionArrives() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();

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

    @Test(timeout = 20000)
    public void testSyncSendMessageRejected() throws Exception {
        doSyncSendMessageNotAcceptedTestImpl(new Rejected());
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageReleased() throws Exception {
        doSyncSendMessageNotAcceptedTestImpl(new Released());
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedDeliveryFailed() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setUndeliverableHere(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedDeliveryFailedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    private void doSyncSendMessageNotAcceptedTestImpl(ListDescribedType responseState) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createTextMessage("content");

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), false, responseState, true);
            testPeer.expectClose();

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            try {
                producer.send(message);
                fail("Expected an exception to be thrown");
            } catch (JMSException e) {
                //TODO: more explicit exception type?
                // Expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageRejected() throws Exception {
        doAsyncSendMessageNotAcceptedTestImpl(new Rejected());
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageReleased() throws Exception {
        doAsyncSendMessageNotAcceptedTestImpl(new Released());
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedDeliveryFailed() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setUndeliverableHere(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedDeliveryFailedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    private void doAsyncSendMessageNotAcceptedTestImpl(ListDescribedType responseState) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            final CountDownLatch asyncError = new CountDownLatch(1);

            connection.setForceAsyncSend(true);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    LOG.debug("ExceptionListener got error: {}", exception.getMessage());
                    asyncError.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create a second producer which allows for a safe wait for credit for the
            // first producer without the need for a sleep.  Otherwise the first producer
            // might not do an actual async send due to not having received credit yet.
            session.createProducer(queue);

            Message message = session.createTextMessage("content");

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), false, responseState, true);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            try {
                producer.send(message);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Should get a non-fatal error", asyncError.await(10, TimeUnit.SECONDS));

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            try {
                producer.send(message);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksWhenConnectionNotStarted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectDetach(true, true, true);
            producer.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksAfterConnectionStopped() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            connection.stop();

            producer.send(session.createMessage());

            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.close();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreditDrainedAfterSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // After the first send lets drain off the remaining credit from the sender
            testPeer.expectTransferRespondWithDrain(messageMatcher, 1);
            testPeer.expectLinkFlow(true, false, Matchers.equalTo(UnsignedInteger.ZERO));
            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.send(session.createMessage());

            // We don't have any credit now since we were drained, so the send should
            // block until more credit is issued.
            try {
                producer.send(session.createMessage());
                fail("Should have timed out waiting for credit to send.");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught expected send timeout.");
            }

            producer.close();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdSetWhenConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSetWhenNotConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenNotConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private class CustomForeignMessage extends ForeignJmsMessage {

        @Override
        public Enumeration<?> getPropertyNames() throws JMSException {
            Enumeration<?> properties = super.getPropertyNames();

            Set<Object> names = new HashSet<Object>();
            while (properties.hasMoreElements()) {
                names.add(properties.nextElement());
            }

            names.add("JMSXUserID");

            return Collections.enumeration(names);
        }

        @Override
        public Object getObjectProperty(String name) throws JMSException {
            if (name.equals("JMSXUserID")) {
                return "spoofed";
            }

            return message.getObjectProperty(name);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenConfiguredForInclusionWithForgeinMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = new CustomForeignMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenNotConfiguredForInclusionWithForeignMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlainConnect(user, pass, null, null);
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = new CustomForeignMessage();
            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
