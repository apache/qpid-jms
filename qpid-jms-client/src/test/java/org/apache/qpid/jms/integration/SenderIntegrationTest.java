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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Ignore;
import org.junit.Test;

public class SenderIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 10000)
    public void testCloseSender() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            testPeer.expectDetach(true);
            producer.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 10000)
    public void testDefaultDeliveryModeProducesDurableMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
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

            Message message = session.createTextMessage();

            producer.send(message);
            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 10000)
    public void testProducerOverridesMessageDeliveryMode() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
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

            Message message = session.createTextMessage();
            message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());

            producer.send(message);

            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent the JMSDestination header is set to the Destination used by the
     * producer, and the emitted AMQP message has the relevant value set in the 'to' field of properties,
     * with associated message annotation value to indicate the Destination type.
     */
    @Test(timeout = 10000)
    public void testSendingMessageSetsJMSDestination() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true)
                    .withEntry(Symbol.valueOf(AmqpMessageSupport.AMQP_TO_ANNOTATION),
                            equalTo(AmqpMessageSupport.QUEUE_ATTRIBUTES));
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withTo(equalTo(queueName));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage(text);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            producer.send(message);

            assertEquals("Should have had JMSDestination set", queue, message.getJMSDestination());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 10000)
    public void testSendingMessageSetsJMSTimestamp() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            // Add matcher to expect the creation time field of the properties section to be set to a value greater than
            // or equal to 'now'
            Date currentTime = Calendar.getInstance().getTime();
            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withCreationTime(greaterThanOrEqualTo(currentTime));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage(text);

            producer.send(message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Ignore
    // TODO: currently failing as we arent setting the ttl field of header until later in the JMS converter layer, so the value doesnt match expectation
    @Test(timeout = 10000)
    public void testSendingMessageSetsJMSExpirationRelatedAbsoluteExpiryAndTtlFields() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            long currentTime = System.currentTimeMillis();
            long ttl = 100_000;
            Date expiration = new Date(currentTime + ttl);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            headersMatcher.withTtl(equalTo(UnsignedInteger.valueOf(ttl)));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true)
                    .withAbsoluteExpiryTime(greaterThanOrEqualTo(expiration));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage(text);

            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent with default priority of 4, the emitted AMQP message has no value in the header
     * priority field, since the default for that field is already 4.
     */
    @Ignore
    // TODO: currently failing as we always populate the field
    @Test(timeout = 10000)
    public void testDefaultPriorityProducesMessagesWithoutPriorityField() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
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

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message);

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent with a non-default priority, the emitted AMQP message has that value in the
     * header priority field, and the JMS message has had JMSPriority set.
     */
    @Test(timeout = 10000)
    public void testNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsJMSPriority() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
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

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message, DeliveryMode.PERSISTENT, priority, Message.DEFAULT_TIME_TO_LIVE);

            assertEquals(priority, message.getJMSPriority());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that upon sending a message, the sender sets the JMSMessageID on the Message object,
     * and that the value is included in the AMQP message sent by the client.
     */
    @Test(timeout = 10000)
    public void testSendingMessageSetsJMSMessageID() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
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

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            //Get the value that was actually transmitted/received, verify it is a string, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);
            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected string message id to be sent", receivedMessageId instanceof String);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith((String)receivedMessageId));
        }
    }
}
