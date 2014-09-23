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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsClientProperties;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageIdHelper;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.ApplicationPropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.ApplicationPropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.Ignore;
import org.junit.Test;

public class MessageIntegrationTest extends QpidJmsTestCase
{
    private static final String NULL_STRING_PROP = "nullStringProperty";
    private static final String NULL_STRING_PROP_VALUE = null;
    private static final String STRING_PROP = "stringProperty";
    private static final String STRING_PROP_VALUE = "string";
    private static final String BOOLEAN_PROP = "booleanProperty";
    private static final boolean BOOLEAN_PROP_VALUE = true;
    private static final String BYTE_PROP = "byteProperty";
    private static final byte   BYTE_PROP_VALUE = (byte)1;
    private static final String SHORT_PROP = "shortProperty";
    private static final short  SHORT_PROP_VALUE = (short)1;
    private static final String INT_PROP = "intProperty";
    private static final int    INT_PROP_VALUE = Integer.MAX_VALUE;
    private static final String LONG_PROP = "longProperty";
    private static final long   LONG_PROP_VALUE = Long.MAX_VALUE;
    private static final String FLOAT_PROP = "floatProperty";
    private static final float  FLOAT_PROP_VALUE = Float.MAX_VALUE;
    private static final String DOUBLE_PROP = "doubleProperty";
    private static final double DOUBLE_PROP_VALUE = Double.MAX_VALUE;

    private final IntegrationTestFixture _testFixture = new IntegrationTestFixture();

    //TODO: use Message instead of TextMessage
    @Test(timeout = 2000)
    public void testSendTextMessageWithApplicationProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            ApplicationPropertiesSectionMatcher appPropsMatcher = new ApplicationPropertiesSectionMatcher(true);
            appPropsMatcher.withEntry(NULL_STRING_PROP, nullValue());
            appPropsMatcher.withEntry(STRING_PROP, equalTo(STRING_PROP_VALUE));
            appPropsMatcher.withEntry(BOOLEAN_PROP, equalTo(BOOLEAN_PROP_VALUE));
            appPropsMatcher.withEntry(BYTE_PROP, equalTo(BYTE_PROP_VALUE));
            appPropsMatcher.withEntry(SHORT_PROP, equalTo(SHORT_PROP_VALUE));
            appPropsMatcher.withEntry(INT_PROP, equalTo(INT_PROP_VALUE));
            appPropsMatcher.withEntry(LONG_PROP, equalTo(LONG_PROP_VALUE));
            appPropsMatcher.withEntry(FLOAT_PROP, equalTo(FLOAT_PROP_VALUE));
            appPropsMatcher.withEntry(DOUBLE_PROP, equalTo(DOUBLE_PROP_VALUE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));

            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);

            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withTo(equalTo(queueName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setApplicationPropertiesMatcher(appPropsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setStringProperty(NULL_STRING_PROP, NULL_STRING_PROP_VALUE);
            message.setStringProperty(STRING_PROP, STRING_PROP_VALUE);
            message.setBooleanProperty(BOOLEAN_PROP, BOOLEAN_PROP_VALUE);
            message.setByteProperty(BYTE_PROP, BYTE_PROP_VALUE);
            message.setShortProperty(SHORT_PROP, SHORT_PROP_VALUE);
            message.setIntProperty(INT_PROP, INT_PROP_VALUE);
            message.setLongProperty(LONG_PROP, LONG_PROP_VALUE);
            message.setFloatProperty(FLOAT_PROP, FLOAT_PROP_VALUE);
            message.setDoubleProperty(DOUBLE_PROP, DOUBLE_PROP_VALUE);

            producer.send(message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 2000)
    public void testReceiveMessageWithApplicationProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            ApplicationPropertiesDescribedType appProperties = new ApplicationPropertiesDescribedType();
            appProperties.setApplicationProperty(STRING_PROP, STRING_PROP_VALUE);
            appProperties.setApplicationProperty(NULL_STRING_PROP, NULL_STRING_PROP_VALUE);
            appProperties.setApplicationProperty(BOOLEAN_PROP, BOOLEAN_PROP_VALUE);
            appProperties.setApplicationProperty(BYTE_PROP, BYTE_PROP_VALUE);
            appProperties.setApplicationProperty(SHORT_PROP, SHORT_PROP_VALUE);
            appProperties.setApplicationProperty(INT_PROP, INT_PROP_VALUE);
            appProperties.setApplicationProperty(LONG_PROP, LONG_PROP_VALUE);
            appProperties.setApplicationProperty(FLOAT_PROP, FLOAT_PROP_VALUE);
            appProperties.setApplicationProperty(DOUBLE_PROP, DOUBLE_PROP_VALUE);

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, appProperties, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertTrue(receivedMessage.propertyExists(STRING_PROP));
            assertTrue(receivedMessage.propertyExists(NULL_STRING_PROP));
            assertTrue(receivedMessage.propertyExists(BYTE_PROP));
            assertTrue(receivedMessage.propertyExists(BOOLEAN_PROP));
            assertTrue(receivedMessage.propertyExists(SHORT_PROP));
            assertTrue(receivedMessage.propertyExists(INT_PROP));
            assertTrue(receivedMessage.propertyExists(LONG_PROP));
            assertTrue(receivedMessage.propertyExists(FLOAT_PROP));
            assertTrue(receivedMessage.propertyExists(DOUBLE_PROP));
            assertNull(receivedMessage.getStringProperty(NULL_STRING_PROP));
            assertEquals(STRING_PROP_VALUE, receivedMessage.getStringProperty(STRING_PROP));
            assertEquals(STRING_PROP_VALUE, receivedMessage.getStringProperty(STRING_PROP));
            assertEquals(BOOLEAN_PROP_VALUE, receivedMessage.getBooleanProperty(BOOLEAN_PROP));
            assertEquals(BYTE_PROP_VALUE, receivedMessage.getByteProperty(BYTE_PROP));
            assertEquals(SHORT_PROP_VALUE, receivedMessage.getShortProperty(SHORT_PROP));
            assertEquals(INT_PROP_VALUE, receivedMessage.getIntProperty(INT_PROP));
            assertEquals(LONG_PROP_VALUE, receivedMessage.getLongProperty(LONG_PROP));
            assertEquals(FLOAT_PROP_VALUE, receivedMessage.getFloatProperty(FLOAT_PROP), 0.0);
            assertEquals(DOUBLE_PROP_VALUE, receivedMessage.getDoubleProperty(DOUBLE_PROP), 0.0);
        }
    }

    @Ignore//TODO: currently fails due to NPE during delivery processing due to lack of message id
    @Test(timeout = 2000)
    public void testReceiveMessageWithoutMessageId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(2000);

            assertNull(receivedMessage.getJMSMessageID());
        }
    }

    /**
     * Tests that the {@link AmqpMessageSupport#AMQP_TO_ANNOTATION} set on a message to
     * indicate its 'to' address represents a Topic results in the JMSDestination object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     */
    @Ignore//TODO: currently fails due to handling of AmqpMessageSupport.AMQP_TO_ANNOTATION not being complete
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithToTypeAnnotationForTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.AMQP_TO_ANNOTATION, AmqpMessageSupport.TOPIC_ATTRIBUTES);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setTo(myTopicAddress );
            props.setMessageId("myMessageIDString");
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSDestination();
            assertTrue(dest instanceof Topic);
            assertEquals(myTopicAddress, ((Topic)dest).getTopicName());
        }
    }

    /**
     * Tests that the lack of a 'to' in the Properties section of the incoming message (e.g
     * one sent by a non-JMS client) is handled by making the JMSDestination method simply
     * return the Destination used to create the consumer that received the message.
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithoutToResultsInUseOfConsumerDestination() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSDestination();
            assertTrue(dest instanceof Queue);
            assertEquals(queueName, ((Queue)dest).getQueueName());
        }
    }

    /**
     * Tests that the {@link AmqpMessageSupport#AMQP_REPLY_TO_ANNOTATION} set on a message to
     * indicate its 'reply-to' address represents a Topic results in the JMSReplyTo object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithReplyToTypeAnnotationForTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.AMQP_REPLY_TO_ANNOTATION, AmqpMessageSupport.TOPIC_ATTRIBUTES);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setReplyTo(myTopicAddress);
            props.setMessageId("myMessageIDString");
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSReplyTo();
            assertTrue(dest instanceof Topic);
            assertEquals(myTopicAddress, ((Topic)dest).getTopicName());
        }
    }

    /**
     * Tests that lack of the {@link AmqpMessageSupport#AMQP_REPLY_TO_ANNOTATION} set on a
     * message to indicate type of its 'reply-to' address results in it being classed as the same
     * type as the destination used to create the consumer.
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithReplyToWithoutTypeAnnotationResultsInUseOfConsumerDestinationType() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            String myOtherQueueAddress = "myOtherQueueAddress";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setReplyTo(myOtherQueueAddress);
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSReplyTo();
            assertTrue(dest instanceof Queue);
            assertEquals(myOtherQueueAddress, ((Queue)dest).getQueueName());
        }
    }

    /**
     * Tests that lack of the reply-to set on a message results in it returning null for JMSReplyTo
     * and not the consumer destination as happens for JMSDestination.
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithNoReplyToReturnsNull() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertNull(receivedMessage.getJMSReplyTo());
        }
    }

    /**
     * Tests that lack of the absolute-expiry-time and ttl fields on a message results
     * in it returning 0 for for JMSExpiration
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithNoAbsoluteExpiryOrTtlReturnsJMSExpirationZero() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertEquals(0L, receivedMessage.getJMSExpiration());
        }
    }

    /**
     * Tests that setting a non-zero value in the absolute-expiry-time field on a
     * message results in it returning this value for JMSExpiration
     */
    @Test(timeout = 2000)
    public void testReceivedMessageFromQueueWithAbsoluteExpiryReturnsJMSExpirationNonZero() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            long timestamp = System.currentTimeMillis();

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(timestamp));
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertEquals(timestamp, receivedMessage.getJMSExpiration());
        }
    }

    /**
     * Tests that receiving a message with a string typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added.
     */
    @Test(timeout = 2000)
    public void testReceivedMessageWithStringMessageIdReturnsExpectedJMSMessageID() throws Exception {
        receivedMessageWithMessageIdTestImpl("myTestMessageIdString");
    }

    /**
     * Tests that receiving a message with a UUID typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added to the UUID.tostring()
     */
    @Ignore//TODO: failing because handling of non-String messageid values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithUUIDMessageIdReturnsExpectedJMSMessageID() throws Exception {
        receivedMessageWithMessageIdTestImpl(UUID.randomUUID());
    }

    /**
     * Tests that receiving a message with a ulong typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added to the UUID.tostring()
     */
    @Ignore//TODO: failing because handling of non-String messageid values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithLongMessageIdReturnsExpectedJMSMessageID() throws Exception {
        receivedMessageWithMessageIdTestImpl(BigInteger.valueOf(123456789L));
    }

    private void receivedMessageWithMessageIdTestImpl(Object messageIdForAmqpMessageClass) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Object underlyingAmqpMessageId = classifyUnderlyingIdType(messageIdForAmqpMessageClass);

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId(underlyingAmqpMessageId);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            String expectedBaseIdString = new AmqpMessageIdHelper().toBaseMessageIdString(messageIdForAmqpMessageClass);

            assertEquals("ID:" + expectedBaseIdString, receivedMessage.getJMSMessageID());
        }
    }

    /**
     * Tests that receiving a message with a string typed correlation-id results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix has been added.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because adding of the 'ID:' prefix to correlation-id values is not yet implemented
    public void testReceivedMessageWithStringCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        receivedMessageWithCorrelationIdTestImpl("myTestCorrelationIdString", false);
    }

    /**
     * Tests that receiving a message with a string typed correlation-id, which is indicated to be an
     * application-specific value, results in returning the expected value for JMSCorrelationID
     * where the JMS "ID:" prefix has NOT been added.
     */
    @Ignore//TODO: failing because the transformer code tries to set an illegal JMS property based on the 'x-opt-app-correlation-id' message annotation
           //TODO: would probably fail anyway because explicit handling based on that annotation is not implemented yet
    @Test(timeout = 2000)
    public void testReceivedMessageWithAppSpecificStringCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        receivedMessageWithCorrelationIdTestImpl("myTestCorrelationIdString", true);
    }

    /**
     * Tests that receiving a message with a UUID typed correlation-id results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix has been added to the UUID.tostring()
     */
    @Ignore//TODO: failing because handling of non-String correlation-id values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithUUIDCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        receivedMessageWithCorrelationIdTestImpl(UUID.randomUUID(), false);
    }

    /**
     * Tests that receiving a message with a UUID typed correlation-id results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix has been added to the UUID.tostring()
     */
    @Ignore//TODO: failing because handling of non-String correlation-id values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithLongCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        receivedMessageWithCorrelationIdTestImpl(BigInteger.valueOf(123456789L), false);
    }

    private void receivedMessageWithCorrelationIdTestImpl(Object correlationIdForAmqpMessageClass, boolean appSpecific) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Object underlyingAmqpCorrelationId = classifyUnderlyingIdType(correlationIdForAmqpMessageClass);

            PropertiesDescribedType props = new PropertiesDescribedType();
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);
            MessageAnnotationsDescribedType ann = null;

            props.setMessageId("myMessageIdString");
            props.setCorrelationId(underlyingAmqpCorrelationId);
            if (appSpecific) {
                ann = new MessageAnnotationsDescribedType();
                ann.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_APP_CORRELATION_ID, true);
            }

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, ann, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            String expectedBaseIdString = new AmqpMessageIdHelper().toBaseMessageIdString(correlationIdForAmqpMessageClass);
            String expected = expectedBaseIdString;
            if (!appSpecific) {
                expected = "ID:" + expected;
            }

            assertEquals(expected, receivedMessage.getJMSCorrelationID());
        }
    }

    /**
     * Tests that sending a message with a uuid typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is uuid, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present, and there is
     * no presence of the message annotation to indicate an app-specific correlation-id.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because handling of non-String correlation-id values is not yet implemented
    public void testSentMessageWithUUIDCorrelationId() throws Exception {
        UUID uuid = UUID.randomUUID();
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX +  uuid.toString();
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, uuid, false);
    }

    /**
     * Tests that sending a message with a binary typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is binary, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present, and there is
     * no presence of the message annotation to indicate an app-specific correlation-id.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because handling of non-String correlation-id values is not yet implemented
    public void testSentMessageWithBinaryCorrelationId() throws Exception
    {
        ByteBuffer bin = ByteBuffer.wrap(new byte[]{(byte)0x01, (byte)0x23, (byte) 0xAF, (byte) 0x00});
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +  "0123af00";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, bin, false);
    }

    /**
     * Tests that sending a message with a ulong typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is ulong, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present, and there is
     * no presence of the message annotation to indicate an app-specific correlation-id.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because handling of non-String correlation-id values is not yet implemented
    public void testSentMessageWithUlongCorrelationId() throws Exception {
        BigInteger ulong = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN);
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX +  ulong.toString();
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, ulong, false);
    }

    /**
     * Tests that sending a message with a string typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the "ID:" prefix of the JMSCorrelationID value is not present, and there is
     * no presence of the message annotation to indicate an app-specific correlation-id.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because removal of the 'ID:' prefix in correlation-id values is not yet implemented
    public void testSentMessageWithStringCorrelationId() throws Exception {
        String stringCorrelationId = "ID:myTestMessageIdString";
        String underlyingCorrelationId = "myTestMessageIdString";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, underlyingCorrelationId, false);
    }

    /**
     * Tests that sending a message with a string typed correlation-id value which is a
     * app-specific results in an AMQP message with the expected encoding of the correlation-id,
     * and the presence of the message annotation to indicate an app-specific correlation-id.
     */
    @Ignore//TODO: failing because handling of 'x-opt-app-correlation-id' annotation is not yet implemented
    @Test(timeout = 2000)
    public void testSentMessageWithAppSpecificStringCorrelationId() throws Exception {
        String stringCorrelationId = "myTestAppSpecificString";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, stringCorrelationId, true);
    }

    private void sentMessageWithCorrelationIdTestImpl(String stringCorrelationId, Object correlationIdForAmqpMessageClass, boolean appSpecific) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);

            //Set matcher to validate the correlation-id, and the annotation
            //presence+value if it is application-specific
            Object underlyingAmqpCorrelationId = classifyUnderlyingIdType(correlationIdForAmqpMessageClass);
            propsMatcher.withCorrelationId(equalTo(underlyingAmqpCorrelationId));
            if (appSpecific) {
                msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_APP_CORRELATION_ID), equalTo(Boolean.TRUE));
            }

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setJMSCorrelationID(stringCorrelationId);

            producer.send(message);

            testPeer.waitForAllHandlersToComplete(3000);

            //validate the annotation was not present if the value was a message-id
            if (!appSpecific) {
                assertFalse(msgAnnotationsMatcher.keyExistsInReceivedAnnotations(Symbol.valueOf(AmqpMessageSupport.JMS_APP_CORRELATION_ID)));
            }
        }
    }

    /**
     * Tests that receiving a message with a string typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     */
    @Ignore//TODO: failing because removal of the 'ID:' prefix in correlation-id values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithStringMessageIdAndSendValueAsCorrelationId() throws Exception {
        recieveMessageIdSendCorrelationIdTestImpl("myStringMessageId");
    }

    /**
     * Tests that receiving a message with a UUID typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     */
    @Test(timeout = 2000)
    @Ignore//TODO: failing because handling of non-String message/correlation-id values is not yet implemented
    public void testReceivedMessageWithUUIDMessageIdAndSendValueAsCorrelationId() throws Exception {
        recieveMessageIdSendCorrelationIdTestImpl(UUID.randomUUID());
    }

    /**
     * Tests that receiving a message with a ulong typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     */
    @Ignore//TODO: failing because handling of non-String message/correlation-id values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithUlongMessageIdAndSendValueAsCorrelationId() throws Exception {
        recieveMessageIdSendCorrelationIdTestImpl(BigInteger.valueOf(123456789L));
    }

    /**
     * Tests that receiving a message with a binary typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     */
    @Ignore//TODO: failing because handling of non-String message/correlation-id values is not yet implemented
    @Test(timeout = 2000)
    public void testReceivedMessageWithBinaryMessageIdAndSendValueAsCorrelationId() throws Exception {
        recieveMessageIdSendCorrelationIdTestImpl(ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0xCD, (byte) 0xEF, (byte) 0x01}));
    }

    private void recieveMessageIdSendCorrelationIdTestImpl(Object idForAmqpMessageClass) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Object underlyingAmqpMessageId = classifyUnderlyingIdType(idForAmqpMessageClass);

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId(underlyingAmqpMessageId);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            String expectedBaseIdString = new AmqpMessageIdHelper().toBaseMessageIdString(idForAmqpMessageClass);

            String jmsMessageID = receivedMessage.getJMSMessageID();
            assertEquals("ID:" + expectedBaseIdString, jmsMessageID);

            //Now take the received JMSMessageID, and send a message with it set
            //as the JMSCorrelationID and verify we get the same AMQP id as we started with.

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);

            //Set matcher to validate the correlation-id matches the previous message-id
            propsMatcher.withCorrelationId(equalTo(underlyingAmqpMessageId));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setJMSCorrelationID(jmsMessageID);

            producer.send(message);

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    private Object classifyUnderlyingIdType(Object idForAmqpMessageClass) {
        Object underlyingAmqpMessageId = idForAmqpMessageClass;

        if (underlyingAmqpMessageId instanceof BigInteger) {
            // Proton uses UnsignedLong
            underlyingAmqpMessageId = UnsignedLong.valueOf((BigInteger) underlyingAmqpMessageId);
        } else if (underlyingAmqpMessageId instanceof ByteBuffer) {
            // Proton uses Binary
            underlyingAmqpMessageId = Binary.create((ByteBuffer) underlyingAmqpMessageId);
        }

        return underlyingAmqpMessageId;
    }

    /**
     * Tests that when receiving a message with the group-id, reply-to-group-id, and group-sequence
     * fields of the AMQP properties section set, that the expected values are returned when getting
     * the appropriate JMSX or JMS_AMQP properties from the JMS message.
     */
    @Ignore//TODO: failing because the JMS_AMQP_REPLY_TO_GROUP_ID property handling is not yet wired up.
    @Test(timeout = 2000)
    public void testReceivedMessageWithGroupRelatedPropertiesSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);
            MessageAnnotationsDescribedType ann = null;

            String expectedGroupId = "myGroupId123";
            int expectedGroupSeq = 1;
            String expectedReplyToGroupId = "myReplyToGroupId456";

            props.setGroupId(expectedGroupId);
            props.setGroupSequence(UnsignedInteger.valueOf(expectedGroupSeq));
            props.setReplyToGroupId(expectedReplyToGroupId);
            props.setMessageId("myMessageIDString");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, ann, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull("did not receive the message", receivedMessage);
            assertEquals("did not get the expected JMSXGroupID", expectedGroupId, receivedMessage.getStringProperty(JmsClientProperties.JMSXGROUPID));
            assertEquals("did not get the expected JMSXGroupSeq", expectedGroupSeq, receivedMessage.getIntProperty(JmsClientProperties.JMSXGROUPSEQ));
            assertEquals("did not get the expected JMS_AMQP_REPLY_TO_GROUP_ID", expectedReplyToGroupId, receivedMessage.getStringProperty(AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID));
        }
    }

    /**
     * Tests that when sending a message with the JMSXGroupID, JMSXGroupSeq, and JMS_AMQP_REPLY_TO_GROUP_ID
     * properties of the JMS message set, that the expected values are included in the fields of
     * the AMQP message emitted.
     */
    @Ignore//TODO: failing because the JMSXGROUPID etc property handling is not yet wired up.
    @Test(timeout = 2000)
    public void testSendMessageWithGroupRelatedPropertiesSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = _testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);

            String expectedGroupId = "myGroupId123";
            int expectedGroupSeq = 1;
            String expectedReplyToGroupId = "myReplyToGroupId456";

            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withGroupId(equalTo(expectedGroupId));
            propsMatcher.withReplyToGroupId(equalTo(expectedReplyToGroupId));
            propsMatcher.withGroupSequence(equalTo(UnsignedInteger.valueOf(expectedGroupSeq)));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setStringProperty(JmsClientProperties.JMSXGROUPID, expectedGroupId);
            message.setIntProperty(JmsClientProperties.JMSXGROUPSEQ, expectedGroupSeq);
            message.setStringProperty(AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID, expectedReplyToGroupId);

            producer.send(message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
