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
package org.apache.qpid.jms;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.impl.DestinationHelper;
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
import org.apache.qpid.proton.amqp.DescribedType;
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

    @Test
    public void testSendTextMessageWithApplicationProperties() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
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
        }
    }

    @Test
    public void testReceiveTextMessageWithApplicationProperties() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

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
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, appProperties, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertNull(((TextMessage)receivedMessage).getText());

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

    /**
     * Tests that the {@link DestinationHelper#TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME} set on a message to
     * indicate its 'to' address represents a Topic results in the JMSDestination object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     */
    @Test
    public void testReceivedMessageFromQueueWithToTypeAnnotationForTopic() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, DestinationHelper.TOPIC_ATTRIBUTES_STRING);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setTo(myTopicAddress );
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
    @Test
    public void testReceivedMessageFromQueueWithoutToResultsInUseOfConsumerDestination() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
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
     * Tests that the {@link DestinationHelper#REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME} set on a message to
     * indicate its 'reply-to' address represents a Topic results in the JMSReplyTo object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     */
    @Test
    public void testReceivedMessageFromQueueWithReplyToTypeAnnotationForTopic() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, DestinationHelper.TOPIC_ATTRIBUTES_STRING);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setReplyTo(myTopicAddress);
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
     * Tests that lack of the {@link DestinationHelper#REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME} set on a
     * message to indicate type of its 'reply-to' address results in it being classed as the same
     * type as the destination used to create the consumer.
     */
    @Test
    public void testReceivedMessageFromQueueWithReplyToWithoutTypeAnnotationResultsInUseOfConsumerDestinationType() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            String myOtherQueueAddress = "myOtherQueueAddress";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setReplyTo(myOtherQueueAddress);

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
    @Test
    public void testReceivedMessageFromQueueWithNoReplyToReturnsNull() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
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
    @Test
    public void testReceivedMessageFromQueueWithNoAbsoluteExpiryOrTtlReturnsJMSExpirationZero() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
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
    @Test
    public void testReceivedMessageFromQueueWithAbsoluteExpiryReturnsJMSExpirationNonZero() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            long timestamp = System.currentTimeMillis();

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(timestamp));
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
}
