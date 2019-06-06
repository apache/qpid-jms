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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.QUEUE_PREFIX;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TOPIC_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsClientProperties;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageIdHelper;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.ApplicationPropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
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

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testReceiveMessageAndGetBody() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectClose();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertTrue(receivedMessage.isBodyAssignableTo(Object.class));
            assertTrue(receivedMessage.isBodyAssignableTo(String.class));
            assertTrue(receivedMessage.isBodyAssignableTo(byte[].class));
            assertTrue(receivedMessage.isBodyAssignableTo(Serializable.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Map.class));

            assertNull(receivedMessage.getBody(Object.class));
            assertNull(receivedMessage.getBody(String.class));
            assertNull(receivedMessage.getBody(byte[].class));
            assertNull(receivedMessage.getBody(Serializable.class));
            assertNull(receivedMessage.getBody(Map.class));

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    //==== Application Properties Section ====
    //========================================

    @Test(timeout = 20000)
    public void testSendMessageWithApplicationProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
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
            //TODO: currently we aren't sending any body section, decide if this is allowed
            //messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
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

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveMessageWithApplicationProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

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
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.expectClose();
            connection.close();

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

    @Test(timeout = 20000)
    public void testReceiveMessageWithInvalidPropertyName() throws Exception {
        doReceiveMessageWithInvalidPropertyNameTestImpl(false);
    }

    @Test(timeout = 20000)
    public void testReceiveMessageWithInvalidPropertyNameAndWithValidationDisabled() throws Exception {
        doReceiveMessageWithInvalidPropertyNameTestImpl(true);
    }

    private void doReceiveMessageWithInvalidPropertyNameTestImpl(boolean disableValidation) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.validatePropertyNames=" + !disableValidation);
            connection.start();

            testPeer.expectBegin();

            String invalidPropName = "invalid-name";
            String value = "valueA";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            ApplicationPropertiesDescribedType appProperties = new ApplicationPropertiesDescribedType();
            appProperties.setApplicationProperty(invalidPropName, value);

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, appProperties, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            if(!disableValidation) {
                assertFalse("Expected property to be indicated as not existing", receivedMessage.propertyExists(invalidPropName));

                try {
                    receivedMessage.getStringProperty(invalidPropName);
                    fail("Expected exception to be thrown");
                } catch (IllegalArgumentException iae) {
                    // expected
                }
            } else {
                assertTrue(receivedMessage.propertyExists(invalidPropName));
                assertEquals(value, receivedMessage.getStringProperty(invalidPropName));
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testSendMessageWithInvalidPropertyName() throws Exception {
        doSendMessageWithInvalidPropertyNameTestImpl(false);
    }

    @Test(timeout = 20000)
    public void testSendMessageWithInvalidPropertyNameAndWithValidationDisabled() throws Exception {
        doSendMessageWithInvalidPropertyNameTestImpl(true);
    }

    private void doSendMessageWithInvalidPropertyNameTestImpl(boolean disableValidation) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.validatePropertyNames=" + !disableValidation);
            connection.start();

            testPeer.expectBegin();

            String invalidPropName = "invalid-name";
            String value = "valueA";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Message message = session.createMessage();

            if (!disableValidation) {
                try {
                    message.setStringProperty(invalidPropName, value);
                    fail("Expected exception to be thrown");
                } catch (IllegalArgumentException iae) {
                    // expected
                }
            } else {
                message.setStringProperty(invalidPropName, value);

                MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
                MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
                MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
                ApplicationPropertiesSectionMatcher appPropsMatcher = new ApplicationPropertiesSectionMatcher(true);
                appPropsMatcher.withEntry(invalidPropName, equalTo(value));

                TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
                messageMatcher.setHeadersMatcher(headersMatcher);
                messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
                messageMatcher.setPropertiesMatcher(propsMatcher);

                testPeer.expectSenderAttach();

                MessageProducer producer = session.createProducer(queue);

                testPeer.expectTransfer(messageMatcher);

                producer.send(message);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    //==== Destination Handling ====
    //==============================

    // --- missing to/reply-to field values --- //

    /**
     * Tests that the lack of a 'to' in the Properties section of the incoming message (e.g
     * one sent by a non-JMS client) is handled by making the JMSDestination method simply
     * return the Queue Destination used to create the consumer that received the message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithoutToResultsInUseOfConsumerDestinationQueue() throws Exception {
        receivedMessageFromQueueWithoutToResultsInUseOfConsumerDestinationImpl(true);
    }

    /**
     * Tests that the lack of a 'to' in the Properties section of the incoming message (e.g
     * one sent by a non-JMS client) is handled by making the JMSDestination method simply
     * return the Topic Destination used to create the consumer that received the message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithoutToResultsInUseOfConsumerDestinationTopic() throws Exception {
        receivedMessageFromQueueWithoutToResultsInUseOfConsumerDestinationImpl(false);
    }

    public void receivedMessageFromQueueWithoutToResultsInUseOfConsumerDestinationImpl(boolean useQueue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            String queueName = "myQueue";
            String topicName = "myTopic";
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = null;
            if (useQueue) {
                destination = session.createQueue(queueName);
            } else {
                destination = session.createTopic(topicName);
            }

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(destination);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSDestination();
            if (useQueue) {
                assertNotNull("expected Queue instance, got null", dest);
                assertTrue("expected Queue instance. Actual type was: " + dest.getClass().getName(), dest instanceof Queue);
                assertEquals(queueName, ((Queue) dest).getQueueName());
            } else {
                assertNotNull("expected Topic instance, got null", dest);
                assertTrue("expected Topic instance. Actual type was: " + dest.getClass().getName(), dest instanceof Topic);
                assertEquals(topicName, ((Topic) dest).getTopicName());
            }
        }
    }

    /**
     * Tests that lack of the reply-to set on a message results in it returning null for JMSReplyTo
     * and not the consumer destination as happens for JMSDestination.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithNoReplyToReturnsNull() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertNull(receivedMessage.getJMSReplyTo());
        }
    }

    // --- destination prefix handling --- //

    /**
     * Tests that the a connection with a 'topic prefix' set on it strips the
     * prefix from the content of the to/reply-to fields for incoming messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithTopicDestinationsOnConnectionWithTopicPrefix() throws Exception {
        Class<? extends Destination> destType = Topic.class;
        String destPrefix = "t12321-";
        String destName = "myTopic";
        String replyName = "myReplyTopic";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.TOPIC_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.TOPIC_TYPE;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that the a connection with a 'topic prefix' set on it strips the
     * prefix from the content of the to/reply-to fields for incoming messages
     * if they don't have the 'destination type annotation' set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithNoTypeAnnotationAndTopicDestinationsOnConnectionWithTopicPrefix() throws Exception {
        Class<? extends Destination> destType = Topic.class;
        String destPrefix = "t12321-";
        String destName = "myTopic";
        String replyName = "myReplyTopic";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = null;
        Byte annotationValue = null;
        String replyAnnotationName = null;
        Byte replyAnnotationValue = null;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that the a connection with a 'queue prefix' set on it strips the
     * prefix from the content of the to/reply-to fields for incoming messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithQueueDestinationsOnConnectionWithQueuePrefix() throws Exception {
        Class<? extends Destination> destType = Queue.class;
        String destPrefix = "q12321-";
        String destName = "myQueue";
        String replyName = "myReplyQueue";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.QUEUE_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.QUEUE_TYPE;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that the a connection with a 'queue prefix' set on it strips the
     * prefix from the content of the to/reply-to fields for incoming messages
     * if they don't have the 'destination type annotation' set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithNoTypeAnnotationAndQueueDestinationsOnConnectionWithQueuePrefix() throws Exception {
        Class<? extends Destination> destType = Queue.class;
        String destPrefix = "q12321-";
        String destName = "myQueue";
        String replyName = "myReplyQueue";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = null;
        Byte annotationValue = null;
        String replyAnnotationName = null;
        Byte replyAnnotationValue = null;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that a connection with a 'prefixes' set on its does not alter the
     * address for a temporary queue in the to/reply-to fields for incoming messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithTemporaryQueueDestinationsOnConnectionWithPrefixes() throws Exception {
        Class<? extends Destination> destType = TemporaryQueue.class;
        String destPrefix = "q12321-";
        String destName = "temp-queue://myTempQueue";
        String replyName = "temp-queue://myReplyTempQueue";
        String destAddress = destName; // We won't manipulate the temporary addresses generated by the broker
        String replyAddress = replyName; // We won't manipulate the temporary addresses generated by the broker
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.TEMP_QUEUE_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.TEMP_QUEUE_TYPE;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that a connection with a 'prefixes' set on its does not alter the
     * address for a temporary queue in the to/reply-to fields for incoming messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithTemporaryTopicDestinationsOnConnectionWithPrefixes() throws Exception {
        Class<? extends Destination> destType = TemporaryTopic.class;
        String destPrefix = "q12321-";
        String destName = "temp-topic://myTempTopic";
        String replyName = "temp-topic://myReplyTempTopic";
        String destAddress = destName; // We won't manipulate the temporary addresses generated by the broker
        String replyAddress = replyName; // We won't manipulate the temporary addresses generated by the broker
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.TEMP_TOPIC_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.TEMP_TOPIC_TYPE;

        doReceivedMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, replyName,
                                                        destAddress, replyAddress, annotationName,
                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    private void doReceivedMessageOnConnectionWithPrefixTestImpl(Class<? extends Destination> destType,
                                                                 String destPrefix,
                                                                 String destName,
                                                                 String replyName,
                                                                 String destAddress,
                                                                 String replyAddress,
                                                                 String annotationName,
                                                                 Object annotationValue,
                                                                 String replyAnnotationName,
                                                                 Object replyAnnotationValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = null;
            if (destType == Topic.class) {
                connection = testFixture.establishConnecton(testPeer, "?jms.topicPrefix=" + destPrefix);
            } else if (destType == Queue.class) {
                connection = testFixture.establishConnecton(testPeer, "?jms.queuePrefix=" + destPrefix);
            } else {
                //Set both the non-temporary prefixes, we wont use non-temp dests but want to ensure they don't affect anything
                connection = testFixture.establishConnecton(testPeer, "?jms.topicPrefix=" + destPrefix + "&jms.queuePrefix=" + destPrefix);
            }

            connection.start();

            // Set the prefix if Topic or Queue dest type.
            if (destType == Topic.class) {
                ((JmsConnection) connection).setTopicPrefix(destPrefix);
            } else if (destType == Queue.class) {
                ((JmsConnection) connection).setQueuePrefix(destPrefix);
            }

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination
            Destination dest = null;
            if (destType == Topic.class) {
                dest= session.createTopic(destName);
            } else if (destType == Queue.class) {
                dest = session.createQueue(destName);
            } else if (destType == TemporaryTopic.class) {
                testPeer.expectTempTopicCreationAttach(destAddress);
                dest = session.createTemporaryTopic();
            } else if (destType == TemporaryQueue.class) {
                testPeer.expectTempQueueCreationAttach(destAddress);
                dest = session.createTemporaryQueue();
            }

            MessageAnnotationsDescribedType msgAnnotations = null;
            if (annotationName != null || replyAnnotationName != null) {
                msgAnnotations = new MessageAnnotationsDescribedType();
                if (annotationName != null) {
                    msgAnnotations.setSymbolKeyedAnnotation(annotationName, annotationValue);
                }

                if (replyAnnotationName != null) {
                    msgAnnotations.setSymbolKeyedAnnotation(replyAnnotationName, replyAnnotationValue);
                }
            }

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setTo(destAddress);
            props.setReplyTo(replyAddress);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(destAddress));

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(dest);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.waitForAllHandlersToComplete(2000);
            assertNotNull(receivedMessage);

            Destination jmsDest = receivedMessage.getJMSDestination();
            Destination jmsReplyTo = receivedMessage.getJMSReplyTo();

            assertNotNull("Expected JMSDestination but got null", jmsDest);
            assertNotNull("Expected JMSReplyTo but got null", jmsReplyTo);

            // Verify destination/replyto names on received message
            String recievedName = null;
            String recievedReplyName = null;
            if (destType == Topic.class || destType == TemporaryTopic.class) {
                recievedName = ((Topic) jmsDest).getTopicName();
                recievedReplyName = ((Topic) jmsReplyTo).getTopicName();
            } else if (destType == Queue.class || destType == TemporaryQueue.class) {
                recievedName = ((Queue) jmsDest).getQueueName();
                recievedReplyName = ((Queue) jmsReplyTo).getQueueName();
            }

            assertEquals("Unexpected name for JMSDestination", destName, recievedName);
            assertEquals("Unexpected name for JMSReplyTo", replyName, recievedReplyName);

            if (destType == TemporaryQueue.class || destType == TemporaryTopic.class) {
                assertEquals("Temporary destination name and address should be equal", destName, destAddress);
                assertEquals("Temporary replyto name and address should be equal", replyName, replyAddress);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that the a connection with a 'topic prefix' set on it adds the
     * prefix to the content of the to/reply-to fields for outgoing messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithTopicDestinationsOnConnectionWithTopicPrefix() throws Exception {
        Class<? extends Destination> destType = Topic.class;
        String destPrefix = "t12321-";
        String destName = "myTopic";
        String destAddress = destPrefix + destName;
        Byte annotationValue = AmqpDestinationHelper.TOPIC_TYPE;

        doSendMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    /**
     * Tests that the a connection with a 'queue prefix' set on it adds the
     * prefix to the content of the to/reply-to fields for outgoing messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithQueueDestinationsOnConnectionWithQueuePrefix() throws Exception {
        Class<? extends Destination> destType = Queue.class;
        String destPrefix = "q12321-";
        String destName = "myQueue";
        String destAddress = destPrefix + destName;
        Byte annotationValue = AmqpDestinationHelper.QUEUE_TYPE;

        doSendMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    /**
     * Tests that the a connection with 'destination prefixes' set on it does not add
     * the prefix to the content of the to/reply-to fields for TemporaryQueues.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithTemporaryQueueDestinationsOnConnectionWithDestinationPrefixes() throws Exception {
        Class<? extends Destination> destType = TemporaryQueue.class;
        String destPrefix = "q12321-";
        String destName = null;
        String destAddress = "temp-queue://myTempQueue";
        Byte annotationValue = AmqpDestinationHelper.TEMP_QUEUE_TYPE;

        doSendMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    /**
     * Tests that the a connection with 'destination prefixes' set on it does not add
     * the prefix to the content of the to/reply-to fields for TemporaryTopics.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithTemporaryTopicDestinationsOnConnectionWithDestinationPrefixes() throws Exception {
        Class<? extends Destination> destType = TemporaryTopic.class;
        String destPrefix = "q12321-";
        String destName = null;
        String destAddress = "temp-topic://myTempTopic";
        Byte annotationValue = AmqpDestinationHelper.TEMP_TOPIC_TYPE;

        doSendMessageOnConnectionWithPrefixTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    private void doSendMessageOnConnectionWithPrefixTestImpl(Class<? extends Destination> destType,
                                                             String destPrefix,
                                                             String destName,
                                                             String destAddress,
                                                             Byte destTypeAnnotationValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = null;
            if (destType == Topic.class) {
                connection = testFixture.establishConnecton(testPeer, "?jms.topicPrefix=" + destPrefix);
            } else if (destType == Queue.class) {
                connection = testFixture.establishConnecton(testPeer, "?jms.queuePrefix=" + destPrefix);
            } else {
                // Set both the non-temporary prefixes, we wont use non-temp dests but want to ensure they don't affect anything
                connection = testFixture.establishConnecton(testPeer, "?jms.topicPrefix=" + destPrefix + "&jms.queuePrefix=" + destPrefix);
            }

            connection.start();

            // Set the prefix if Topic or Queue dest type.
            if (destType == Topic.class) {
                ((JmsConnection) connection).setTopicPrefix(destPrefix);
            } else if (destType == Queue.class) {
                ((JmsConnection) connection).setQueuePrefix(destPrefix);
            }

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination
            Destination dest = null;
            if (destType == Topic.class) {
                dest = session.createTopic(destName);
            } else if (destType == Queue.class) {
                dest = session.createQueue(destName);
            } else if (destType == TemporaryTopic.class) {
                testPeer.expectTempTopicCreationAttach(destAddress);
                dest = session.createTemporaryTopic();
            } else if (destType == TemporaryQueue.class) {
                testPeer.expectTempQueueCreationAttach(destAddress);
                dest = session.createTemporaryQueue();
            }

            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(destAddress));

            testPeer.expectSenderAttach(targetMatcher, false, false);

            MessageProducer producer = session.createProducer(dest);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString()), equalTo(destTypeAnnotationValue));
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString()), equalTo(destTypeAnnotationValue));
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withTo(equalTo(destAddress));
            propsMatcher.withReplyTo(equalTo(destAddress));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);

            //TODO: currently we aren't sending any body section, decide if this is allowed
            //messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setJMSReplyTo(dest);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Tests that a connection with 'prefixes' set on it via broker-provided connection properties
     * strips the prefix from the to/reply-to fields for incoming messages with Topic destinations.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithTopicDestinationsOnConnectionWithBrokerDefinedPrefixProperties() throws Exception {
        Class<? extends Destination> destType = Topic.class;
        String destPrefix = "t-broker-provided-prefix-";
        String destName = "myTopic";
        String replyName = "myReplyTopic";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.TOPIC_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.TOPIC_TYPE;

        doReceivedMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(destType, destPrefix, destName, replyName,
                                                                        destAddress, replyAddress, annotationName,
                                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    /**
     * Tests that a connection with 'prefixes' set on it via broker-provided connection properties
     * strips the prefix from the to/reply-to fields for incoming messages with Queue destinations.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithQueueDestinationsOnConnectionWithBrokerDefinedPrefixProperties() throws Exception {
        Class<? extends Destination> destType = Queue.class;
        String destPrefix = "q-broker-provided-prefix-";
        String destName = "myQueue";
        String replyName = "myReplyQueue";
        String destAddress = destPrefix + destName;
        String replyAddress = destPrefix + replyName;
        String annotationName = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte annotationValue = AmqpDestinationHelper.QUEUE_TYPE;
        String replyAnnotationName = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString();
        Byte replyAnnotationValue = AmqpDestinationHelper.QUEUE_TYPE;

        doReceivedMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(destType, destPrefix, destName, replyName,
                                                                        destAddress, replyAddress, annotationName,
                                                                        annotationValue, replyAnnotationName, replyAnnotationValue);
    }

    private void doReceivedMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(Class<? extends Destination> destType,
                                                                                  String destPrefix,
                                                                                  String destName,
                                                                                  String replyName,
                                                                                  String destAddress,
                                                                                  String replyAddress,
                                                                                  String annotationName,
                                                                                  Object annotationValue,
                                                                                  String replyAnnotationName,
                                                                                  Object replyAnnotationValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Have the test peer provide the destination prefixes as connection properties
            Map<Symbol, Object> properties = new HashMap<Symbol, Object>();
            properties.put(QUEUE_PREFIX, destPrefix);
            properties.put(TOPIC_PREFIX, destPrefix);

            Connection connection = testFixture.establishConnecton(testPeer, null, null, properties);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination
            Destination dest = null;
            if (destType == Topic.class) {
                dest= session.createTopic(destName);
            } else if (destType == Queue.class) {
                dest = session.createQueue(destName);
            } else {
                fail("non-temporary destination type set");
            }

            MessageAnnotationsDescribedType msgAnnotations = null;
            if (annotationName != null || replyAnnotationName != null) {
                msgAnnotations = new MessageAnnotationsDescribedType();
                if (annotationName != null) {
                    msgAnnotations.setSymbolKeyedAnnotation(annotationName, annotationValue);
                }

                if (replyAnnotationName != null) {
                    msgAnnotations.setSymbolKeyedAnnotation(replyAnnotationName, replyAnnotationValue);
                }
            }

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setTo(destAddress);
            props.setReplyTo(replyAddress);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(destAddress));

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(dest);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.waitForAllHandlersToComplete(2000);
            assertNotNull(receivedMessage);

            Destination jmsDest = receivedMessage.getJMSDestination();
            Destination jmsReplyTo = receivedMessage.getJMSReplyTo();

            assertNotNull("Expected JMSDestination but got null", jmsDest);
            assertNotNull("Expected JMSReplyTo but got null", jmsReplyTo);

            // Verify destination/replyto names on received message
            String recievedName = null;
            String recievedReplyName = null;
            if (destType == Topic.class) {
                recievedName = ((Topic) jmsDest).getTopicName();
                recievedReplyName = ((Topic) jmsReplyTo).getTopicName();
            } else if (destType == Queue.class) {
                recievedName = ((Queue) jmsDest).getQueueName();
                recievedReplyName = ((Queue) jmsReplyTo).getQueueName();
            }

            assertEquals("Unexpected name for JMSDestination", destName, recievedName);
            assertEquals("Unexpected name for JMSReplyTo", replyName, recievedReplyName);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that the a connection with a 'queue prefix' set on it via broker-provided connection
     * properties adds the prefix to the content of the to/reply-to fields for outgoing messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithQueueDestinationsOnConnectionWithBrokerDefinedPrefixProperties() throws Exception {
        Class<? extends Destination> destType = Queue.class;
        String destPrefix = "q-broker-provided-prefix-";
        String destName = "myQueue";
        String destAddress = destPrefix + destName;
        Byte annotationValue = AmqpDestinationHelper.QUEUE_TYPE;

        doSendMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    /**
     * Tests that the a connection with a 'topic prefix' set on it via broker-provided connection
     * properties adds the prefix to the content of the to/reply-to fields for outgoing messages.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithTopicDestinationsOnConnectionWithBrokerDefinedPrefixProperties() throws Exception {
        Class<? extends Destination> destType = Topic.class;
        String destPrefix = "t-broker-provided-prefix-";
        String destName = "myTopic";
        String destAddress = destPrefix + destName;
        Byte annotationValue = AmqpDestinationHelper.TOPIC_TYPE;

        doSendMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(destType, destPrefix, destName, destAddress, annotationValue);
    }

    private void doSendMessageOnConnectionWithBrokerDefinedPrefixPropertiesTestImpl(Class<? extends Destination> destType,
                                                             String destPrefix,
                                                             String destName,
                                                             String destAddress,
                                                             Byte destTypeAnnotationValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Have the test peer provide the destination prefixes as connection properties
            Map<Symbol, Object> properties = new HashMap<Symbol, Object>();
            properties.put(QUEUE_PREFIX, destPrefix);
            properties.put(TOPIC_PREFIX, destPrefix);

            Connection connection = testFixture.establishConnecton(testPeer, null, null, properties);

            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination
            Destination dest = null;
            if (destType == Topic.class) {
                dest = session.createTopic(destName);
            } else if (destType == Queue.class) {
                dest = session.createQueue(destName);
            } else {
                fail("non-temporary destination type set");
            }

            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(destAddress));

            testPeer.expectSenderAttach(targetMatcher, false, false);

            MessageProducer producer = session.createProducer(dest);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL, equalTo(destTypeAnnotationValue));
            msgAnnotationsMatcher.withEntry(AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL, equalTo(destTypeAnnotationValue));
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withTo(equalTo(destAddress));
            propsMatcher.withReplyTo(equalTo(destAddress));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);

            //TODO: currently we aren't sending any body section, decide if this is allowed
            //messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setJMSReplyTo(dest);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    // --- missing destination type annotation values --- //

    /**
     * Tests that lack of any destination type annotation value (via either
     * {@link AmqpDestinationHelper#JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL}
     * or {@link AmqpMessageSupport#LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL}) set
     * on a message to indicate type of its 'reply-to' address results in it
     * being classed as the same type as the consumer destination.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromTopicWithReplyToWithoutTypeAnnotationResultsInUseOfConsumerDestinationType() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("myTopic");

            String myReplyTopicAddress = "myReplyTopicAddress";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setReplyTo(myReplyTopicAddress);
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(topic);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSReplyTo();
            assertNotNull("JMSReplyTo should not be null", dest);
            assertTrue("Destination not of expected type: " + dest.getClass(), dest instanceof Topic);
            assertEquals(myReplyTopicAddress, ((Topic)dest).getTopicName());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // --- byte destination type annotation values --- //

    /**
     * Tests that the {@link AmqpDestinationHelper#JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL} is set as a byte on
     * a sent message to indicate its 'to' address represents a Topic JMSDestination.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageContainsToTypeAnnotationByte() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String topicName = "myTopic";

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            Symbol annotationKey = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL;
            msgAnnotationsMatcher.withEntry(annotationKey, equalTo(AmqpDestinationHelper.TOPIC_TYPE));

            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withTo(equalTo(topicName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            Topic topic = session.createTopic(topicName);
            MessageProducer producer = session.createProducer(topic);
            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Tests that the {@link AmqpDestinationHelper#JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL} is set as a byte on
     * a sent message to indicate its 'reply-to' address represents a Topic JMSDestination.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageContainsReplyToTypeAnnotationByte() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            String replyTopicName = "myReplyTopic";

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            Symbol annotationKey = AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL;
            msgAnnotationsMatcher.withEntry(annotationKey, equalTo(AmqpDestinationHelper.TOPIC_TYPE));

            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withReplyTo(equalTo(replyTopicName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Topic replyTopic = session.createTopic(replyTopicName);
            Message message = session.createMessage();
            message.setJMSReplyTo(replyTopic);

            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    // --- old string destination type annotation values --- //

    /**
     * Tests that the {@link AmqpMessageSupport#LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL} set on a message to
     * indicate its 'to' address represents a Topic results in the JMSDestination object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithToLegacyTypeAnnotationForTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString(), AmqpMessageSupport.LEGACY_TOPIC_ATTRIBUTE);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setTo(myTopicAddress );
            props.setMessageId("myMessageIDString");
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSDestination();
            assertNotNull("Expected Topic destination but got null", dest);
            assertTrue("Expected Topic instance but did not get one. Actual type was: " + dest.getClass().getName(), dest instanceof Topic);
            assertEquals(myTopicAddress, ((Topic)dest).getTopicName());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that the {@link AmqpMessageSupport#LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL} set on a message to
     * indicate its 'reply-to' address represents a Topic results in the JMSReplyTo object being a
     * Topic. Ensure the consumers destination is not used by consuming from a Queue.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithLegacyReplyToTypeAnnotationForTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL.toString(), AmqpMessageSupport.LEGACY_TOPIC_ATTRIBUTE);

            PropertiesDescribedType props = new PropertiesDescribedType();
            String myTopicAddress = "myTopicAddress";
            props.setReplyTo(myTopicAddress);
            props.setMessageId("myMessageIDString");
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            Destination dest = receivedMessage.getJMSReplyTo();
            assertTrue(dest instanceof Topic);
            assertEquals(myTopicAddress, ((Topic)dest).getTopicName());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //==== TTL / Expiration Handling ====
    //===================================

    /**
     * Tests that lack of the absolute-expiry-time and ttl fields on a message results
     * in it returning 0 for for JMSExpiration
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithNoAbsoluteExpiryOrTtlReturnsJMSExpirationZero() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId("myMessageIDString");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertEquals(0L, receivedMessage.getJMSExpiration());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that setting a non-zero value in the absolute-expiry-time field on a
     * message results in it returning this value for JMSExpiration
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageFromQueueWithAbsoluteExpiryReturnsJMSExpirationNonZero() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Disable local expiration checking in consumer
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.localMessageExpiry=false");
            connection.start();

            testPeer.expectBegin();

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
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertEquals(timestamp, receivedMessage.getJMSExpiration());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //==== MessageID and CorrelationID Handling ====
    //==============================================

    @Test(timeout = 20000)
    public void testReceiveMessageWithoutMessageId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(2000);

            assertNull(receivedMessage.getJMSMessageID());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that receiving a message with a string typed message-id with "ID:" prefix results
     * in returning the expected value for JMSMessageId .
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithStringMessageIdReturnsExpectedJMSMessageID() throws Exception {
        String messageId = "ID:myTestMessageIdString";
        receivedMessageWithMessageIdTestImpl(messageId, messageId);
    }

    /**
     * Tests that receiving a message with a string typed message-id with no "ID:" prefix results
     * in returning the expected value for JMSMessageId where the JMS "ID:" prefix has been added.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithStringMessageIdNoPrefixReturnsExpectedJMSMessageID() throws Exception {
        String messageIdNoPrefix = "myTestMessageIdString";
        String expected = "ID:AMQP_NO_PREFIX:" + messageIdNoPrefix;
        receivedMessageWithMessageIdTestImpl(messageIdNoPrefix, expected);
    }

    /**
     * Tests that receiving a message with a UUID typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added to the UUID.tostring()
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithUUIDMessageIdReturnsExpectedJMSMessageID() throws Exception {
        UUID uuid = UUID.randomUUID();
        String expected = "ID:AMQP_UUID:" + uuid.toString();
        receivedMessageWithMessageIdTestImpl(uuid, expected);
    }

    /**
     * Tests that receiving a message with a ulong typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added to the UnsignedLong.tostring()
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithUnsignedLongMessageIdReturnsExpectedJMSMessageID() throws Exception {
        UnsignedLong ulong = UnsignedLong.valueOf(123456789L);
        String expected = "ID:AMQP_ULONG:123456789";
        receivedMessageWithMessageIdTestImpl(ulong, expected);
    }

    /**
     * Tests that receiving a message with a binary typed message-id results in returning the
     * expected value for JMSMessageId where the JMS "ID:" prefix has been added to the hex representation of the binary.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithBinaryMessageIdReturnsExpectedJMSMessageID() throws Exception {
        Binary binary = new Binary(new byte[]{(byte)0x02, (byte)0x20, (byte) 0xAE, (byte) 0x00});
        String expected = "ID:AMQP_BINARY:0220AE00";
        receivedMessageWithMessageIdTestImpl(binary, expected);
    }

    private void receivedMessageWithMessageIdTestImpl(Object underlyingAmqpMessageId, String expected) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId(underlyingAmqpMessageId);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            assertEquals(expected, receivedMessage.getJMSMessageID());
            assertTrue(receivedMessage.getJMSMessageID().startsWith("ID:"));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that receiving a message with a string typed correlation-id which is indicated
     * to be a JMSMessageID by presence of "ID:" prefix, results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix is retained.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithStringCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        String underlyingCorrelationId = "ID:myTestCorrelationIdString";
        String expected = underlyingCorrelationId;
        receivedMessageWithCorrelationIdTestImpl(underlyingCorrelationId, expected);
    }

    /**
     * Tests that receiving a message with a string typed correlation-id, which is indicated to be an
     * application-specific value, by lacking the "ID:" prefix, results in returning the expected value
     * for JMSCorrelationID where the JMS "ID:" prefix has NOT been added.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithAppSpecificStringCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        String underlyingCorrelationId = "myTestCorrelationIdString";
        String expected = underlyingCorrelationId;
        receivedMessageWithCorrelationIdTestImpl(underlyingCorrelationId, expected);
    }

    /**
     * Tests that receiving a message with a UUID typed correlation-id results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix has been added to the UUID.tostring()
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithUUIDCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        UUID underlyingCorrelationId = UUID.randomUUID();
        String expected = "ID:AMQP_UUID:" + underlyingCorrelationId.toString();
        receivedMessageWithCorrelationIdTestImpl(underlyingCorrelationId, expected);
    }

    /**
     * Tests that receiving a message with a UUID typed correlation-id results in returning the
     * expected value for JMSCorrelationID where the JMS "ID:" prefix has been added to the UUID.tostring()
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithLongCorrelationIdReturnsExpectedJMSCorrelationID() throws Exception {
        UnsignedLong underlyingCorrelationId = UnsignedLong.valueOf(123456789L);
        String expected = "ID:AMQP_ULONG:" + underlyingCorrelationId.toString();
        receivedMessageWithCorrelationIdTestImpl(underlyingCorrelationId, expected);
    }

    private void receivedMessageWithCorrelationIdTestImpl(Object underlyingCorrelationId, String expected) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            props.setMessageId("myMessageIdString");
            props.setCorrelationId(underlyingCorrelationId);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            assertEquals(expected, receivedMessage.getJMSCorrelationID());
        }
    }

    /**
     * Tests that sending a message with a uuid typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is uuid, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present, and there is
     * no presence of the message annotation to indicate an app-specific correlation-id.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithUUIDCorrelationId() throws Exception {
        UUID uuid = UUID.randomUUID();
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX +  uuid.toString();
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, uuid);
    }

    /**
     * Tests that sending a message with a binary typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is binary, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithBinaryCorrelationId() throws Exception
    {
        Binary bin = new Binary(new byte[]{(byte)0x01, (byte)0x23, (byte) 0xAF, (byte) 0x00});
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +  "0123af00";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, bin);
    }

    /**
     * Tests that sending a message with a ulong typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the type is ulong, the "ID:" prefix of the JMSCorrelationID value is (obviously) not present.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithUlongCorrelationId() throws Exception {
        UnsignedLong ulong = UnsignedLong.valueOf(Long.MAX_VALUE);
        String stringCorrelationId = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX +  ulong.toString();
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, ulong);
    }

    /**
     * Tests that sending a message with a string typed correlation-id value which is a
     * message-id results in an AMQP message with the expected encoding of the correlation-id,
     * where the "ID:" prefix of the JMSCorrelationID value is still present
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithStringCorrelationId() throws Exception {
        String stringCorrelationId = "ID:myTestMessageIdString";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, stringCorrelationId);
    }

    /**
     * Tests that sending a message with a string typed correlation-id value which is came
     * from a received message-id that lacked the "ID:" prefix and had it added, results in
     * an AMQP message with the expected encoding of the correlation-id, where the
     * the "ID:" prefix of the JMSCorrelationID value has been removed present
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithNoPrefixEncodedStringCorrelationId() throws Exception {
        String idSuffix = "myNoIdPrefixString";
        String stringCorrelationId = "ID:" + AmqpMessageIdHelper.AMQP_NO_PREFIX + idSuffix;
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, idSuffix);
    }

    /**
     * Tests that sending a message with a string typed correlation-id value which is a
     * app-specific results in an AMQP message with the expected encoding of the correlation-id.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSentMessageWithAppSpecificStringCorrelationId() throws Exception {
        String stringCorrelationId = "myTestAppSpecificString";
        sentMessageWithCorrelationIdTestImpl(stringCorrelationId, stringCorrelationId);
    }

    private void sentMessageWithCorrelationIdTestImpl(String stringCorrelationId, Object correlationIdForAmqpMessageClass) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);

            //Set matcher to validate the correlation-id
            propsMatcher.withCorrelationId(equalTo(correlationIdForAmqpMessageClass));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setJMSCorrelationID(stringCorrelationId);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    /**
     * Tests that receiving a message with a string typed message-id, that has the "ID:" prefix, and then sending
     * a message which uses the result of calling getJMSMessageID as the value for setJMSCorrelationId
     * results in transmission of the expected AMQP message content.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithStringMessageIdAndSendValueAsCorrelationId() throws Exception {
        String string = "ID:myStringMessageId";
        recieveMessageIdSendCorrelationIdTestImpl(string, string);
    }

    /**
     * Tests that receiving a message with a string typed message-id, that has no "ID:" prefix, and then sending
     * a message which uses the result of calling getJMSMessageID as the value for setJMSCorrelationId
     * results in transmission of the expected AMQP message content.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithStringNoPrefixMessageIdAndSendValueAsCorrelationId() throws Exception {
        String stringNoPrefix = "myStringMessageId";
        String expected = "ID:AMQP_NO_PREFIX:" + stringNoPrefix;
        recieveMessageIdSendCorrelationIdTestImpl(stringNoPrefix, expected);
    }

    /**
     * Tests that receiving a message with a UUID typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithUUIDMessageIdAndSendValueAsCorrelationId() throws Exception {
        UUID uuid = UUID.randomUUID();
        String expected = "ID:AMQP_UUID:" +  uuid.toString();
        recieveMessageIdSendCorrelationIdTestImpl(uuid, expected);
    }

    /**
     * Tests that receiving a message with a ulong typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithUlongMessageIdAndSendValueAsCorrelationId() throws Exception {
        UnsignedLong ulong = UnsignedLong.valueOf(123456789L);
        String expected = "ID:AMQP_ULONG:123456789";
        recieveMessageIdSendCorrelationIdTestImpl(ulong, expected);
    }

    /**
     * Tests that receiving a message with a binary typed message-id, and then sending a message which
     * uses the result of calling getJMSMessageID as the value for setJMSCorrelationId results in
     * transmission of the expected AMQP message content.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithBinaryMessageIdAndSendValueAsCorrelationId() throws Exception {
        Binary binary = new Binary(new byte[]{(byte)0x00, (byte)0xCD, (byte) 0xEF, (byte) 0x01});
        String expected = "ID:AMQP_BINARY:00CDEF01";
        recieveMessageIdSendCorrelationIdTestImpl(binary, expected);
    }

    private void recieveMessageIdSendCorrelationIdTestImpl(Object amqpIdObject, String expectedMessageId) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setMessageId(amqpIdObject);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);

            String jmsMessageID = receivedMessage.getJMSMessageID();
            assertEquals("Unexpected value for JMSMessageID", expectedMessageId, jmsMessageID);

            //Now take the received JMSMessageID, and send a message with it set
            //as the JMSCorrelationID and verify we send the same AMQP id as we started with.

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);

            //Set matcher to validate the correlation-id on the wire matches the previous message-id
            propsMatcher.withCorrelationId(equalTo(amqpIdObject));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setJMSCorrelationID(jmsMessageID);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    //==== Group Property Handling ====
    //=================================

    /**
     * Tests that when receiving a message with the group-id, reply-to-group-id, and group-sequence
     * fields of the AMQP properties section set, that the expected JMSX or JMS_AMQP properties
     * are present, and the expected values are returned when retrieved from the JMS message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceivedMessageWithGroupRelatedPropertiesSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

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
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull("did not receive the message", receivedMessage);

            boolean foundGroupId = false;
            boolean foundGroupSeq = false;
            boolean foundReplyToGroupId = false;

            Enumeration<?> names = receivedMessage.getPropertyNames();
            assertTrue("Message had no property names", names.hasMoreElements());
            while (names.hasMoreElements()) {
                Object element = names.nextElement();

                if (JmsClientProperties.JMSXGROUPID.equals(element)) {
                    foundGroupId = true;
                }

                if (JmsClientProperties.JMSXGROUPSEQ.equals(element)) {
                    foundGroupSeq = true;
                }

                if (AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID.equals(element)) {
                    foundReplyToGroupId = true;
                }
            }

            assertTrue("JMSXGroupID not in property names", foundGroupId);
            assertTrue("JMSXGroupSeq  not in property names", foundGroupSeq);
            assertTrue("JMS_AMQP_REPLY_TO_GROUP_ID not in property names", foundReplyToGroupId);

            assertTrue("JMSXGroupID does not exist", receivedMessage.propertyExists(JmsClientProperties.JMSXGROUPID));
            assertTrue("JMSXGroupSeq does not exist", receivedMessage.propertyExists(JmsClientProperties.JMSXGROUPSEQ));
            assertTrue("JMS_AMQP_REPLY_TO_GROUP_ID does not exist", receivedMessage.propertyExists(AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID));

            assertEquals("did not get the expected JMSXGroupID", expectedGroupId, receivedMessage.getStringProperty(JmsClientProperties.JMSXGROUPID));
            assertEquals("did not get the expected JMSXGroupSeq", expectedGroupSeq, receivedMessage.getIntProperty(JmsClientProperties.JMSXGROUPSEQ));
            assertEquals("did not get the expected JMS_AMQP_REPLY_TO_GROUP_ID", expectedReplyToGroupId, receivedMessage.getStringProperty(AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Tests that when sending a message with the JMSXGroupID, JMSXGroupSeq, and JMS_AMQP_REPLY_TO_GROUP_ID
     * properties of the JMS message set, that the expected values are included in the fields of
     * the AMQP message emitted.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendMessageWithGroupRelatedPropertiesSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
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

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendDoesNotMarkMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createMessage();
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response so that we can check that
            // the inflight message is read-only
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            try {
                producer.send(message);
            } catch (Throwable error) {
                fail("Send should not fail for async.");
            }

            try {
                message.setJMSCorrelationID("test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSCorrelationIDAsBytes(new byte[]{});
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSDestination(queue);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSExpiration(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSMessageID(queueName);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSPriority(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSRedelivered(false);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSReplyTo(queue);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSTimestamp(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSType(queueName);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setStringProperty("test", "test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMarksMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createMessage();
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response so that we can check that
            // the inflight message is read-only
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            try {
                producer.send(message, listener);
            } catch (Throwable error) {
                fail("Send should not fail for async.");
            }

            try {
                message.setJMSCorrelationID("test");
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSCorrelationIDAsBytes(new byte[]{});
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSDestination(queue);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSExpiration(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSMessageID(queueName);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSPriority(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSRedelivered(false);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSReplyTo(queue);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSTimestamp(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSType(queueName);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setStringProperty("test", "test");
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }

    //==== DeliveryTime Handling ====
    //===============================

    @Test(timeout = 20000)
    public void testReceivedMessageWithDeliveryTimeAnnotation() throws Exception {
        long deliveryTime = System.currentTimeMillis() + 13526;
        doReceivedMessageDeliveryTimeTestImpl(true, deliveryTime);
    }

    @Test(timeout = 20000)
    public void testReceivedMessageWithDeliveryTimeAnnotationTimestampValue() throws Exception {
        Date deliveryTime = new Date(System.currentTimeMillis() + 13526);
        doReceivedMessageDeliveryTimeTestImpl(true, deliveryTime);
    }

    @Test(timeout = 20000)
    public void testReceivedMessageWithDeliveryTimeAnnotationUnsignedLongValue() throws Exception {
        UnsignedLong deliveryTime = new UnsignedLong(System.currentTimeMillis() + 13526);
        doReceivedMessageDeliveryTimeTestImpl(true, deliveryTime);
    }

    @Test(timeout = 20000)
    public void testReceivedMessageWithoutDeliveryTimeAnnotation() throws Exception {
        doReceivedMessageDeliveryTimeTestImpl(false, null);
    }

    private void doReceivedMessageDeliveryTimeTestImpl(boolean setDeliveryTimeAnnotation, Object annotationValue) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            final long creationTime = System.currentTimeMillis();
            final long expectedDeliveryTime;
            if (setDeliveryTimeAnnotation) {
                if (annotationValue instanceof Long) {
                    expectedDeliveryTime = (Long) annotationValue;
                } else if (annotationValue instanceof Date) {
                    expectedDeliveryTime = ((Date) annotationValue).getTime();
                } else if (annotationValue instanceof UnsignedLong) {
                    expectedDeliveryTime = ((UnsignedLong) annotationValue).longValue();
                } else {
                    throw new IllegalArgumentException("Unexpected annotation value");
                }
            } else {
                expectedDeliveryTime = creationTime;
            }

            MessageAnnotationsDescribedType msgAnnotations = null;
            if (setDeliveryTimeAnnotation) {
                msgAnnotations = new MessageAnnotationsDescribedType();
                msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_DELIVERY_TIME.toString(), annotationValue);
            }

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setTo("myAddress");
            props.setMessageId("ID:myMessageIDString");
            props.setCreationTime(new Date(creationTime));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, props, null, new AmqpValueDescribedType(null));
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull("should have recieved a message", receivedMessage);

            assertEquals("Unexpected delivery time", expectedDeliveryTime, receivedMessage.getJMSDeliveryTime());
        }
    }
}
