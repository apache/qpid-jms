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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedDataMatcher;
import org.apache.qpid.jms.util.SimplePojo;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectMessageIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectMessageIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    //==== Java serialization encoding ====

    @Test(timeout = 20000)
    public void testSendBasicObjectMessageWithSerializedContent() throws Exception {
        doSendBasicObjectMessageWithSerializedContentTestImpl("myObjectString", false);
    }

    @Test(timeout = 20000)
    public void testSendBasicObjectMessageWithSerializedContentExplicitNull() throws Exception {
        doSendBasicObjectMessageWithSerializedContentTestImpl(null, true);
    }

    @Test(timeout = 20000)
    public void testSendBasicObjectMessageWithSerializedContentImplicitNull() throws Exception {
        doSendBasicObjectMessageWithSerializedContentTestImpl(null, false);
    }

    private void doSendBasicObjectMessageWithSerializedContentTestImpl(String content, boolean setObjectIfNull) throws JMSException, IOException, InterruptedException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(content);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(bytes)));

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            ObjectMessage message = session.createObjectMessage();
            if (content != null || setObjectIfNull) {
                message.setObject(content);
            }

            producer.send(message);

            if (content == null) {
                assertTrue(message.isBodyAssignableTo(String.class));
                assertTrue(message.isBodyAssignableTo(Serializable.class));
                assertTrue(message.isBodyAssignableTo(Object.class));
                assertTrue(message.isBodyAssignableTo(Boolean.class));
                assertTrue(message.isBodyAssignableTo(byte[].class));
            } else {
                assertTrue(message.isBodyAssignableTo(String.class));
                assertTrue(message.isBodyAssignableTo(Serializable.class));
                assertTrue(message.isBodyAssignableTo(Object.class));
                assertFalse(message.isBodyAssignableTo(Boolean.class));
                assertFalse(message.isBodyAssignableTo(byte[].class));
            }

            if (content == null) {
                assertNull(message.getBody(Object.class));
                assertNull(message.getBody(Serializable.class));
                assertNull(message.getBody(String.class));
                assertNull(message.getBody(byte[].class));
            } else {
                assertNotNull(message.getBody(Object.class));
                assertNotNull(message.getBody(Serializable.class));
                assertNotNull(message.getBody(String.class));
                try {
                    message.getBody(byte[].class);
                    fail("Cannot read TextMessage with this type.");
                } catch (MessageFormatException mfe) {
                }
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBasicObjectMessageWithSerializedContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

            String expectedContent = "expectedContent";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(expectedContent);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_OBJECT_MESSAGE);

            DescribedType dataContent = new DataDescribedType(new Binary(bytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectClose();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof ObjectMessage);
            ObjectMessage objectMessage = (ObjectMessage)receivedMessage;

            Object object = objectMessage.getObject();
            assertNotNull("Expected object but got null", object);
            assertEquals("Message body object was not as expected", expectedContent, object);

            assertTrue(receivedMessage.isBodyAssignableTo(String.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Serializable.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Object.class));
            assertFalse(receivedMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(receivedMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(receivedMessage.getBody(Object.class));
            assertNotNull(receivedMessage.getBody(Serializable.class));
            assertNotNull(receivedMessage.getBody(String.class));
            try {
                receivedMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveAndThenResendBasicObjectMessageWithSerializedContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_OBJECT_MESSAGE);
            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

            String expectedContent = "expectedContent";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(expectedContent);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            DescribedType dataContent = new DataDescribedType(new Binary(bytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof ObjectMessage);

            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(bytes)));

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            producer.send(receivedMessage);

            assertTrue(receivedMessage.isBodyAssignableTo(String.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Serializable.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Object.class));
            assertFalse(receivedMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(receivedMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(receivedMessage.getBody(Object.class));
            assertNotNull(receivedMessage.getBody(Serializable.class));
            assertNotNull(receivedMessage.getBody(String.class));
            try {
                receivedMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBlockedSerializedContentFailsOnGetObject() throws Exception {
        // We arent allowing the test class
        doTestReceiveSerializedContentPolicyTest("java.lang,java.util", null, false);
    }

    @Test(timeout = 20000)
    public void testReceiveBlockAllSerializedContentFailsOnGetObject() throws Exception {
        // We are blocking everything
        doTestReceiveSerializedContentPolicyTest(null, "*", false);
    }

    @Test(timeout = 20000)
    public void testReceiveBlockSomeSerializedContentFailsOnGetObject() throws Exception {
        // We arent allowing the UUID
        doTestReceiveSerializedContentPolicyTest("org.apache.qpid.jms", null, false);
    }

    @Test(timeout = 20000)
    public void testReceiveWithWrongUnblockedSerializedContentFailsOnGetObject() throws Exception {
        // We arent allowing the UUID a different way
        doTestReceiveSerializedContentPolicyTest("java.lang,org.apache.qpid.jms", null, false);
    }

    @Test(timeout = 20000)
    public void testReceiveWithFullyWhitelistedSerializedContentSucceeds() throws Exception {
        // We are allowing everything needed
        doTestReceiveSerializedContentPolicyTest("java.lang,java.util,org.apache.qpid.jms", null, true);
    }

    @Test(timeout = 20000)
    public void testReceiveWithFullyWhitelistedSerializedContentFailsDueToBlackList() throws Exception {
        // We are whitelisting everything needed, but then the blacklist is overriding to block some
        doTestReceiveSerializedContentPolicyTest("java.lang,java.util,org.apache.qpid.jms", "java.util", false);
    }

    private void doTestReceiveSerializedContentPolicyTest(String whiteList, String blackList, boolean succeed) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            String options = null;
            if(whiteList != null) {
                options = "?jms.deserializationPolicy.whiteList=" + whiteList;
            }

            if(blackList != null) {
                if(options == null) {
                    options = "?";
                } else {
                    options += "&";
                }

                options +="jms.deserializationPolicy.blackList=" + blackList;
            }

            Connection connection = testFixture.establishConnecton(testPeer, options);

            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_OBJECT_MESSAGE);
            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

            SimplePojo expectedContent = new SimplePojo(UUID.randomUUID());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(expectedContent);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            DescribedType dataContent = new DataDescribedType(new Binary(bytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof ObjectMessage);

            ObjectMessage objectMessage = (ObjectMessage) receivedMessage;
            Object received = null;
            try {
                received = objectMessage.getObject();
                if(!succeed) {
                    fail("Should not be able to read blocked content");
                }
            } catch (JMSException jmsEx) {
                LOG.debug("Caught: ", jmsEx);
                if(succeed) {
                    fail("Should have been able to read blocked content");
                }
            }

            if(succeed) {
                assertEquals("Content not as expected", expectedContent, received);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    //==== AMQP type system encoding ====

    @Test(timeout = 20000)
    public void testSendBasicObjectMessageWithAmqpTypedContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            HashMap<String,String> map = new HashMap<String,String>();
            map.put("key", "myObjectString");

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(nullValue());//check there is no content-type
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(map));

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            ObjectMessage message = session.createObjectMessage();
            message.setBooleanProperty(AmqpMessageSupport.JMS_AMQP_TYPED_ENCODING, true);
            message.setObject(map);

            producer.send(message);

            assertTrue(message.isBodyAssignableTo(Map.class));
            assertTrue(message.isBodyAssignableTo(Serializable.class));
            assertTrue(message.isBodyAssignableTo(Object.class));
            assertFalse(message.isBodyAssignableTo(Boolean.class));
            assertFalse(message.isBodyAssignableTo(byte[].class));

            assertNotNull(message.getBody(Object.class));
            assertNotNull(message.getBody(Serializable.class));
            assertNotNull(message.getBody(Map.class));
            try {
                message.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testRecieveBasicObjectMessageWithAmqpTypedContentAndJMSMessageTypeAnnotation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_OBJECT_MESSAGE);

            HashMap<String,String> map = new HashMap<String,String>();
            map.put("key", "myObjectString");

            DescribedType amqpValueContent = new AmqpValueDescribedType(map);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectClose();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue("Expected ObjectMessage instance, but got: " + receivedMessage.getClass().getName(), receivedMessage instanceof ObjectMessage);
            ObjectMessage objectMessage = (ObjectMessage)receivedMessage;

            Object object = objectMessage.getObject();
            assertNotNull("Expected object but got null", object);
            assertEquals("Message body object was not as expected", map, object);

            assertTrue(receivedMessage.isBodyAssignableTo(Map.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Serializable.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Object.class));
            assertFalse(receivedMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(receivedMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(receivedMessage.getBody(Object.class));
            assertNotNull(receivedMessage.getBody(Serializable.class));
            assertNotNull(receivedMessage.getBody(Map.class));
            try {
                receivedMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendDoesNotMarkObjectMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            ObjectMessage message = session.createObjectMessage();
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
            try {
                message.setObject("test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMarksObjectMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            ObjectMessage message = session.createObjectMessage();
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
            try {
                message.setObject("test");
                fail("Message should not be writable after a send.");
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
}
