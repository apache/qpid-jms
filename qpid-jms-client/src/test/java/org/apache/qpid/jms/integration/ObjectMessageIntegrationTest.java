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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

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
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class ObjectMessageIntegrationTest extends QpidJmsTestCase
{
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
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(Symbol.valueOf(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE)));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(bytes)));

            testPeer.expectTransfer(messageMatcher);

            ObjectMessage message = session.createObjectMessage();
            if (content != null || setObjectIfNull) {
                message.setObject(content);
            }

            producer.send(message);

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
            properties.setContentType(Symbol.valueOf(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE));

            String expectedContent = "expectedContent";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(expectedContent);
            oos.flush();
            oos.close();
            byte[] bytes = baos.toByteArray();

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_OBJECT_MESSAGE);

            DescribedType dataContent = new DataDescribedType(new Binary(bytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof ObjectMessage);
            ObjectMessage objectMessage = (ObjectMessage)receivedMessage;

            Object object = objectMessage.getObject();
            assertNotNull("Expected object but got null", object);
            assertEquals("Message body object was not as expected", expectedContent, object);
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
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_OBJECT_MESSAGE);
            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(Symbol.valueOf(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE));

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
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(Symbol.valueOf(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE)));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(bytes)));

            testPeer.expectTransfer(messageMatcher);

            producer.send(receivedMessage);

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
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_OBJECT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(nullValue());//check there is no content-type
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(map));

            testPeer.expectTransfer(messageMatcher);

            ObjectMessage message = session.createObjectMessage();
            message.setBooleanProperty(AmqpMessageSupport.JMS_AMQP_TYPED_ENCODING, true);
            message.setObject(map);

            producer.send(message);

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
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_OBJECT_MESSAGE);

            HashMap<String,String> map = new HashMap<String,String>();
            map.put("key", "myObjectString");

            DescribedType amqpValueContent = new AmqpValueDescribedType(map);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue("Expected ObjectMessage instance, but got: " + receivedMessage.getClass().getName(), receivedMessage instanceof ObjectMessage);
            ObjectMessage objectMessage = (ObjectMessage)receivedMessage;

            Object object = objectMessage.getObject();
            assertNotNull("Expected object but got null", object);
            assertEquals("Message body object was not as expected", map, object);
        }
    }
}
