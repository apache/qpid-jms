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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

public class MapMessageIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    /**
     * Test that a message received from the test peer with an AmqpValue section containing
     * a map which holds entries of the various supported entry types is returned as a
     * {@link MapMessage}, and verify the values can all be retrieved as expected.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBasicMapMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing an
            // AmqpValue section holding a map with entries for each supported type,
            // and annotated as a JMS map message.
            String myBoolKey = "myBool";
            boolean myBool = true;
            String myByteKey = "myByte";
            byte myByte = 4;
            String myBytesKey = "myBytes";
            byte[] myBytes = myBytesKey.getBytes();
            String myCharKey = "myChar";
            char myChar = 'd';
            String myDoubleKey = "myDouble";
            double myDouble = 1234567890123456789.1234;
            String myFloatKey = "myFloat";
            float myFloat = 1.1F;
            String myIntKey = "myInt";
            int myInt = Integer.MAX_VALUE;
            String myLongKey = "myLong";
            long myLong = Long.MAX_VALUE;
            String myShortKey = "myShort";
            short myShort = 25;
            String myStringKey = "myString";
            String myString = myStringKey;
            String myEmptyStringKey = "myEmtpyString";
            String myNullStringKey = "myNullString";

            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put(myBoolKey, myBool);
            map.put(myByteKey, myByte);
            map.put(myBytesKey, new Binary(myBytes));// the underlying AMQP message uses Binary rather than byte[] directly.
            map.put(myCharKey, myChar);
            map.put(myDoubleKey, myDouble);
            map.put(myFloatKey, myFloat);
            map.put(myIntKey, myInt);
            map.put(myLongKey, myLong);
            map.put(myShortKey, myShort);
            map.put(myStringKey, myString);
            map.put(myEmptyStringKey, "");
            map.put(myNullStringKey, null);

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_MAP_MESSAGE);

            DescribedType amqpValueSectionContent = new AmqpValueDescribedType(map);

            // receive the message from the test peer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueSectionContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            // verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a MapMessage", receivedMessage instanceof MapMessage);
            MapMessage receivedMapMessage = (MapMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedMapMessage.getBoolean(myBoolKey));
            assertEquals("Unexpected byte value", myByte, receivedMapMessage.getByte(myByteKey));
            byte[] readBytes = receivedMapMessage.getBytes(myBytesKey);
            assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
            assertEquals("Unexpected char value", myChar, receivedMapMessage.getChar(myCharKey));
            assertEquals("Unexpected double value", myDouble, receivedMapMessage.getDouble(myDoubleKey), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedMapMessage.getFloat(myFloatKey), 0.0);
            assertEquals("Unexpected int value", myInt, receivedMapMessage.getInt(myIntKey));
            assertEquals("Unexpected long value", myLong, receivedMapMessage.getLong(myLongKey));
            assertEquals("Unexpected short value", myShort, receivedMapMessage.getShort(myShortKey));
            assertEquals("Unexpected UTF value", myString, receivedMapMessage.getString(myStringKey));
            assertEquals("Unexpected value", "", receivedMapMessage.getString(myEmptyStringKey));
            assertNull("Unexpected value", receivedMapMessage.getString(myNullStringKey));

            assertTrue(receivedMapMessage.isBodyAssignableTo(Map.class));
            assertTrue(receivedMapMessage.isBodyAssignableTo(Object.class));
            assertFalse(receivedMapMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(receivedMapMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(receivedMapMessage.getBody(Object.class));
            assertNotNull(receivedMapMessage.getBody(Map.class));
            try {
                receivedMapMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    /*
     * TODO: decide what to do about this
     *
     * The test below fails if a char is added and matched, unless we cast the matcher to expect an int.
     * This is because the DataImpl-based decoder used by the test peer decodes the char to an Integer object
     * and thus the EncodedAmqpValueMatcher would fail the comparison of its contained map due to the differing types.
     * This doesn't happen in the above test as the reversed roles mean it is protons DecoderImpl doing the decoding
     * and it does a similarly ugly cast on the integer value to char before output.
     */
    @Test(timeout = 20000)
    public void testSendBasicMapMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            String myBoolKey = "myBool";
            boolean myBool = true;
            String myByteKey = "myByte";
            byte myByte = 4;
            String myBytesKey = "myBytes";
            byte[] myBytes = myBytesKey.getBytes();
            String myCharKey = "myChar";
            char myChar = 'd';
            String myDoubleKey = "myDouble";
            double myDouble = 1234567890123456789.1234;
            String myFloatKey = "myFloat";
            float myFloat = 1.1F;
            String myIntKey = "myInt";
            int myInt = Integer.MAX_VALUE;
            String myLongKey = "myLong";
            long myLong = Long.MAX_VALUE;
            String myShortKey = "myShort";
            short myShort = 25;
            String myStringKey = "myString";
            String myString = myStringKey;

            // Prepare a MapMessage to send to the test peer to send
            MapMessage mapMessage = session.createMapMessage();

            mapMessage.setBoolean(myBoolKey, myBool);
            mapMessage.setByte(myByteKey, myByte);
            mapMessage.setBytes(myBytesKey, myBytes);
            mapMessage.setChar(myCharKey, myChar);
            mapMessage.setDouble(myDoubleKey, myDouble);
            mapMessage.setFloat(myFloatKey, myFloat);
            mapMessage.setInt(myIntKey, myInt);
            mapMessage.setLong(myLongKey, myLong);
            mapMessage.setShort(myShortKey, myShort);
            mapMessage.setString(myStringKey, myString);

            // prepare a matcher for the test peer to use to receive and verify the message
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put(myBoolKey, myBool);
            map.put(myByteKey, myByte);
            map.put(myBytesKey, new Binary(myBytes));// the underlying AMQP message uses Binary rather than byte[] directly.
            // TODO: see note above to explain the ugly cast
            map.put(myCharKey, (int) myChar);
            map.put(myDoubleKey, myDouble);
            map.put(myFloatKey, myFloat);
            map.put(myIntKey, myInt);
            map.put(myLongKey, myLong);
            map.put(myShortKey, myShort);
            map.put(myStringKey, myString);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_MAP_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(map));

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            // send the message
            producer.send(mapMessage);

            assertTrue(mapMessage.isBodyAssignableTo(Map.class));
            assertTrue(mapMessage.isBodyAssignableTo(Object.class));
            assertFalse(mapMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(mapMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(mapMessage.getBody(Object.class));
            assertNotNull(mapMessage.getBody(Map.class));
            try {
                mapMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testSendMapMessageIsWritable() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            String myIntKey = "myInt";
            int myInt = Integer.MAX_VALUE;
            String myStringKey = "myString";
            String myString = myStringKey;

            // Prepare a MapMessage to send to the test peer to send
            MapMessage mapMessage = session.createMapMessage();

            mapMessage.setString(myStringKey, myString);

            // prepare a matcher for the test peer to use to receive and verify the message
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put(myStringKey, myString);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_MAP_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(map));

            testPeer.expectTransfer(messageMatcher);

            // send the message
            producer.send(mapMessage);

            // Update the message and matcher and send again
            mapMessage.setInt(myIntKey, myInt);
            map.put(myIntKey, myInt);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            producer.send(mapMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendDoesNotMarkMapMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            MapMessage message = session.createMapMessage();
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
                message.setString("test", "test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMarksMapMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            MapMessage message = session.createMapMessage();
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
                message.setJMSDeliveryMode(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSDestination(queue);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSExpiration(DeliveryMode.PERSISTENT);
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
                message.setString("test", "test");
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
