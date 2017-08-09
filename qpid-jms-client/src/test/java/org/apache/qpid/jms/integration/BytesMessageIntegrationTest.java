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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
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
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedDataMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class BytesMessageIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testSendBasicBytesMessageWithContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            byte[] content = "myBytes".getBytes();

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(Symbol.valueOf(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE)));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(content)));

            testPeer.expectTransfer(messageMatcher);

            BytesMessage message = session.createBytesMessage();
            message.writeBytes(content);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStream() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE, true);
    }

    @Test(timeout = 20000)
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStreamNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE, false);
    }

    @Test(timeout = 20000)
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeEmptyNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl("", false);
    }

    @Test(timeout = 20000)
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeUnknownNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl("type/unknown", false);
    }

    private void doReceiveBasicBytesMessageUsingDataSectionTestImpl(String contentType, boolean typeAnnotation) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(Symbol.valueOf(contentType));

            MessageAnnotationsDescribedType msgAnnotations = null;
            if (typeAnnotation) {
                msgAnnotations = new MessageAnnotationsDescribedType();
                msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_BYTES_MESSAGE);
            }

            final byte[] expectedContent = "expectedContent".getBytes();
            DescribedType dataContent = new DataDescribedType(new Binary(expectedContent));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(expectedContent.length, bytesMessage.getBodyLength());
            byte[] recievedContent = new byte[expectedContent.length];
            int readBytes = bytesMessage.readBytes(recievedContent);
            assertEquals(recievedContent.length, readBytes);
            assertTrue(Arrays.equals(expectedContent, recievedContent));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    /**
     * Test that a message received from the test peer with a Data section and content type of
     * {@link AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE} is returned as a BytesMessage, verify it
     * gives the expected data values when read, and when reset and left mid-stream before being
     * resent that it results in the expected AMQP data body section and properties content type
     * being received by the test peer.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBytesMessageAndResendAfterResetAndPartialRead() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing the content type and
            // a data body section populated with expected bytes for use as a JMS BytesMessage
            PropertiesDescribedType properties = new PropertiesDescribedType();
            Symbol contentType = Symbol.valueOf(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);
            properties.setContentType(contentType);

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_BYTES_MESSAGE);

            boolean myBool = true;
            byte myByte = 4;
            byte[] myBytes = "myBytes".getBytes();
            char myChar = 'd';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = 25;
            String myUTF = "myString";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeBoolean(myBool);
            dos.writeByte(myByte);
            dos.write(myBytes);
            dos.writeChar(myChar);
            dos.writeDouble(myDouble);
            dos.writeFloat(myFloat);
            dos.writeInt(myInt);
            dos.writeLong(myLong);
            dos.writeShort(myShort);
            dos.writeUTF(myUTF);

            byte[] bytesPayload = baos.toByteArray();
            Binary binaryPayload = new Binary(bytesPayload);
            DescribedType dataSectionContent = new DataDescribedType(binaryPayload);

            // receive the message from the test peer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataSectionContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            // verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a BytesMessage", receivedMessage instanceof BytesMessage);
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedBytesMessage.readBoolean());
            assertEquals("Unexpected byte value", myByte, receivedBytesMessage.readByte());
            byte[] readBytes = new byte[myBytes.length];
            assertEquals("Did not read the expected number of bytes", myBytes.length, receivedBytesMessage.readBytes(readBytes));
            assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
            assertEquals("Unexpected char value", myChar, receivedBytesMessage.readChar());
            assertEquals("Unexpected double value", myDouble, receivedBytesMessage.readDouble(), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedBytesMessage.readFloat(), 0.0);
            assertEquals("Unexpected int value", myInt, receivedBytesMessage.readInt());
            assertEquals("Unexpected long value", myLong, receivedBytesMessage.readLong());
            assertEquals("Unexpected short value", myShort, receivedBytesMessage.readShort());
            assertEquals("Unexpected UTF value", myUTF, receivedBytesMessage.readUTF());

            // reset and read the first item, leaving message marker in the middle of its content
            receivedBytesMessage.reset();
            assertEquals("Unexpected boolean value after reset", myBool, receivedBytesMessage.readBoolean());

            // Send the received message back to the test peer and have it check the result is as expected
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withContentType(equalTo(contentType));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(binaryPayload));
            testPeer.expectTransfer(messageMatcher);

            producer.send(receivedBytesMessage);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testGetBodyBytesMessageFailsWhenWrongTypeRequested() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(Symbol.valueOf(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE));

            MessageAnnotationsDescribedType msgAnnotations = null;
            msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_BYTES_MESSAGE);

            final byte[] expectedContent = "expectedContent".getBytes();
            DescribedType dataContent = new DataDescribedType(new Binary(expectedContent));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent, 2);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message readMsg1 = messageConsumer.receive(1000);
            Message readMsg2 = messageConsumer.receive(1000);

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(readMsg1);
            assertNotNull(readMsg2);

            try {
                readMsg1.getBody(String.class);
                fail("Should have thrown MessageFormatException");
            } catch (MessageFormatException mfe) {
            }

            try {
                readMsg2.getBody(Map.class);
                fail("Should have thrown MessageFormatException");
            } catch (MessageFormatException mfe) {
            }

            byte[] received1 = readMsg1.getBody(byte[].class);
            byte[] received2 = (byte[]) readMsg2.getBody(Object.class);

            assertTrue(Arrays.equals(expectedContent, received1));
            assertTrue(Arrays.equals(expectedContent, received2));

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    /**
     * Test that a message received from the test peer with an AmqpValue section containing
     * Binary and no content type is returned as a BytesMessage, verify it gives the
     * expected data values when read, and when sent to the test peer it results in an
     * AMQP message containing a data body section and content type of
     * {@link AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE}
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBytesMessageWithAmqpValueAndResendResultsInData() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing an amqp-value
            // body section populated with expected bytes for use as a JMS BytesMessage,
            // and do not set content type, or the message type annotation

            boolean myBool = true;
            byte myByte = 4;
            byte[] myBytes = "myBytes".getBytes();
            char myChar = 'd';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = 25;
            String myUTF = "myString";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeBoolean(myBool);
            dos.writeByte(myByte);
            dos.write(myBytes);
            dos.writeChar(myChar);
            dos.writeDouble(myDouble);
            dos.writeFloat(myFloat);
            dos.writeInt(myInt);
            dos.writeLong(myLong);
            dos.writeShort(myShort);
            dos.writeUTF(myUTF);

            byte[] bytesPayload = baos.toByteArray();
            Binary binaryPayload = new Binary(bytesPayload);

            DescribedType amqpValueSectionContent = new AmqpValueDescribedType(binaryPayload);

            // receive the message from the test peer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueSectionContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            // verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a BytesMessage", receivedMessage instanceof BytesMessage);
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedBytesMessage.readBoolean());
            assertEquals("Unexpected byte value", myByte, receivedBytesMessage.readByte());
            byte[] readBytes = new byte[myBytes.length];
            assertEquals("Did not read the expected number of bytes", myBytes.length, receivedBytesMessage.readBytes(readBytes));
            assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
            assertEquals("Unexpected char value", myChar, receivedBytesMessage.readChar());
            assertEquals("Unexpected double value", myDouble, receivedBytesMessage.readDouble(), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedBytesMessage.readFloat(), 0.0);
            assertEquals("Unexpected int value", myInt, receivedBytesMessage.readInt());
            assertEquals("Unexpected long value", myLong, receivedBytesMessage.readLong());
            assertEquals("Unexpected short value", myShort, receivedBytesMessage.readShort());
            assertEquals("Unexpected UTF value", myUTF, receivedBytesMessage.readUTF());

            // reset and read the first item, leaving message marker in the middle of its content
            receivedBytesMessage.reset();
            assertEquals("Unexpected boolean value after reset", myBool, receivedBytesMessage.readBoolean());

            // Send the received message back to the test peer and have it check the result is as expected
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withContentType(equalTo(Symbol.valueOf(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE)));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(binaryPayload));
            testPeer.expectTransfer(messageMatcher);

            producer.send(receivedBytesMessage);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendDoesNotMarksBytesMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            BytesMessage message = session.createBytesMessage();
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
                fail("Send should not fail for async send.");
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
                message.clearBody();
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to clear on inflight message");
            }
            try {
                message.writeBoolean(true);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to write to on inflight message");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMarksBytesMessageReadOnly() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            BytesMessage message = session.createBytesMessage();
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
                message.writeBoolean(true);
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
