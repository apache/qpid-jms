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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpSequenceMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class StreamMessageIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    /**
     * Test that a message received from the test peer with an AmqpValue section containing
     * a list which holds entries of the various supported entry types is returned as a
     * {@link StreamMessage}, and verify the values can all be retrieved as expected.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBasicStreamMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            //Prepare an AMQP message for the test peer to send, containing an
            //AmqpValue section holding a list with entries for each supported type,
            //and annotated as a JMS stream message.
            boolean myBool = true;
            byte myByte = Byte.MAX_VALUE;
            char myChar = 'c';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = Short.MAX_VALUE;
            String myString = "myString";
            byte[] myBytes = "myBytes".getBytes();

            List<Object> list = new ArrayList<Object>();
            list.add(myBool);
            list.add(myByte);
            list.add(new Binary(myBytes));//the underlying AMQP message uses Binary rather than byte[] directly.
            list.add(myChar);
            list.add(myDouble);
            list.add(myFloat);
            list.add(myInt);
            list.add(myLong);
            list.add(myShort);
            list.add(myString);

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE, AmqpMessageSupport.JMS_STREAM_MESSAGE);

            DescribedType amqpValueSectionContent = new AmqpValueDescribedType(list);

            //receive the message from the test peer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueSectionContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            //verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a MapMessage", receivedMessage instanceof StreamMessage);
            StreamMessage receivedStreamMessage  = (StreamMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedStreamMessage.readBoolean());
            assertEquals("Unexpected byte value", myByte, receivedStreamMessage.readByte());
            byte[] readBytes = (byte[]) receivedStreamMessage.readObject();//using readObject to get a new byte[]
            assertArrayEquals("Read bytes were not as expected", myBytes, readBytes);
            assertEquals("Unexpected char value", myChar, receivedStreamMessage.readChar());
            assertEquals("Unexpected double value", myDouble, receivedStreamMessage.readDouble(), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedStreamMessage.readFloat(), 0.0);
            assertEquals("Unexpected int value", myInt, receivedStreamMessage.readInt());
            assertEquals("Unexpected long value", myLong, receivedStreamMessage.readLong());
            assertEquals("Unexpected short value", myShort, receivedStreamMessage.readShort());
            assertEquals("Unexpected UTF value", myString, receivedStreamMessage.readString());
        }
    }

/*
 * TODO: decide what to do about this
 *
 * The test below fails if a char is added, because the DataImpl-based decoder used by the test peer
 * decodes the char to an Integer object and thus the EncodedAmqpValueMatcher fails the comparison
 * of its contained map due to the differing types. This doesn't happen in the above test as the
 * reversed roles mean it is DecoderImpl doing the decoding and it casts the output to a char.
 *
 * The below test has a hack to 'expect an int' to work round this currently.

    /**
     * Test that sending a stream message to the test peer results in receipt of a message with
     * an AmqpValue section containing a list which holds entries of the various supported entry
     * types with the expected values.
     */
    @Test(timeout = 20000)
    public void testSendBasicStreamMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            boolean myBool = true;
            byte myByte = Byte.MAX_VALUE;
            char myChar = 'c';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = Short.MAX_VALUE;
            String myString = "myString";
            byte[] myBytes = "myBytes".getBytes();

            //Prepare a MapMessage to send to the test peer to send
            StreamMessage streamMessage = session.createStreamMessage();

            streamMessage.writeBoolean(myBool);
            streamMessage.writeByte(myByte);
            streamMessage.writeBytes(myBytes);
            streamMessage.writeChar(myChar);
            streamMessage.writeDouble(myDouble);
            streamMessage.writeFloat(myFloat);
            streamMessage.writeInt(myInt);
            streamMessage.writeLong(myLong);
            streamMessage.writeShort(myShort);
            streamMessage.writeString(myString);

            //prepare a matcher for the test peer to use to receive and verify the message
            List<Object> list = new ArrayList<Object>();
            list.add(myBool);
            list.add(myByte);
            list.add(new Binary(myBytes));//the underlying AMQP message uses Binary rather than byte[] directly.
            list.add((int) myChar);//TODO: see note above about chars
            list.add(myDouble);
            list.add(myFloat);
            list.add(myInt);
            list.add(myLong);
            list.add(myShort);
            list.add(myString);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), equalTo(AmqpMessageSupport.JMS_STREAM_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpSequenceMatcher(list));

            //send the message
            testPeer.expectTransfer(messageMatcher);
            producer.send(streamMessage);
        }
    }
}
