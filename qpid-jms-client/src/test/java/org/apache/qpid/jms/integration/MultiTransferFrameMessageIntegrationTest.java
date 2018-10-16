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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matchers;
import org.junit.Test;

public class MultiTransferFrameMessageIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testReceiveMultiFrameBytesMessage() throws Exception {
        doReceiveMultiFrameBytesMessageTestImpl(false);
    }

    @Test(timeout = 20000)
    public void testReceiveMultiFrameBytesMessageWithEmptyFinalTransfer() throws Exception {
        doReceiveMultiFrameBytesMessageTestImpl(true);
    }

    private void doReceiveMultiFrameBytesMessageTestImpl(boolean sendFinalTransferFrameWithoutPayload) throws JMSException, InterruptedException, Exception, IOException {
        int payloadSizeInBytes = 20_123_321;
        int msgPayloadPerFrame = 101_234;

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE);

            Random rand = new Random(System.currentTimeMillis());
            int payloadStartPoint = rand.nextInt(6);

            final byte[] expectedContent = createMessageBodyContent(payloadSizeInBytes, payloadStartPoint);
            DescribedType dataContent = new DataDescribedType(new Binary(expectedContent));

            testPeer.expectReceiverAttach();

            testPeer.expectLinkFlowAndSendBackMessages(null, msgAnnotations, properties, null, dataContent, 1,
                                                      true, false, Matchers.equalTo(UnsignedInteger.valueOf(1)), 1,
                                                      false, false, msgPayloadPerFrame, sendFinalTransferFrameWithoutPayload);

            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receiveNoWait();
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals("Unexpected message body length", expectedContent.length, bytesMessage.getBodyLength());

            byte[] receivedContent = new byte[expectedContent.length];
            int readBytes = bytesMessage.readBytes(receivedContent);

            assertEquals("Unexpected content length read", receivedContent.length, readBytes);
            assertTrue("Unexpected content", Arrays.equals(expectedContent, receivedContent));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    private static byte[] createMessageBodyContent(int sizeInBytes, int startPoint) {
        byte[] payload = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            // An odd number of digit characters
            int offset = (startPoint + i) % 7;
            payload[i] = (byte) (48 + offset);
        }

        return payload;
    }
}
