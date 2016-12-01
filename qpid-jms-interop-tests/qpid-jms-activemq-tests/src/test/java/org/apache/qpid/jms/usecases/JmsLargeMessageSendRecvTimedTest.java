/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.usecases;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsLargeMessageSendRecvTimedTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsLargeMessageSendRecvTimedTest.class);

    private final Random rand = new Random(System.currentTimeMillis());

    private byte[] createLargePayload(int sizeInBytes) {
        byte[] payload = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            payload[i] = (byte) rand.nextInt(256);
        }

        LOG.debug("Created buffer with size : " + sizeInBytes + " bytes");
        return payload;
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testSendSmallerMessages() throws Exception {
        for (int i = 512; i <= (16 * 1024); i += 512) {
            doTestSendLargeMessage(i);
        }
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testSendFixedSizedMessages() throws Exception {
        doTestSendLargeMessage(65536);
        doTestSendLargeMessage(65536 * 2);
        doTestSendLargeMessage(65536 * 4);
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testSend10MBMessage() throws Exception {
        doTestSendLargeMessage(1024 * 1024 * 10);
    }

    @Ignore
    @Test(timeout = 5 * 60 * 1000)
    public void testSend100MBMessage() throws Exception {
        doTestSendLargeMessage(1024 * 1024 * 100);
    }

    public void doTestSendLargeMessage(int expectedSize) throws Exception{
        LOG.info("doTestSendLargeMessage called with expectedSize " + expectedSize);
        byte[] payload = createLargePayload(expectedSize);
        assertEquals(expectedSize, payload.length);

        Connection connection = createAmqpConnection();

        long startTime = System.currentTimeMillis();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(payload);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Set this to non-default to get a Header in the encoded message.
        producer.setPriority(4);
        producer.send(message);
        long endTime = System.currentTimeMillis();

        LOG.info("Returned from send after {} ms", endTime - startTime);
        startTime = System.currentTimeMillis();
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();

        LOG.info("Calling receive");
        Message received = consumer.receive();
        assertNotNull(received);
        assertTrue(received instanceof BytesMessage);
        BytesMessage bytesMessage = (BytesMessage) received;
        assertNotNull(bytesMessage);
        endTime = System.currentTimeMillis();

        LOG.info("Returned from receive after {} ms", endTime - startTime);
        byte[] bytesReceived = new byte[expectedSize];
        assertEquals(expectedSize, bytesMessage.readBytes(bytesReceived, expectedSize));
        assertTrue(Arrays.equals(payload, bytesReceived));
        connection.close();
    }
}
