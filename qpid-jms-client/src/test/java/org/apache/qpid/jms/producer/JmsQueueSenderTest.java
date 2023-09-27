/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueueSession;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

public class JmsQueueSenderTest extends JmsConnectionTestSupport {

    private JmsQueueSession session;
    private final MockRemotePeer remotePeer = new MockRemotePeer();

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        remotePeer.start();
        connection = createConnectionToMockProvider();
        session = (JmsQueueSession) connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        try {
            remotePeer.terminate();
        } finally {
            super.tearDown();
        }
    }

    @Test
    @Timeout(10)
    public void testMultipleCloseCallsNoErrors() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        sender.close();
        sender.close();
    }

    @Test
    @Timeout(10)
    public void testGetQueue() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        assertSame(queue, sender.getQueue());
    }

    @Test
    @Timeout(10)
    public void testSendToQueueWithNullOnExplicitQueueSender() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(null);
        Message message = session.createMessage();
        sender.send(queue, message);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(queue, destination);
    }

    @Test
    @Timeout(10)
    public void testSendToQueueWithDeliveryOptsWithNullOnExplicitQueueSender() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(null);
        Message message = session.createMessage();
        sender.send(queue, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(queue, destination);
    }

    @Test
    @Timeout(10)
    public void testSendToQueueWithNullOnExplicitQueueSenderThrowsInvalidDestinationException() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        Message message = session.createMessage();
        try {
            sender.send((Queue) null, message);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testSendToQueueWithDeliveryOptsWithNullOnExplicitQueueSenderThrowsInvalidDestinationException() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        Message message = session.createMessage();
        try {
            sender.send((Queue) null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }
}
