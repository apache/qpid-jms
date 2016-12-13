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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueueSession;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JmsQueueSenderTest extends JmsConnectionTestSupport {

    private JmsQueueSession session;
    private final MockRemotePeer remotePeer = new MockRemotePeer();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        remotePeer.start();
        connection = createConnectionToMockProvider();
        session = (JmsQueueSession) connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            remotePeer.terminate();
        } finally {
            super.tearDown();
        }
    }

    @Test(timeout = 10000)
    public void testMultipleCloseCallsNoErrors() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        sender.close();
        sender.close();
    }

    @Test(timeout = 10000)
    public void testGetQueue() throws Exception {
        Queue queue = session.createQueue(getTestName());
        QueueSender sender = session.createSender(queue);
        assertSame(queue, sender.getQueue());
    }

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
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
