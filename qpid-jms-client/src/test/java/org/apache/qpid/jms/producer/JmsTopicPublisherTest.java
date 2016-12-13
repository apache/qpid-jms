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
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsTopicSession;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JmsTopicPublisherTest extends JmsConnectionTestSupport {

    private JmsTopicSession session;
    private final MockRemotePeer remotePeer = new MockRemotePeer();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        remotePeer.start();
        connection = createConnectionToMockProvider();
        session = (JmsTopicSession) connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
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
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.close();
        publisher.close();
    }

    @Test(timeout = 10000)
    public void testGetTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        assertSame(topic, publisher.getTopic());
    }

    @Test(timeout = 10000)
    public void testPublishMessage() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();
        publisher.publish(message);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test(timeout = 10000)
    public void testPublishMessageOnProvidedTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(null);
        Message message = session.createMessage();
        publisher.publish(topic, message);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test(timeout = 10000)
    public void testPublishMessageOnProvidedTopicWhenNotAnonymous() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();

        try {
            publisher.publish(session.createTopic(getTestName() + "1"), message);
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException uoe) {}

        try {
            publisher.publish((Topic) null, message);
            fail("Should throw InvalidDestinationException");
        } catch (InvalidDestinationException ide) {}
    }

    @Test(timeout = 10000)
    public void testPublishMessageWithDeliveryOptions() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();
        publisher.publish(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test(timeout = 10000)
    public void testPublishMessageWithOptionsOnProvidedTopic() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(null);
        Message message = session.createMessage();
        publisher.publish(topic, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        Destination destination = message.getJMSDestination();
        assertEquals(topic, destination);
    }

    @Test(timeout = 10000)
    public void testPublishMessageWithOptionsOnProvidedTopicWhenNotAnonymous() throws Exception {
        Topic topic = session.createTopic(getTestName());
        TopicPublisher publisher = session.createPublisher(topic);
        Message message = session.createMessage();

        try {
            publisher.publish(session.createTopic(getTestName() + "1"), message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException uoe) {}

        try {
            publisher.publish((Topic) null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Should throw InvalidDestinationException");
        } catch (InvalidDestinationException ide) {}
    }
}
