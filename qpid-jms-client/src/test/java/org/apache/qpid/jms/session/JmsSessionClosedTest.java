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
package org.apache.qpid.jms.session;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests behavior after a Session is closed.
 */
public class JmsSessionClosedTest extends JmsConnectionTestSupport {

    protected Session session;
    protected MessageProducer sender;
    protected MessageConsumer receiver;

    protected void createTestResources() throws Exception {
        connection = createConnectionToMockProvider();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testName.getMethodName());
        sender = session.createProducer(destination);
        receiver = session.createConsumer(destination);
        session.close();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestResources();
    }

    @Test(timeout=30000)
    public void testSessionCloseAgain() throws Exception {
        session.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateMessageFails() throws Exception {
        session.createMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTextMessageFails() throws Exception {
        session.createTextMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTextMessageWithTextFails() throws Exception {
        session.createTextMessage("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateMapMessageFails() throws Exception {
        session.createMapMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateStreamMessageFails() throws Exception {
        session.createStreamMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateBytesMessageFails() throws Exception {
        session.createBytesMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateObjectMessageFails() throws Exception {
        session.createObjectMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateObjectMessageWithObjectFails() throws Exception {
        session.createObjectMessage("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetTransactedFails() throws Exception {
        session.getTransacted();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetAcknowledgeModeFails() throws Exception {
        session.getAcknowledgeMode();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCommitFails() throws Exception {
        session.commit();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRollbackFails() throws Exception {
        session.rollback();
    }

    @Test(timeout=30000)
    public void testCloseDoesNotFail() throws Exception {
        session.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRecoverFails() throws Exception {
        session.recover();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMessageListenerFails() throws Exception {
        session.getMessageListener();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetMessageListenerFails() throws Exception {
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        };
        session.setMessageListener(listener);
    }

    @Test(timeout=30000, expected=RuntimeException.class)
    public void testRunFails() throws Exception {
        session.run();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateProducerFails() throws Exception {
        Destination destination = session.createQueue("test");
        session.createProducer(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioFails() throws Exception {
        Destination destination = session.createQueue("test");
        session.createConsumer(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioSelectorFails() throws Exception {
        Destination destination = session.createQueue("test");
        session.createConsumer(destination, "a = b");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioSelectorBooleanFails() throws Exception {
        Destination destination = session.createQueue("test");
        session.createConsumer(destination, "a = b", true);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueFails() throws Exception {
        session.createQueue("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTopicFails() throws Exception {
        session.createTopic("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTemporaryQueueFails() throws Exception {
        session.createTemporaryQueue();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTemporaryTopicFails() throws Exception {
        session.createTemporaryQueue();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableSubscriberFails() throws Exception {
        Topic destination = session.createTopic("TEST");
        session.createDurableSubscriber(destination, "test");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableSubscriberSelectorBooleanFails() throws Exception {
        Topic destination = session.createTopic("TEST");
        session.createDurableSubscriber(destination, "test", "a = b", false);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableConsumerSelectorBooleanFails() throws Exception {
        Topic destination = session.createTopic("TEST");
        session.createDurableConsumer(destination, "test", "a = b", false);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueBrowserFails() throws Exception {
        Queue destination = session.createQueue("test");
        session.createBrowser(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueBrowserWithSelectorFails() throws Exception {
        Queue destination = session.createQueue("test");
        session.createBrowser(destination, "a = b");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testUnsubscribeFails() throws Exception {
        session.unsubscribe("test");
    }

    // --- Test effects on consumer/producer opened previously on the session ---

    @Test(timeout=30000)
    public void testConsumerCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        receiver.close();
    }

    @Test(timeout=30000)
    public void testProducerCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        sender.close();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testConsumerGetMessageListenerFails() throws Exception {
        receiver.getMessageListener();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testProducerGetDestinationFails() throws Exception {
        sender.getDestination();
    }
}
