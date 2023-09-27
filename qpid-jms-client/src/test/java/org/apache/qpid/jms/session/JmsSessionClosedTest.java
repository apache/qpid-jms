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

import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

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
        Queue destination = session.createQueue(_testMethodName);
        sender = session.createProducer(destination);
        receiver = session.createConsumer(destination);
        session.close();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestResources();
    }

    @Test
    @Timeout(30)
    public void testSessionCloseAgain() throws Exception {
        session.close();
    }

    @Test
    @Timeout(30)
    public void testCreateMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateTextMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createTextMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateTextMessageWithTextFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createTextMessage("TEST");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateMapMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createMapMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateStreamMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createStreamMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateBytesMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createBytesMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateObjectMessageFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createObjectMessage();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateObjectMessageWithObjectFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createObjectMessage("TEST");
        });
    }

    @Test
    @Timeout(30)
    public void testGetTransactedFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.getTransacted();
        });
    }

    @Test
    @Timeout(30)
    public void testGetAcknowledgeModeFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.getAcknowledgeMode();
        });
    }

    @Test
    @Timeout(30)
    public void testCommitFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.commit();
        });
    }

    @Test
    @Timeout(30)
    public void testRollbackFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.rollback();
        });
    }

    @Test
    @Timeout(30)
    public void testCloseDoesNotFail() throws Exception {
        session.close();
    }

    @Test
    @Timeout(30)
    public void testRecoverFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.recover();
        });
    }

    @Test
    @Timeout(30)
    public void testGetMessageListenerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.getMessageListener();
        });
    }

    @Test
    @Timeout(30)
    public void testSetMessageListenerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            MessageListener listener = new MessageListener() {
                @Override
                public void onMessage(Message message) {
                }
            };
            session.setMessageListener(listener);
        });
    }

    @Test
    @Timeout(30)
    public void testRunFails() throws Exception {
        assertThrows(RuntimeException.class, () -> {
            session.run();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateProducerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Destination destination = session.createQueue("test");
            session.createProducer(destination);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerDestinatioFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Destination destination = session.createQueue("test");
            session.createConsumer(destination);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerDestinatioSelectorFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Destination destination = session.createQueue("test");
            session.createConsumer(destination, "a = b");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerDestinatioSelectorBooleanFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Destination destination = session.createQueue("test");
            session.createConsumer(destination, "a = b", true);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createQueue("TEST");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateTopicFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createTopic("TEST");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateTemporaryQueueFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createTemporaryQueue();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateTemporaryTopicFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.createTemporaryQueue();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableSubscriberFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Topic destination = session.createTopic("TEST");
            session.createDurableSubscriber(destination, "test");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableSubscriberSelectorBooleanFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Topic destination = session.createTopic("TEST");
            session.createDurableSubscriber(destination, "test", "a = b", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableConsumerSelectorBooleanFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Topic destination = session.createTopic("TEST");
            session.createDurableConsumer(destination, "test", "a = b", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueBrowserFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Queue destination = session.createQueue("test");
            session.createBrowser(destination);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueBrowserWithSelectorFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            Queue destination = session.createQueue("test");
            session.createBrowser(destination, "a = b");
        });
    }

    @Test
    @Timeout(30)
    public void testUnsubscribeFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            session.unsubscribe("test");
        });
    }

    // --- Test effects on consumer/producer opened previously on the session ---

    @Test
    @Timeout(30)
    public void testConsumerCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        receiver.close();
    }

    @Test
    @Timeout(30)
    public void testProducerCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        sender.close();
    }

    @Test
    @Timeout(30)
    public void testConsumerGetMessageListenerFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            receiver.getMessageListener();
        });
    }

    @Test
    @Timeout(30)
    public void testProducerGetDestinationFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getDestination();
        });
    }
}
