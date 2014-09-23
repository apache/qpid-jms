/**
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
package org.apache.qpid.jms;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Validates all Session contracts following a close() call.
 */
public class JmsSessionClosedTest extends AmqpTestSupport {

    protected Connection connection;

    protected Session createSession() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.close();
        return session;
    }

    @Override
    public void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateMessageFails() throws Exception {
        Session session = createSession();
        session.createMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTextMessageFails() throws Exception {
        Session session = createSession();
        session.createTextMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTextMessageWithTextFails() throws Exception {
        Session session = createSession();
        session.createTextMessage("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateMapMessageFails() throws Exception {
        Session session = createSession();
        session.createMapMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateStreamMessageFails() throws Exception {
        Session session = createSession();
        session.createStreamMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateBytesMessageFails() throws Exception {
        Session session = createSession();
        session.createBytesMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateObjectMessageFails() throws Exception {
        Session session = createSession();
        session.createObjectMessage();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateObjectMessageWithObjectFails() throws Exception {
        Session session = createSession();
        session.createObjectMessage("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetTransactedFails() throws Exception {
        Session session = createSession();
        session.getTransacted();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetAcknowledgeModeFails() throws Exception {
        Session session = createSession();
        session.getAcknowledgeMode();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCommitFails() throws Exception {
        Session session = createSession();
        session.commit();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRollbackFails() throws Exception {
        Session session = createSession();
        session.rollback();
    }

    @Test(timeout=30000)
    public void testCloseDoesNotFail() throws Exception {
        Session session = createSession();
        session.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRecoverFails() throws Exception {
        Session session = createSession();
        session.recover();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMessageListenerFails() throws Exception {
        Session session = createSession();
        session.getMessageListener();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetMessageListenerFails() throws Exception {
        Session session = createSession();
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        };
        session.setMessageListener(listener);
    }

    @Test(timeout=30000, expected=RuntimeException.class)
    public void testRunFails() throws Exception {
        Session session = createSession();
        session.run();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateProducerFails() throws Exception {
        Session session = createSession();
        Destination destination = session.createQueue("test");
        session.createProducer(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioFails() throws Exception {
        Session session = createSession();
        Destination destination = session.createQueue("test");
        session.createConsumer(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioSelectorFails() throws Exception {
        Session session = createSession();
        Destination destination = session.createQueue("test");
        session.createConsumer(destination, "a = b");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConsumerDestinatioSelectorBooleanFails() throws Exception {
        Session session = createSession();
        Destination destination = session.createQueue("test");
        session.createConsumer(destination, "a = b", true);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueFails() throws Exception {
        Session session = createSession();
        session.createQueue("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTopicFails() throws Exception {
        Session session = createSession();
        session.createTopic("TEST");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTemporaryQueueFails() throws Exception {
        Session session = createSession();
        session.createTemporaryQueue();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateTemporaryTopicFails() throws Exception {
        Session session = createSession();
        session.createTemporaryQueue();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableSubscriberFails() throws Exception {
        Session session = createSession();
        Topic destination = session.createTopic("TEST");
        session.createDurableSubscriber(destination, "test");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableSubscriberSelectorBooleanFails() throws Exception {
        Session session = createSession();
        Topic destination = session.createTopic("TEST");
        session.createDurableSubscriber(destination, "test", "a = b", false);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueBrowserFails() throws Exception {
        Session session = createSession();
        Queue destination = session.createQueue("test");
        session.createBrowser(destination);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateQueueBrowserWithSelectorFails() throws Exception {
        Session session = createSession();
        Queue destination = session.createQueue("test");
        session.createBrowser(destination, "a = b");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testUnsubscribeFails() throws Exception {
        Session session = createSession();
        session.unsubscribe("test");
    }
}
