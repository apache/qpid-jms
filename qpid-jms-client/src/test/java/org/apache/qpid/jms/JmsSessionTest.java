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
package org.apache.qpid.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Test basic contracts of the JmsSession class using a mocked connection.
 */
public class JmsSessionTest extends JmsConnectionTestSupport {

    private static final int NO_ACKNOWLEDGE = 257;
    private static final int ARTEMIS_PRE_ACKNOWLEDGE = 100;
    private static final int INDIVIDUAL_ACKNOWLEDGE = 101;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        connection = createConnectionToMockProvider();
    }

    @Test
    @Timeout(10)
    public void testGetMessageListener() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNull(session.getMessageListener());
        session.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        assertNotNull(session.getMessageListener());
    }

    @Test
    @Timeout(10)
    public void testGetAcknowledgementMode() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, NO_ACKNOWLEDGE);
        assertEquals(NO_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, ARTEMIS_PRE_ACKNOWLEDGE);
        assertEquals(ARTEMIS_PRE_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, INDIVIDUAL_ACKNOWLEDGE);
        assertEquals(INDIVIDUAL_ACKNOWLEDGE, session.getAcknowledgeMode());
    }

    @Test
    @Timeout(10)
    public void testIsAutoAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertTrue(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
        assertFalse(session.isNoAcknowledge());
        assertFalse(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testIsDupsOkAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertTrue(session.isDupsOkAcknowledge());
        assertFalse(session.isNoAcknowledge());
        assertFalse(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testIsClientAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertTrue(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
        assertFalse(session.isNoAcknowledge());
        assertFalse(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testIsNoAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, NO_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
        assertTrue(session.isNoAcknowledge());
        assertFalse(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testIsNoAcknowledgeWithArtemisMode() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, ARTEMIS_PRE_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
        assertTrue(session.isNoAcknowledge());
        assertFalse(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testIsTransacted() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isTransacted());
        session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        assertTrue(session.isTransacted());
    }

    @Test
    @Timeout(10)
    public void testIsIndividualAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, INDIVIDUAL_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
        assertFalse(session.isNoAcknowledge());
        assertTrue(session.isIndividualAcknowledge());
    }

    @Test
    @Timeout(10)
    public void testRecoverThrowsForTxSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
            session.recover();
        });
    }

    @Test
    @Timeout(10)
    public void testRecoverWithNoSessionActivity() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.recover();
    }

    @Test
    @Timeout(10)
    public void testRollbackThrowsOnNonTxSession() throws JMSException {
        assertThrows(JMSException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.rollback();
        });
    }

    @Test
    @Timeout(10)
    public void testCommitThrowsOnNonTxSession() throws JMSException {
        assertThrows(JMSException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.commit();
        });
    }

    @Test
    @Timeout(10)
    public void testCreateMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateBytesMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createBytesMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateStreamMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createStreamMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateMapMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMapMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateObjectMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createObjectMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateObjectMessageWithValue() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ObjectMessage message = session.createObjectMessage("TEST-MESSAGE");
        assertNotNull(message);
        assertNotNull(message.getObject());
        assertTrue(message.getObject() instanceof String);
        assertEquals("TEST-MESSAGE", message.getObject());
    }

    @Test
    @Timeout(10)
    public void testCreateTextMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createTextMessage());
    }

    @Test
    @Timeout(10)
    public void testCreateTextMessageWithValue() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TextMessage message = session.createTextMessage("TEST-MESSAGE");
        assertNotNull(message);
        assertEquals("TEST-MESSAGE", message.getText());
    }

    @Test
    @Timeout(10)
    public void testUnsubscribe() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("some-subscription");
    }

    @Test
    @Timeout(10)
    public void testCreateConsumerNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(null);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateConsumerNullDestinationWithSelectorThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(null, "a > b");
        });
    }

    @Test
    @Timeout(10)
    public void testCreateConsumerNullDestinationWithSelectorNoLocalThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(null, "a > b", true);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateReceiverNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createReceiver(null);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateReceiverNullDestinationWithSelectorThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(null, "a > b");
        });
    }

    @Test
    @Timeout(10)
    public void testCreateBrowserNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createBrowser(null);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateBrowserNullDestinationWithSelectorThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createBrowser(null, "a > b");
        });
    }

    @Test
    @Timeout(10)
    public void testCreateSubscriberNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createSubscriber(null);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateSubscriberNullDestinationWithSelectorNoLocalThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createSubscriber(null, "a > b", true);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateDurableSubscriberNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(null, "name");
        });
    }

    @Test
    @Timeout(10)
    public void testCreateDurableSubscriberNullDestinationWithSelectorNoLocalThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(null, "name", "a > b", true);
        });
    }

    @Test
    @Timeout(10)
    public void testCreateDurableConsumerNullDestinationThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableConsumer(null, "name");
        });
    }

    @Test
    @Timeout(10)
    public void testCreateDurableConsumerNullDestinationWithSelectorNoLocalThrowsIDE() throws JMSException {
        assertThrows(InvalidDestinationException.class, () -> {
            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableConsumer(null, "name", "a > b", true);
        });
    }

    @Test
    @Timeout(10)
    public void testSendWithNullDestThrowsIDE() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JmsMessageProducer mockProducer = Mockito.mock(JmsMessageProducer.class);

        try {
            session.send(mockProducer, null, null, 0, 0, 0, true, true, 0, null);
            fail("Should not be able to send");
        } catch (InvalidDestinationException idex) {}
    }

    @Test
    @Timeout(10)
    public void testCannotCreateConsumerOnTempDestinationFromSomeOtherSource() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = new JmsTemporaryQueue("ID:" + UUID.randomUUID().toString());

        try {
            session.createConsumer(tempQueue);
            fail("Should not be able to create a consumer");
        } catch (InvalidDestinationException idex) {}
    }

    @Test
    @Timeout(10)
    public void testCannotCreateConsumerOnDeletedTemporaryDestination() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(tempQueue);

        try {
            producer.send(session.createMessage());
        } catch (Exception ex) {
            fail("Should be able to send to this temporary destination");
        }

        tempQueue.delete();

        try {
            producer.send(session.createMessage());
            fail("Should not be able to send to this temporary destination");
        } catch (IllegalStateException ise) {}
    }

    @Test
    @Timeout(10)
    public void testSessionRunFailsWhenSessionIsClosed() throws Exception {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        session.close();

        try {
            session.run();
            fail("Session is closed.");
        } catch (RuntimeException re) {}
    }

    @Test
    @Timeout(10)
    public void testCreateSharedConsumer() throws Exception {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic("test");
        JmsMessageConsumer consumer = (JmsMessageConsumer) session.createSharedConsumer(topic, "subscription");

        assertNotNull(consumer);
        assertNull(consumer.getMessageSelector(), "unexpected selector");
        assertEquals(topic, consumer.getDestination(), "unexpected topic");
    }

    @Test
    @Timeout(10)
    public void testCreateSharedConsumerWithSelector() throws Exception {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String selector = "a = b";
        Topic topic = session.createTopic("test");
        JmsMessageConsumer consumer = (JmsMessageConsumer) session.createSharedConsumer(topic, "subscription", selector);

        assertNotNull(consumer);
        assertEquals(selector, consumer.getMessageSelector(), "unexpected selector");
        assertEquals(topic, consumer.getDestination(), "unexpected topic");
    }

    @Test
    @Timeout(10)
    public void testCreateSharedDurableConsumer() throws Exception {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic("test");
        JmsMessageConsumer consumer = (JmsMessageConsumer) session.createSharedDurableConsumer(topic, "subscription");

        assertNotNull(consumer);
        assertNull(consumer.getMessageSelector(), "unexpected selector");
        assertEquals(topic, consumer.getDestination(), "unexpected topic");
    }

    @Test
    @Timeout(10)
    public void testCreateSharedDurableConsumerWithSelector() throws Exception {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String selector = "a = b";
        Topic topic = session.createTopic("test");
        JmsMessageConsumer consumer = (JmsMessageConsumer) session.createSharedDurableConsumer(topic, "subscription", selector);

        assertNotNull(consumer);
        assertEquals(selector, consumer.getMessageSelector(), "unexpected selector");
        assertEquals(topic, consumer.getDestination(), "unexpected topic");
    }
}
