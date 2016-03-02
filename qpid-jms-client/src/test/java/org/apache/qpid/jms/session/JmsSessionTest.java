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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsSession;
import org.junit.Before;
import org.junit.Test;

/**
 * Test basic contracts of the JmsSession class using a mocked connection.
 */
public class JmsSessionTest extends JmsConnectionTestSupport {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockProvider();
    }

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
    public void testGetAcknowledgementMode() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
    }

    @Test(timeout = 10000)
    public void testIsAutoAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertTrue(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
    }

    @Test(timeout = 10000)
    public void testIsDupsOkAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertFalse(session.isClientAcknowledge());
        assertTrue(session.isDupsOkAcknowledge());
    }

    @Test(timeout = 10000)
    public void testIsClientAcknowledge() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertFalse(session.isAutoAcknowledge());
        assertTrue(session.isClientAcknowledge());
        assertFalse(session.isDupsOkAcknowledge());
    }

    @Test(timeout = 10000)
    public void testIsTransacted() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session.isTransacted());
        session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        assertTrue(session.isTransacted());
    }

    @Test(timeout = 10000, expected=IllegalStateException.class)
    public void testRecoverThrowsForTxSession() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        session.recover();
    }

    @Test(timeout = 10000)
    public void testRecoverWithNoSessionActivity() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.recover();
    }

    @Test(timeout = 10000, expected=JMSException.class)
    public void testRollbackThrowsOnNonTxSession() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.rollback();
    }

    @Test(timeout = 10000, expected=JMSException.class)
    public void testCommitThrowsOnNonTxSession() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.commit();
    }

    @Test(timeout = 10000)
    public void testCreateMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMessage());
    }

    @Test(timeout = 10000)
    public void testCreateBytesMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createBytesMessage());
    }

    @Test(timeout = 10000)
    public void testCreateStreamMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createStreamMessage());
    }

    @Test(timeout = 10000)
    public void testCreateMapMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMapMessage());
    }

    @Test(timeout = 10000)
    public void testCreateObjectMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createObjectMessage());
    }

    @Test(timeout = 10000)
    public void testCreateObjectMessageWithValue() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ObjectMessage message = session.createObjectMessage("TEST-MESSAGE");
        assertNotNull(message);
        assertNotNull(message.getObject());
        assertTrue(message.getObject() instanceof String);
        assertEquals("TEST-MESSAGE", message.getObject());
    }

    @Test(timeout = 10000)
    public void testCreateTextMessage() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createTextMessage());
    }

    @Test(timeout = 10000)
    public void testCreateTextMessageWithValue() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TextMessage message = session.createTextMessage("TEST-MESSAGE");
        assertNotNull(message);
        assertEquals("TEST-MESSAGE", message.getText());
    }

    @Test(timeout = 10000)
    public void testUnsubscribe() throws JMSException {
        JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("some-subscription");
    }
}
