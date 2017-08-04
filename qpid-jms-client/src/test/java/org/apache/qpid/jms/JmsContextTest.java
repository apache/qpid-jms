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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

import java.io.Serializable;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test various aspects of the JmsContext class.
 */
public class JmsContextTest extends JmsConnectionTestSupport {

    private final MockRemotePeer remotePeer = new MockRemotePeer();

    private JmsContext context;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        remotePeer.start();
        context = createJMSContextToMockProvider();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (context != null) {
            context.close();
        }
    }

    //----- Test basic interface behaviors -----------------------------------//

    @Test
    public void testCreateContextWithNewAcknowledgementMode() {
        JMSContext newContext = context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        try {
            assertNotNull(newContext);
            assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, newContext.getSessionMode());
        } finally {
            newContext.close();
        }
    }

    @Test
    public void testGetMeataData() {
        assertNotNull(context.getMetaData());
    }

    @Test
    public void testGetTransactedFromContext() {
        assertFalse(context.getTransacted());
        JMSContext newContext = context.createContext(JMSContext.SESSION_TRANSACTED);
        try {
            assertNotNull(newContext);
            assertEquals(JMSContext.SESSION_TRANSACTED, newContext.getSessionMode());
            assertTrue(newContext.getTransacted());
        } finally {
            newContext.close();
        }
    }

    @Test
    public void testCreateContextFromClosedContextThrowsISRE() {
        context.close();
        try {
            context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
            fail("Should throw state exception");
        } catch (IllegalStateRuntimeException isre) {
        }
    }

    @Test
    public void testCreateTextMessage() throws JMSException {
        TextMessage message = context.createTextMessage();
        assertNotNull(message);
        assertNull(message.getText());
    }

    @Test
    public void testCreateTextMessageWithBody() throws JMSException {
        TextMessage message = context.createTextMessage("test");
        assertNotNull(message);
        assertEquals("test", message.getText());
    }

    @Test
    public void testCreateBytesMessage() throws JMSException {
        BytesMessage message = context.createBytesMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateMapMessage() throws JMSException {
        MapMessage message = context.createMapMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateMessage() throws JMSException {
        Message message = context.createMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateStreamMessage() throws JMSException {
        StreamMessage message = context.createStreamMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateObjectMessage() throws JMSException {
        ObjectMessage message = context.createObjectMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateObjectMessageWithBody() throws JMSException {
        UUID payload = UUID.randomUUID();
        ObjectMessage message = context.createObjectMessage(payload);
        assertNotNull(message);
        assertEquals(payload, message.getObject());
    }

    @Test
    public void testInternalSessionLazyCreate() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        // No session until needed.
        Mockito.verify(connection, Mockito.times(0)).createSession(anyInt());
        assertNotNull(context.createTemporaryQueue());
        Mockito.verify(connection, Mockito.times(1)).createSession(anyInt());

        context.close();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(anyInt())).thenReturn(session);
        Mockito.when(session.createConsumer(any(Destination.class))).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createConsumer(context.createTemporaryQueue());
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Destination.class));
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(anyInt())).thenReturn(session);
        Mockito.when(session.createConsumer(any(Destination.class))).thenReturn(consumer);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createConsumer(context.createTemporaryQueue());
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Destination.class));
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionMessageConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createConsumer(any(Destination.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createConsumer(context.createTemporaryTopic(), "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionMessageConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createConsumer(any(Destination.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createConsumer(context.createTemporaryTopic(), "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionMessageConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(session.createConsumer(any(Destination.class), anyString(), anyBoolean())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createConsumer(context.createTemporaryTopic(), "a = b", true);
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Topic.class), anyString(), anyBoolean());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionMessageConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(session.createConsumer(any(Destination.class), anyString(), anyBoolean())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createConsumer(context.createTemporaryTopic(), "a = b", true);
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Topic.class), anyString(), anyBoolean());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionDurableMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createDurableConsumer(any(Topic.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionDurableMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(session.createDurableConsumer(any(Topic.class), anyString())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionDurableMessageConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name", "a = b", true);
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionDurableMessageConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name", "a = b", true);
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionSharedMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedConsumer(any(Topic.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionSharedMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(session.createSharedConsumer(any(Topic.class), anyString())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionMessageSharedConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(session.createSharedConsumer(any(Topic.class), anyString(), anyString())).thenReturn(consumer);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name", "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString(), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionSharedMessageConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedConsumer(any(Topic.class), anyString(), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name", "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString(), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionSharedDurableMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedDurableConsumer(any(Topic.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionSharedDurableMessageConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedDurableConsumer(any(Topic.class), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionMessageSharedDurableConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedDurableConsumer(any(Topic.class), anyString(), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name", "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString(), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionSharedDurableMessageConsumerSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer consumer = Mockito.mock(JmsMessageConsumer.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createSharedDurableConsumer(any(Topic.class), anyString(), anyString())).thenReturn(consumer);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name", "a = b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString(), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionQueueBrowser() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsQueueBrowser browser = Mockito.mock(JmsQueueBrowser.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createBrowser(any(Queue.class))).thenReturn(browser);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createBrowser(context.createTemporaryQueue());
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class));
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionQueueBrowser() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsQueueBrowser browser = Mockito.mock(JmsQueueBrowser.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createBrowser(any(Queue.class))).thenReturn(browser);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createBrowser(context.createTemporaryQueue());
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class));
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAutoStartOnDoesStartTheConnectionQueueBrowserWithSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsQueueBrowser browser = Mockito.mock(JmsQueueBrowser.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createBrowser(any(Queue.class))).thenReturn(browser);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(true);

        try {
            context.createBrowser(context.createTemporaryQueue(), "a == b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class), anyString());
        Mockito.verify(connection, Mockito.times(1)).start();
    }

    @Test
    public void testAutoStartOffDoesNotStartTheConnectionQueueBrowserWithSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsQueueBrowser browser = Mockito.mock(JmsQueueBrowser.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createBrowser(any(Queue.class))).thenReturn(browser);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);
        context.setAutoStart(false);

        try {
            context.createBrowser(context.createTemporaryQueue(), "a == b");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class), anyString());
        Mockito.verify(connection, Mockito.times(0)).start();
    }

    @Test
    public void testAcknowledgeNoopAutoAcknowledge() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.acknowledge();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(0)).acknowledge(ACK_TYPE.ACCEPTED);
    }

    @Test
    public void testAcknowledgeNoopDupsOkAcknowledge() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.DUPS_OK_ACKNOWLEDGE);

        try {
            context.acknowledge();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(0)).acknowledge(ACK_TYPE.ACCEPTED);
    }

    @Test
    public void testAcknowledgeNoopSessionTransacted() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        try {
            context.acknowledge();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(0)).acknowledge(ACK_TYPE.ACCEPTED);
    }

    @Test
    public void testAcknowledgeAcceptsMessages() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        try {
            context.acknowledge();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).acknowledge(ACK_TYPE.ACCEPTED);
    }

    //----- Test that calls pass through to the underlying connection --------//

    @Test
    public void testStopPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        try {
            context.stop();
        } finally {
            context.close();
        }

        Mockito.verify(connection, Mockito.times(1)).stop();
    }

    @Test
    public void testCommitPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        try {
            context.commit();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).commit();
    }

    @Test
    public void testRollbackPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        try {
            context.rollback();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).rollback();
    }

    @Test
    public void testRecoverPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        try {
            context.recover();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).recover();
    }

    @Test
    public void testUnsubscribePassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        try {
            context.unsubscribe("subscription");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).unsubscribe(anyString());
    }

    @Test
    public void testCreateTopicPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.createTopic("test");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createTopic(anyString());
    }

    @Test
    public void testCreateQueuePassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.createQueue("test");
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createQueue(anyString());
    }

    @Test
    public void testCreateTemporaryQueuePassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.createTemporaryQueue();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createTemporaryQueue();
    }

    @Test
    public void testCreateTemporaryTopicPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);

        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);

        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.createTemporaryTopic();
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createTemporaryTopic();
    }

    @Test
    public void testGetClientIDPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.getClientID();
        } finally {
            context.close();
        }

        Mockito.verify(connection, Mockito.times(1)).getClientID();
    }

    @Test
    public void testSetClientIDPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.setClientID("test");
        } finally {
            context.close();
        }

        Mockito.verify(connection, Mockito.times(1)).setClientID("test");
    }

    @Test
    public void testGetExceptionListenerPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.getExceptionListener();
        } finally {
            context.close();
        }

        Mockito.verify(connection, Mockito.times(1)).getExceptionListener();
    }

    @Test
    public void testSetExceptionListenerPassthrough() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        ExceptionListener listener = new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        };

        try {
            context.setExceptionListener(listener);
        } finally {
            context.close();
        }

        Mockito.verify(connection, Mockito.times(1)).setExceptionListener(listener);
    }

    //----- Test JMSException handling ---------------------------------------//

    @Test
    public void testRuntimeExceptionOnConnectionClose() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).close();

        try {
            context.close();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        }
    }

    @Test
    public void testContextClosePreservesSessionCloseException() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).close();
        Mockito.doThrow(JMSSecurityException.class).when(connection).close();

        context.createTemporaryTopic();
        Mockito.verify(connection, Mockito.times(1)).createSession(JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.close();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateProducer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createProducer(null);

        try {
            context.createProducer();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createConsumer(any(Destination.class));

        try {
            context.createConsumer(context.createTemporaryQueue());
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Destination.class));
    }

    @Test
    public void testRuntimeExceptionOnCreateConsumerWithSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createConsumer(any(Destination.class), anyString());

        try {
            context.createConsumer(context.createTemporaryQueue(), "a = b");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Destination.class), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateConsumerWithSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createConsumer(any(Destination.class), anyString(), anyBoolean());

        try {
            context.createConsumer(context.createTemporaryQueue(), "a = b", true);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createConsumer(any(Destination.class), anyString(), anyBoolean());
    }

    @Test
    public void testRuntimeExceptionOnCreateDurableConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createDurableConsumer(any(Topic.class), anyString());

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateDurableConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).
            createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean());

        try {
            context.createDurableConsumer(context.createTemporaryTopic(), "name", "a = b", true);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createDurableConsumer(any(Topic.class), anyString(), anyString(), anyBoolean());
    }

    @Test
    public void testRuntimeExceptionOnCreateSharedConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createSharedConsumer(any(Topic.class), anyString());

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateSharedConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).
            createSharedConsumer(any(Topic.class), anyString(), anyString());

        try {
            context.createSharedConsumer(context.createTemporaryTopic(), "name", "a = b");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedConsumer(any(Topic.class), anyString(), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateSharedDurableConsumer() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createSharedDurableConsumer(any(Topic.class), anyString());

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateSharedDurableConsumerSelectorNoLocal() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryTopic()).thenReturn(new JmsTemporaryTopic());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).
        createSharedDurableConsumer(any(Topic.class), anyString(), anyString());

        try {
            context.createSharedDurableConsumer(context.createTemporaryTopic(), "name", "a = b");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createSharedDurableConsumer(any(Topic.class), anyString(), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateQueueBrowser() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createBrowser(any(Queue.class));

        try {
            context.createBrowser(context.createTemporaryQueue());
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class));
    }

    @Test
    public void testRuntimeExceptionOnCreateQueueBrowserWithSelector() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createBrowser(any(Queue.class), anyString());

        try {
            context.createBrowser(context.createTemporaryQueue(), "a == b");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }

        Mockito.verify(session, Mockito.times(1)).createBrowser(any(Queue.class), anyString());
    }

    @Test
    public void testRuntimeExceptionOnCreateSession() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).createSession(anyInt());

        try {
            context.createTemporaryQueue();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnStartFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).start();

        try {
            context.start();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnStopFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        Mockito.doThrow(IllegalStateException.class).when(connection).stop();
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        try {
            context.stop();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCommitFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        Mockito.doThrow(IllegalStateException.class).when(session).commit();

        try {
            context.commit();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnRollbackFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.SESSION_TRANSACTED);

        Mockito.doThrow(IllegalStateException.class).when(session).rollback();

        try {
            context.rollback();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnRecoverFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).recover();

        try {
            context.recover();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnAcknowledgeFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).acknowledge(ACK_TYPE.ACCEPTED);

        try {
            context.acknowledge();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnUnsubscribeFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).unsubscribe(anyString());

        try {
            context.unsubscribe("subscription");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateTopicFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createTopic(anyString());

        try {
            context.createTopic("test");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateQueueFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createQueue(anyString());

        try {
            context.createQueue("test");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateTemporaryTopicFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createTemporaryTopic();

        try {
            context.createTemporaryTopic();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateTemporaryQueueFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createTemporaryQueue();

        try {
            context.createTemporaryQueue();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnGetClientIDFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).getClientID();

        try {
            context.getClientID();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnSetClientIDFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).setClientID(anyString());

        try {
            context.setClientID("client");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnGetExceptionListenerFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).getExceptionListener();

        try {
            context.getExceptionListener();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnSetExceptionListenerFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).setExceptionListener(nullable(ExceptionListener.class));

        try {
            context.setExceptionListener(null);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnGetMetaDataFailure() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(connection).getMetaData();

        try {
            context.getMetaData();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createMessage();

        try {
            context.createMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateTextMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createTextMessage();

        try {
            context.createTextMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateTextMessageWithBody() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createTextMessage(anyString());

        try {
            context.createTextMessage("test");
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateBytesMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createBytesMessage();

        try {
            context.createBytesMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateStreamMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createStreamMessage();

        try {
            context.createStreamMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateMapMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createMapMessage();

        try {
            context.createMapMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateObjectMessage() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createObjectMessage();

        try {
            context.createObjectMessage();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnCreateObjectMessageWithBody() throws JMSException {
        JmsConnection connection = Mockito.mock(JmsConnection.class);
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(connection.createSession(Mockito.anyInt())).thenReturn(session);
        JmsContext context = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        Mockito.doThrow(IllegalStateException.class).when(session).createObjectMessage(any(Serializable.class));

        try {
            context.createObjectMessage(UUID.randomUUID());
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            context.close();
        }
    }
}
