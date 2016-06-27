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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMapMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;
import org.apache.qpid.jms.message.facade.JmsMapMessageFacade;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.message.facade.JmsStreamMessageFacade;
import org.apache.qpid.jms.message.facade.JmsTextMessageFacade;
import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the AMQP JMS Message Factory class.
 */
public class AmqpJmsMessageFactoryTest extends QpidJmsTestCase {

    @Test
    public void testAmqpJmsMessageFactoryAmqpConnection() {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        assertNotNull(factory.getAmqpConnection());
    }

    @Test
    public void testCreateMessage() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsMessage message = factory.createMessage();
        JmsMessageFacade facade = message.getFacade();
        assertTrue(facade instanceof AmqpJmsMessageFacade);
        AmqpJmsMessageFacade amqpMessage = (AmqpJmsMessageFacade) facade;

        assertEquals(JMS_MESSAGE, amqpMessage.getJmsMsgType());
    }

    @Test
    public void testCreateTextMessage() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsTextMessage message = factory.createTextMessage();
        JmsTextMessageFacade facade = (JmsTextMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsTextMessageFacade);
        AmqpJmsTextMessageFacade amqpMessage = (AmqpJmsTextMessageFacade) facade;

        assertEquals(JMS_TEXT_MESSAGE, amqpMessage.getJmsMsgType());
        assertNull(amqpMessage.getText());
    }

    @Test
    public void testCreateTextMessageString() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsTextMessage message = factory.createTextMessage("SomeValue");
        JmsTextMessageFacade facade = (JmsTextMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsTextMessageFacade);
        AmqpJmsTextMessageFacade amqpMessage = (AmqpJmsTextMessageFacade) facade;

        assertEquals(JMS_TEXT_MESSAGE, amqpMessage.getJmsMsgType());
        assertEquals("SomeValue", amqpMessage.getText());
    }

    @Test
    public void testCreateBytesMessage() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsBytesMessage message = factory.createBytesMessage();
        JmsBytesMessageFacade facade = (JmsBytesMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsBytesMessageFacade);
        AmqpJmsBytesMessageFacade amqpMessage = (AmqpJmsBytesMessageFacade) facade;

        assertEquals(JMS_BYTES_MESSAGE, amqpMessage.getJmsMsgType());
        assertEquals(0, amqpMessage.getBodyLength());
    }

    @Test
    public void testCreateMapMessage() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsMapMessage message = factory.createMapMessage();
        JmsMapMessageFacade facade = (JmsMapMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsMapMessageFacade);
        AmqpJmsMapMessageFacade amqpMessage = (AmqpJmsMapMessageFacade) facade;

        assertEquals(JMS_MAP_MESSAGE, amqpMessage.getJmsMsgType());
        assertFalse(amqpMessage.getMapNames().hasMoreElements());
    }

    @Test
    public void testCreateStreamMessage() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsStreamMessage message = factory.createStreamMessage();
        JmsStreamMessageFacade facade = (JmsStreamMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsStreamMessageFacade);
        AmqpJmsStreamMessageFacade amqpMessage = (AmqpJmsStreamMessageFacade) facade;

        assertEquals(JMS_STREAM_MESSAGE, amqpMessage.getJmsMsgType());
        assertFalse(amqpMessage.hasNext());
    }

    @Test
    public void testCreateObjectMessage() throws JMSException, ClassNotFoundException, IOException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsObjectMessage message = factory.createObjectMessage();
        JmsObjectMessageFacade facade = (JmsObjectMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsObjectMessageFacade);
        AmqpJmsObjectMessageFacade amqpMessage = (AmqpJmsObjectMessageFacade) facade;

        assertEquals(JMS_OBJECT_MESSAGE, amqpMessage.getJmsMsgType());
        assertFalse(amqpMessage.isAmqpTypedEncoding());
        assertNull(amqpMessage.getObject());
    }

    @Test
    public void testCreateObjectMessageHonorsAmqpTypeConfig() throws JMSException, ClassNotFoundException, IOException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnectionAmqpTypes());
        JmsObjectMessage message = factory.createObjectMessage();
        JmsObjectMessageFacade facade = (JmsObjectMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsObjectMessageFacade);
        AmqpJmsObjectMessageFacade amqpMessage = (AmqpJmsObjectMessageFacade) facade;

        assertEquals(JMS_OBJECT_MESSAGE, amqpMessage.getJmsMsgType());
        assertTrue(amqpMessage.isAmqpTypedEncoding());
        assertNull(amqpMessage.getObject());
    }

    @Test
    public void testCreateObjectMessageSerializable() throws JMSException, ClassNotFoundException, IOException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        JmsObjectMessage message = factory.createObjectMessage(new ArrayList<Boolean>());
        JmsObjectMessageFacade facade = (JmsObjectMessageFacade) message.getFacade();
        assertTrue(facade instanceof AmqpJmsObjectMessageFacade);
        AmqpJmsObjectMessageFacade amqpMessage = (AmqpJmsObjectMessageFacade) facade;

        assertEquals(JMS_OBJECT_MESSAGE, amqpMessage.getJmsMsgType());
        assertFalse(amqpMessage.isAmqpTypedEncoding());
        assertNotNull(amqpMessage.getObject());
    }

    @Test
    public void testCreateObjectMessageWithBadTypeThrowsJMSException() throws JMSException {
        AmqpJmsMessageFactory factory = new AmqpJmsMessageFactory(createMockAmqpConnection());
        try {
            factory.createObjectMessage(new NotSerializable());
            fail("Should have thrown an exception");
        } catch (JMSException e) {
        }
    }

    private class NotSerializable implements Serializable {

        private static final long serialVersionUID = 1L;

        private void writeObject(ObjectOutputStream stream) throws IOException {
            throw new IOException();
        }
    }

    private AmqpConnection createMockAmqpConnectionAmqpTypes() {
        JmsConnectionId connectionId = new JmsConnectionId("ID:MOCK:1");
        AmqpConnection connection = Mockito.mock(AmqpConnection.class);
        Mockito.when(connection.isObjectMessageUsesAmqpTypes()).thenReturn(true);
        Mockito.when(connection.getResourceInfo()).thenReturn(new JmsConnectionInfo(connectionId));

        return connection;
    }

    private AmqpConnection createMockAmqpConnection() {
        JmsConnectionId connectionId = new JmsConnectionId("ID:MOCK:1");
        AmqpConnection connection = Mockito.mock(AmqpConnection.class);
        Mockito.when(connection.getResourceInfo()).thenReturn(new JmsConnectionInfo(connectionId));

        return connection;
    }
}
