/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.impl;

import static org.junit.Assert.*;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpBytesMessage;
import org.apache.qpid.jms.engine.AmqpGenericMessage;
import org.apache.qpid.jms.engine.AmqpListMessage;
import org.apache.qpid.jms.engine.AmqpMapMessage;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpObjectMessage;
import org.apache.qpid.jms.engine.AmqpTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MessageFactoryImplTest extends QpidJmsTestCase
{

    private ConnectionImpl _mockConnection;
    private SessionImpl _mockSession;
    private MessageFactoryImpl _messageFactoryImpl;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockConnection = Mockito.mock(ConnectionImpl.class);
        _mockSession = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSession.getDestinationHelper()).thenReturn(new DestinationHelper());
        _messageFactoryImpl = new MessageFactoryImpl();
    }

    @Test
    public void testCreateJmsMessageWithAmqpGenericMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpGenericMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(GenericAmqpMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithAmqpTextMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpTextMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(TextMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithAmqpBytesMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpBytesMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(BytesMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithAmqpObjectMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpObjectMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(ObjectMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithAmqpListMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpListMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(StreamMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithAmqpMapMessage() throws Exception
    {
        AmqpMessage amqpMessage = Mockito.mock(AmqpMapMessage.class);
        Message jmsMessage = _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
        assertEquals(MapMessageImpl.class, jmsMessage.getClass());
    }

    @Test
    public void testCreateJmsMessageWithUnknownMessageTypeThrowsException() throws Exception
    {
        //AmqpMessage is the abstract supertype, factory doesn't expect it, so using a mock of it
        AmqpMessage amqpMessage = Mockito.mock(AmqpMessage.class);
        try
        {
            _messageFactoryImpl.createJmsMessage(amqpMessage, _mockSession, _mockConnection, null);
            fail("expected exception was not thrown");
        }
        catch(JMSException jmse)
        {
            //expected
        }
    }
}
