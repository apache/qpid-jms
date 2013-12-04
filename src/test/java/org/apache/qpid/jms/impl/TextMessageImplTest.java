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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpTextMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TextMessageImplTest extends QpidJmsTestCase
{
    private Delivery _mockDelivery;
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private AmqpConnection _mockAmqpConnection;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
    }

    @Test
    public void testSetGetTextWithNewMessageToSend() throws Exception
    {
        String text = "myTestText";
        TextMessageImpl textMessageImpl = new TextMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        textMessageImpl.setText(text);
        assertEquals(text, textMessageImpl.getText());

        AmqpTextMessage amqpTextMessage = textMessageImpl.getUnderlyingAmqpMessage(false);
        assertEquals(text, amqpTextMessage.getText());
    }

    @Test
    public void testGetTextDefaultWithNewMessageToSend() throws Exception
    {
        TextMessageImpl textMessageImpl = new TextMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        assertNull("expected null string", textMessageImpl.getText());
    }

    @Test
    public void testGetTextWithReceivedMessageNoBodySectionReturnsNull() throws Exception
    {
        Message message = Proton.message();
        AmqpTextMessage testAmqpMessage1 = new AmqpTextMessage(_mockDelivery, message, _mockAmqpConnection);
        TextMessageImpl textMessageImpl = new TextMessageImpl(testAmqpMessage1, _mockSessionImpl,_mockConnectionImpl, null);

        assertNull("expected null string", textMessageImpl.getText());
    }

    @Test
    public void testGetTextWithReceivedMessageAmqpValueSectionReturnsString() throws Exception
    {
        Message message = Proton.message();
        String value = "myAmqpValueString";
        message.setBody(new AmqpValue(value));
        AmqpTextMessage testAmqpMessage1 = new AmqpTextMessage(_mockDelivery, message, _mockAmqpConnection);
        TextMessageImpl textMessageImpl = new TextMessageImpl(testAmqpMessage1, _mockSessionImpl,_mockConnectionImpl, null);

        assertEquals(value, textMessageImpl.getText());
    }
}
