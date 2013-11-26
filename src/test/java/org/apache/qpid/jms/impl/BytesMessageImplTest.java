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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpBytesMessage;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BytesMessageImplTest extends QpidJmsTestCase
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
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithNoBodySection() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage testAmqpMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(testAmqpMessage, _mockSessionImpl,_mockConnectionImpl);

        assertEquals(0, bytesMessageImpl.getBodyLength());
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithNoBodySectionReturnsEOS() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage testAmqpMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(testAmqpMessage, _mockSessionImpl,_mockConnectionImpl);

        //verify attempting to read bytes returns -1, i.e EOS
        assertEquals(-1, bytesMessageImpl.readBytes(new byte[2]));
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithDataSectionReturnsBytes() throws Exception
    {
        byte[] bytes = "myBytesData".getBytes();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals(-1, bytesMessageImpl.readBytes(receivedBytes));

        assertEquals(bytes.length, bytesMessageImpl.getBodyLength());
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithAmpValueSectionReturnsBytes() throws Exception
    {
        byte[] bytes = "myBytesAmqpValue".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals(-1, bytesMessageImpl.readBytes(receivedBytes));

        assertEquals(bytes.length, bytesMessageImpl.getBodyLength());
    }
}
