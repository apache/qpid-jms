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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Message;
import javax.jms.Session;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpGenericMessage;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpReceiver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReceiverImplTest extends QpidJmsTestCase
{
    private ConnectionImpl _mockConnection;
    private AmqpReceiver _mockAmqpReceiver;
    private SessionImpl _mockSession;
    private AmqpMessage _mockAmqpMessage;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockConnection = Mockito.mock(ConnectionImpl.class);
        _mockAmqpReceiver = Mockito.mock(AmqpReceiver.class);
        _mockSession = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSession.getDestinationHelper()).thenReturn(new DestinationHelper());
        _mockAmqpMessage = Mockito.mock(AmqpGenericMessage.class);
    }

    @Test
    public void testNoMessageReceivedWhenConnectionNotStarted() throws Exception
    {
        Mockito.when(_mockConnection.isStarted()).thenReturn(false);
        Mockito.when(_mockAmqpReceiver.receiveNoWait()).thenReturn(_mockAmqpMessage);

        ImmediateWaitUntil.mockWaitUntil(_mockConnection);

        ReceiverImpl receiver = new ReceiverImpl(_mockConnection, _mockSession, _mockAmqpReceiver, null);

        assertNull("Should not receive a message when connection is not started", receiver.receive(1));
    }

    @Test
    public void testMessageReceivedWhenConnectionIsStarted() throws Exception
    {
        Mockito.when(_mockConnection.isStarted()).thenReturn(true);
        Mockito.when(_mockAmqpReceiver.receiveNoWait()).thenReturn(_mockAmqpMessage);
        Mockito.when(_mockSession.getConnectionImpl()).thenReturn(_mockConnection);
        Mockito.when(_mockSession.getAcknowledgeMode()).thenReturn(Session.AUTO_ACKNOWLEDGE);

        ImmediateWaitUntil.mockWaitUntil(_mockConnection);

        ReceiverImpl receiver = new ReceiverImpl(_mockConnection, _mockSession, _mockAmqpReceiver, null);

        Message message = receiver.receive(1);
        assertNotNull("Should receive a message when connection is started", message);
        assertTrue("Unexpected message implementation", message instanceof MessageImpl<?>);
        assertEquals("Underlying AmqpMessage should be the one provided", _mockAmqpMessage, ((MessageImpl<?>)message).getUnderlyingAmqpMessage(false));
    }
}
