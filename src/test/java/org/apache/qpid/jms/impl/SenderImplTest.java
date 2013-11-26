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

import javax.jms.DeliveryMode;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSentMessageToken;
import org.apache.qpid.jms.engine.TestAmqpMessage;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SenderImplTest extends QpidJmsTestCase
{
    private ConnectionImpl _mockConnection;
    private AmqpSender _mockAmqpSender;
    private SessionImpl _mockSession;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockConnection = Mockito.mock(ConnectionImpl.class);
        _mockAmqpSender = Mockito.mock(AmqpSender.class);
        _mockSession = Mockito.mock(SessionImpl.class);
    }

    @Test
    public void testSenderOverriddesMessageDeliveryMode() throws Exception
    {
        //Create mock sent message token, ensure that it is immediately marked as Accepted
        AmqpSentMessageToken _mockToken = Mockito.mock(AmqpSentMessageToken.class);
        Mockito.when(_mockToken.getRemoteDeliveryState()).thenReturn(Accepted.getInstance());
        Mockito.when(_mockAmqpSender.sendMessage(Mockito.any(AmqpMessage.class))).thenReturn(_mockToken);
        ImmediateWaitUntil.mockWaitUntil(_mockConnection);

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender);

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();
        TestMessageImpl testMessage = new TestMessageImpl(testAmqpMessage, null, null)
        {
            @Override
            protected TestAmqpMessage prepareUnderlyingAmqpMessageForSending(TestAmqpMessage amqpMessage)
            {
                //NO-OP
                return amqpMessage;
            }
        };

        testMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, testMessage.getJMSDeliveryMode());

        senderImpl.send(testMessage);

        assertEquals(DeliveryMode.PERSISTENT, testMessage.getJMSDeliveryMode());
    }
}
