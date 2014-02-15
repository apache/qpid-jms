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

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.TestAmqpMessage;

public class TestMessageImpl extends MessageImpl<TestAmqpMessage>
{
    //message to be sent
    private TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        super(amqpMessage, sessionImpl, connectionImpl);
    }

    //message just received
    private TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected TestAmqpMessage prepareUnderlyingAmqpMessageForSending(TestAmqpMessage amqpMessage)
    {
        //NO-OP
        return amqpMessage;
    }


    public static MessageImpl<TestAmqpMessage> createNewMessage(SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        TestAmqpMessage testAmqpMessage = (TestAmqpMessage) TestAmqpMessage.createNewMessage();

        return new TestMessageImpl(testAmqpMessage, sessionImpl, connectionImpl);
    }

    public static MessageImpl<TestAmqpMessage> createNewMessage(AmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        if(!(amqpMessage instanceof TestAmqpMessage))
        {
            throw new IllegalArgumentException("AmqpMessage must be an instance of " + TestAmqpMessage.class.getName());
        }

        return new TestMessageImpl((TestAmqpMessage) amqpMessage, sessionImpl, connectionImpl);
    }

    public static MessageImpl<TestAmqpMessage> createReceivedMessage(AmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        if(!(amqpMessage instanceof TestAmqpMessage))
        {
            throw new IllegalArgumentException("AmqpMessage must be an instance of " + TestAmqpMessage.class.getName());
        }

        return new TestMessageImpl((TestAmqpMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    public void clearBody() throws JMSException
    {
        throw new UnsupportedOperationException("Not Supported on " + TestMessageImpl.class.getName());
    }
}