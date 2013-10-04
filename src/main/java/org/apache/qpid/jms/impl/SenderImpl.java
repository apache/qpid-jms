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
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSentMessageToken;

public class SenderImpl extends LinkImpl implements MessageProducer
{
    private AmqpSender _amqpSender;

    public SenderImpl(SessionImpl sessionImpl, AmqpSender amqpSender)
    {
        super(sessionImpl.getConnectionImpl(), amqpSender);
        _amqpSender = amqpSender;
    }

    @Override
    public void send(Message message) throws JMSException
    {
        getConnectionImpl().lock();
        try
        {
            AmqpMessage amqpMessage = getAmqpMessageFromJmsMessage(message);

            AmqpSentMessageToken sentMessage = _amqpSender.sendMessage(amqpMessage);

            getConnectionImpl().stateChanged();

            SentMessageTokenImpl sentMessageImpl = new SentMessageTokenImpl(sentMessage, this);
            sentMessageImpl.waitUntilAccepted();
            sentMessage.settle();
        }
        finally
        {
            getConnectionImpl().releaseLock();
        }

    }

    private AmqpMessage getAmqpMessageFromJmsMessage(Message message)
    {
        if(message instanceof MessageImpl)
        {
            return ((MessageImpl)message).getAmqpMessage();
        }
        else
        {
            throw new UnsupportedOperationException("cross-vendor message support has yet to be implemented");
        }
    }

    @Override
    public void setDisableMessageID(boolean value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean getDisableMessageID() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getDeliveryMode() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setPriority(int defaultPriority) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getPriority() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getTimeToLive() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Destination getDestination() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }
}
