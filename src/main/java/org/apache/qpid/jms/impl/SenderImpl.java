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

import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_TTL;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSentMessageToken;

public class SenderImpl extends LinkImpl implements MessageProducer
{
    private static final long UINT_MAX = 0xFFFFFFFFL;

    private final boolean _setJMSXUserId = Boolean.valueOf(System.getProperty(ClientProperties.QPID_SET_JMSXUSERID_ON_SEND, "true"));
    private AmqpSender _amqpSender;
    private Destination _destination;

    public SenderImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl, AmqpSender amqpSender, Destination destination)
    {
        super(connectionImpl, amqpSender);
        _amqpSender = amqpSender;
        _destination = destination;
    }

    private void sendMessage(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        getConnectionImpl().lock();
        try
        {
            long timestamp = System.currentTimeMillis();

            //TODO: Respect the disableJMSMessageId hint
            //TODO: Bypass adding the ID: prefix for our own messages, since we remove it down the stack.
            //set the message id
            UUID uuid = UUID.randomUUID();
            String messageID = MessageIdHelper.JMS_ID_PREFIX + uuid.toString();
            message.setJMSMessageID(messageID);

            //set the priority
            message.setJMSPriority(priority);

            //set the timestamp
            message.setJMSTimestamp(timestamp);

            //set the Destination
            message.setJMSDestination(_destination);

            //set the DeliveryMode if necessary
            if(deliveryMode != message.getJMSDeliveryMode())
            {
                message.setJMSDeliveryMode(deliveryMode);
            }

            //set the JMSExpiration if necessary
            if(timeToLive != Message.DEFAULT_TIME_TO_LIVE)
            {
                message.setJMSExpiration(timestamp + timeToLive);
            }
            else if(message.getJMSExpiration() != 0)
            {
                message.setJMSExpiration(0);
            }

            AmqpMessage amqpMessage = getAmqpMessageFromJmsMessage(message);

            //set the JMSXUserId value
            String existingUserValue = message.getStringProperty(ClientProperties.JMSXUSERID);
            String newUserString = null;
            if(_setJMSXUserId)
            {
                newUserString = getConnectionImpl().getUserName();
            }

            if(userStringValuesDiffer(newUserString, existingUserValue))
            {
                if(isQpidMessage(message))
                {
                    //set the UserId field on the underlying AMQP message
                    byte[] bytes = null;
                    if(newUserString != null)
                    {
                        try
                        {
                            bytes = newUserString.getBytes("UTF-8");
                        }
                        catch (UnsupportedEncodingException e)
                        {
                            throw new QpidJmsException("Unable to encode user id", e);
                        }
                    }
                    amqpMessage.setUserId(bytes);
                }
                else
                {
                    message.setStringProperty(ClientProperties.JMSXUSERID, newUserString);
                }
            }

            //set the AMQP header ttl field if necessary
            if(message.propertyExists(JMS_AMQP_TTL))
            {
                //Use the requested value from the property. 0 means clear TTL header.
                long propTtl = message.getLongProperty(JMS_AMQP_TTL);
                if(propTtl != 0)
                {
                    amqpMessage.setTtl(propTtl);
                }
                else if(amqpMessage.getTtl() != null)
                {
                    amqpMessage.setTtl(null);
                }
            }
            else
            {
                //Set the AMQP header TTL to any non-default value implied by JMS TTL (if possible),
                //otherwise ensure TTL header is clear.
                if(timeToLive != Message.DEFAULT_TIME_TO_LIVE && timeToLive < UINT_MAX)
                {
                    amqpMessage.setTtl(timeToLive);
                }
                else if(amqpMessage.getTtl() != null)
                {
                    amqpMessage.setTtl(null);
                }
            }

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

    private boolean userStringValuesDiffer(String user, String existing)
    {
        return user != null && !user.equals(existing) || (user == null && existing != null);
    }

    private AmqpMessage getAmqpMessageFromJmsMessage(Message message)
    {
        if(isQpidMessage(message))
        {
            return ((MessageImpl<?>)message).getUnderlyingAmqpMessage(true);
        }
        else
        {
            //TODO
            throw new UnsupportedOperationException("cross-vendor message support has yet to be implemented");
        }
    }

    private boolean isQpidMessage(Message message)
    {
        return message instanceof MessageImpl;
    }


    //======= JMS Methods =======


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
    public void send(Message message) throws JMSException
    {
        sendMessage(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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
        sendMessage(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }
}
