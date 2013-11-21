/*
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
 */
package org.apache.qpid.jms.impl;

import java.util.Collections;
import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.engine.AmqpMessage;

public abstract class MessageImpl<T extends AmqpMessage> implements Message
{
    private final T _amqpMessage;

    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        _amqpMessage = amqpMessage;
    }

    T getUnderlyingAmqpMessage(boolean prepareForSending)
    {
        if(prepareForSending)
        {
            return prepareUnderlyingAmqpMessageForSending(_amqpMessage);
        }
        else
        {
            return _amqpMessage;
        }
    }

    protected abstract T prepareUnderlyingAmqpMessageForSending(T amqpMessage);

    private void checkPropertyNameIsValid(String propertyName) throws IllegalArgumentException
    {
        if (propertyName == null)
        {
            throw new IllegalArgumentException("Property name must not be null");
        }
        else if (propertyName.length() == 0)
        {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        //TODO: validate name format?
        //checkPropertyNameFormat(propertyName);
    }

    private boolean checkObjectPropertyValueIsValid(Object object) throws MessageFormatException
    {
        boolean valid = object instanceof Boolean || object instanceof Byte || object instanceof Short ||
                        object instanceof Integer || object instanceof Long || object instanceof Float ||
                        object instanceof Double || object instanceof String|| object == null;
        if(!valid)
        {
            throw new MessageFormatException("Invalid object property value type: " + object.getClass());
        }

        return true;
    }

    //======= JMS Methods =======

    @Override
    public String getJMSMessageID() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getJMSTimestamp() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getJMSCorrelationID() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Destination getJMSDestination() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getJMSType() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSType(String type) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getJMSExpiration() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getJMSPriority() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void clearProperties() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean propertyExists(String name) throws JMSException
    {
        return _amqpMessage.applicationPropertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public byte getByteProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public short getShortProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getIntProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getLongProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public float getFloatProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getStringProperty(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException
    {
        checkPropertyNameIsValid(name);

        //TODO: type conversion if any?
        //TODO: handle non-JMS types?
        return _amqpMessage.getApplicationProperty(name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException
    {
        return Collections.enumeration(_amqpMessage.getApplicationPropertyNames());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException
    {
        checkPropertyNameIsValid(name);
        checkObjectPropertyValueIsValid(value);

        _amqpMessage.setApplicationProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void clearBody() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }
}
