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

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.engine.AmqpMessage;

public abstract class MessageImpl implements Message
{
    private final AmqpMessage _amqpMessage;

    protected MessageImpl()
    {
        _amqpMessage = new AmqpMessage();
    }

    AmqpMessage getAmqpMessage()
    {
        return _amqpMessage;
    }

    @Override
    public String getJMSMessageID() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public long getJMSTimestamp() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public String getJMSCorrelationID() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public Destination getJMSDestination() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public String getJMSType() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSType(String type) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public long getJMSExpiration() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public int getJMSPriority() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void clearProperties() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public boolean propertyExists(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public byte getByteProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public short getShortProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public int getIntProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public long getLongProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public float getFloatProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public String getStringProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void acknowledge() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void clearBody() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }


}
