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
import javax.jms.MapMessage;

import org.apache.qpid.jms.engine.AmqpMapMessage;

public class MapMessageImpl extends MessageImpl<AmqpMapMessage> implements MapMessage
{
    //message to be sent
    public MapMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpMapMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public MapMessageImpl(AmqpMapMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected AmqpMapMessage prepareUnderlyingAmqpMessageForSending(AmqpMapMessage amqpMessage)
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

     //======= JMS Methods =======

    @Override
    public boolean getBoolean(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public byte getByte(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public short getShort(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public char getChar(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getInt(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getLong(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public float getFloat(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public double getDouble(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getString(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public byte[] getBytes(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Object getObject(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getMapNames() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setByte(String name, byte value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setShort(String name, short value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setChar(String name, char value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setInt(String name, int value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setLong(String name, long value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setFloat(String name, float value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setDouble(String name, double value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setString(String name, String value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length)
            throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setObject(String name, Object value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean itemExists(String name) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }
}
