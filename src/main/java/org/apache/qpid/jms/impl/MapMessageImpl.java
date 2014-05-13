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
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.engine.AmqpMapMessage;

/**
 * TODO
 *
 * NOTES:
 * Implement handling for byte[] in the map (e.g convert to/from Binary);
 *
 */
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

    private void setMapEntry(String name, Object value) throws IllegalArgumentException, MessageNotWriteableException
    {
        validateMapKeyName(name);
        checkBodyWritable();

        getUnderlyingAmqpMessage(false).setMapEntry(name, value);
    }

    private Object getMapEntry(String name) throws IllegalArgumentException
    {
        validateMapKeyName(name);

        return getUnderlyingAmqpMessage(false).getMapEntry(name);
    }

    private void validateMapKeyName(String name) throws IllegalArgumentException
    {
        if (name == null)
        {
            throw new IllegalArgumentException("Map key name must not be null");
        }
        else if (name.length() == 0)
        {
            throw new IllegalArgumentException("Map key name must not be the empty string");
        }
    }

    private boolean checkObjectMapValueIsValid(Object object) throws MessageFormatException
    {
        boolean valid = object instanceof Boolean || object instanceof Byte || object instanceof Short ||
                        object instanceof Integer || object instanceof Long || object instanceof Float ||
                        object instanceof Double || object instanceof String|| object instanceof Character ||
                        object instanceof byte[] || object == null;
        if(!valid)
        {
            throw new QpidJmsMessageFormatException("Invalid object value type: " + object.getClass());
        }

        return true;
    }

     //======= JMS Methods =======

    @Override
    public boolean getBoolean(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Boolean)
        {
            return ((Boolean) value).booleanValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Boolean.valueOf((String) value);
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to boolean.");
        }
    }

    @Override
    public byte getByte(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Byte)
        {
            return ((Byte) value).byteValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Byte.valueOf((String) value).byteValue();
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to byte.");
        }
    }

    @Override
    public short getShort(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Short)
        {
            return ((Short) value).shortValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).shortValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Short.valueOf((String) value).shortValue();
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to short.");
        }
    }

    @Override
    public char getChar(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if ((value instanceof Character)) //TODO: verify we dont get a Binary, push down impl to ensure we dont
        {
            return (char) value;
        }
        else if(value == null)
        {
            throw new NullPointerException("Map entry " + name + " with null value cannot be converted to char.");
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to char.");
        }
    }

    @Override
    public int getInt(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Integer)
        {
            return ((Integer) value).intValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).intValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).intValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Integer.valueOf((String) value).intValue();
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to int.");
        }
    }

    @Override
    public long getLong(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Long)
        {
            return ((Long) value).longValue();
        }
        else if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).longValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).longValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Long.valueOf((String) value).longValue();
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to long.");
        }
    }

    @Override
    public float getFloat(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Float)
        {
            return ((Float) value).floatValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Float.valueOf((String) value).floatValue();
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to float.");
        }
    }

    @Override
    public double getDouble(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if (value instanceof Double)
        {
            return ((Double) value).doubleValue();
        }
        else if (value instanceof Float)
        {
            return ((Float) value).doubleValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Double.valueOf((String) value).doubleValue();
        }
        else
        {
            throw new MessageFormatException("Map entry  " + name + " of type " + value.getClass().getName() + " cannot be converted to double.");
        }
    }

    @Override
    public String getString(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if ((value instanceof String) || (value == null))
        {
            return (String) value;
        }
        else if (value instanceof byte[])//TODO: verify we dont get a Binary, push down impl to ensure we dont
        {
            throw new MessageFormatException("Map entry " + name + " of type byte[] " + "cannot be converted to String.");
        }
        else
        {
            return value.toString();
        }
    }

    @Override
    public byte[] getBytes(String name) throws JMSException
    {
        Object value = getMapEntry(name);

        if ((value instanceof byte[]) || (value == null)) //TODO: verify we dont get a Binary, push down impl to ensure we dont
        {
            return (byte[]) value;
        }
        else
        {
            throw new MessageFormatException("Map entry " + name + " of type " + value.getClass().getName() + " cannot be converted to byte[].");
        }
    }

    @Override
    public Object getObject(String name) throws JMSException
    {
        //TODO: verify what happens with byte[] for received messages
        //(i.e does it return a Binary? if so, push impl down to ensure we get a byte[] instead)
        return getMapEntry(name);
    }

    @Override
    public Enumeration<?> getMapNames() throws JMSException
    {
        Set<String> names = getUnderlyingAmqpMessage(false).getMapKeys();

        return Collections.enumeration(names);
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException
    {
        setMapEntry(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException
    {
        setMapEntry(name, value);
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
        checkObjectMapValueIsValid(value);
        setMapEntry(name, value);
    }

    @Override
    public boolean itemExists(String name) throws JMSException
    {
        return getUnderlyingAmqpMessage(false).mapEntryExists(name);
    }

    @Override
    public void clearBody() throws JMSException
    {
        getUnderlyingAmqpMessage(false).clearMapEntries();
        setBodyWritable(true);
    }
}
