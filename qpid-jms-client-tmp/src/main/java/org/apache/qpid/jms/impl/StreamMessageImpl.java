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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotReadableException;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.engine.AmqpListMessage;

public class StreamMessageImpl extends MessageImpl<AmqpListMessage> implements StreamMessage
{
    private static final Set<Class<?>> SUPPORTED_TYPES =  new HashSet<Class<?>>(Arrays.asList(
            Boolean.class, Byte.class, Short.class, Character.class, Integer.class, Long.class, Float.class, Double.class, String.class, byte[].class));

    private static final int NO_BYTES_IN_FLIGHT = -1;
    private int _remainingBytes = NO_BYTES_IN_FLIGHT;

    //message to be sent
    public StreamMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpListMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public StreamMessageImpl(AmqpListMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected AmqpListMessage prepareUnderlyingAmqpMessageForSending(AmqpListMessage amqpMessage)
    {
        //Currently nothing to do, we always operate directly on the underlying AmqpListMessage.
        return amqpMessage;
    }

    private void checkObjectType(Object value) throws QpidJmsMessageFormatException
    {
        if(value != null && !SUPPORTED_TYPES.contains(value.getClass()))
        {
            throw new QpidJmsMessageFormatException("Invalid object value type: " + value.getClass());
        }
    }

    void checkBodyReadable() throws MessageNotReadableException
    {
        if(isBodyWritable())
        {
            throw new MessageNotReadableException("Message body is currently in write-only mode");
        }
    }

    private Object readObjectInternal(boolean checkExistingReadBytesUsage) throws MessageEOFException, QpidJmsMessageFormatException, MessageNotReadableException
    {
        checkBodyReadable();

        if(checkExistingReadBytesUsage)
        {
            if(_remainingBytes != NO_BYTES_IN_FLIGHT)
            {
                throw new QpidJmsMessageFormatException("Partially read byte[] entry still being retrieved using readBytes(byte[] dest)");
            }
        }

        try
        {
            return getUnderlyingAmqpMessage(false).get();
        }
        catch(IndexOutOfBoundsException ioobe)
        {
            throw new MessageEOFException("No more data in message stream");
        }
    }

    private void decrementStreamPosition()
    {
        getUnderlyingAmqpMessage(false).decrementPosition();
    }

    //======= JMS Methods =======

    @Override
    public boolean readBoolean() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Boolean)
        {
            return (Boolean) o;
        }
        else if(o instanceof String || o == null)
        {
            return Boolean.valueOf((String)o);
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to boolean.");
        }
    }

    @Override
    public byte readByte() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Byte)
        {
            return (Byte) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Byte.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to byte.");
        }
    }

    @Override
    public short readShort() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Short)
        {
            return (Short) o;
        }
        else if(o instanceof Byte)
        {
            return (Byte) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Short.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to short.");
        }
    }

    @Override
    public char readChar() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Character)
        {
            return (Character) o;
        }
        else if(o == null)
        {
            decrementStreamPosition();
            throw new NullPointerException("Stream entry with null value cannot be converted to char.");
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to char.");
        }
    }

    @Override
    public int readInt() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Integer)
        {
            return (Integer) o;
        }
        else if(o instanceof Short)
        {
            return (Short) o;
        }
        else if(o instanceof Byte)
        {
            return (Byte) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Integer.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to int.");
        }
    }

    @Override
    public long readLong() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Long)
        {
            return (Long) o;
        }
        else if(o instanceof Integer)
        {
            return (Integer) o;
        }
        else if(o instanceof Short)
        {
            return (Short) o;
        }
        else if(o instanceof Byte)
        {
            return (Byte) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Long.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to long.");
        }
    }

    @Override
    public float readFloat() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Float)
        {
            return (Float) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Float.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to float.");
        }
    }

    @Override
    public double readDouble() throws JMSException
    {
        Object o = readObject();
        if(o instanceof Float)
        {
            return (Float) o;
        }
        else if(o instanceof Double)
        {
            return (Double) o;
        }
        else if(o instanceof String || o == null)
        {
            try
            {
                return Double.valueOf((String)o);
            }
            catch(RuntimeException e)
            {
                decrementStreamPosition();
                throw e;
            }
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to double.");
        }
    }

    @Override
    public String readString() throws JMSException
    {
        Object o = readObject();
        if(o instanceof String || o == null)
        {
            return (String) o;
        }
        else if(o instanceof byte[])
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to String.");
        }
        else
        {
            return String.valueOf(o);
        }
    }

    @Override
    public int readBytes(byte[] dest) throws JMSException
    {
        //TODO
        Object o = readObjectInternal(false);

        if(o == null)
        {
            return -1;
        }

        if(o instanceof byte[])
        {
            byte[] src = (byte[]) o;

            if(src.length == 0)
            {
                return 0;
            }

            if(_remainingBytes == 0)
            {
                //We previously read all the bytes, but must have filled the dest array.
                //Clear the remaining marker and signal completion via return value.
                _remainingBytes = NO_BYTES_IN_FLIGHT;
                return -1;
            }

            if(_remainingBytes == NO_BYTES_IN_FLIGHT)
            {
                //The field is non-null and non-empty, and this is the first read attempt.
                //Set the remaining marker to the full size
                _remainingBytes = src.length;
            }

            int previouslyRead = src.length - _remainingBytes;
            int lengthToCopy = Math.min(dest.length, _remainingBytes);

            if(lengthToCopy > 0)
            {
                System.arraycopy(src, previouslyRead, dest, 0, lengthToCopy);
            }

            _remainingBytes -= lengthToCopy;

            if(_remainingBytes == 0 && lengthToCopy < dest.length)
            {
                //All bytes have been read and dest array was not filled on this call, so the return
                //will enable the caller to determine completion. Clear the remaining marker.
                _remainingBytes = NO_BYTES_IN_FLIGHT;
            }
            else
            {
                decrementStreamPosition();
            }

            return lengthToCopy;
        }
        else
        {
            decrementStreamPosition();
            throw new QpidJmsMessageFormatException("Stream entry of type " + o.getClass().getName() + " cannot be converted to bytes.");
        }
    }

    @Override
    public Object readObject() throws JMSException
    {
        return readObjectInternal(true);
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeShort(short value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeChar(char value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeInt(int value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeLong(long value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeString(String value) throws JMSException
    {
        writeObject(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException
    {
        writeBytes(value, 0, value.length);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException
    {
        checkBodyWritable();

        byte[] dest = new byte[length];
        System.arraycopy(value, offset, dest, 0, length);

        getUnderlyingAmqpMessage(false).add(dest);
    }

    @Override
    public void writeObject(Object value) throws JMSException
    {
        if(value instanceof byte[])
        {
            writeBytes((byte[]) value);
            return;
        }

        checkBodyWritable();
        checkObjectType(value);

        getUnderlyingAmqpMessage(false).add(value);
    }

    @Override
    public void reset() throws JMSException
    {
        getUnderlyingAmqpMessage(false).resetPosition();
        setBodyWritable(false);
        _remainingBytes = -1;
    }

    @Override
    public void clearBody() throws JMSException
    {
        getUnderlyingAmqpMessage(false).clear();
        setBodyWritable(true);
        _remainingBytes = -1;
    }
}
