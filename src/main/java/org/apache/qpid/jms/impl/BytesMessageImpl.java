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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;

import org.apache.qpid.jms.engine.AmqpBytesMessage;

public class BytesMessageImpl extends MessageImpl<AmqpBytesMessage> implements BytesMessage
{
    private OutputStreamHelper _outputStreamHelper = new OutputStreamHelper();
    private InputStreamHelper _inputStreamHelper = new InputStreamHelper();

    //message to be sent
    public BytesMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpBytesMessage(), sessionImpl, connectionImpl);
        _outputStreamHelper.createOutputStreams();
    }

    //message just received
    public BytesMessageImpl(AmqpBytesMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        createDataInputStreamFromUnderlyingMessage();
    }

    private void createDataInputStreamFromUnderlyingMessage()
    {
        AmqpBytesMessage amqpBytesMessage = getUnderlyingAmqpMessage(false);
        ByteArrayInputStream byteArrayInputStream = amqpBytesMessage.getByteArrayInputStream();

        _inputStreamHelper.createNewInputStream(byteArrayInputStream);
    }

    @Override
    protected AmqpBytesMessage prepareUnderlyingAmqpMessageForSending(AmqpBytesMessage amqpMessage)
    {
        if(_outputStreamHelper.hasOutputStreams())
        {
            byte[] data = _outputStreamHelper.getByteOutput();
            amqpMessage.setBytes(data);
        }
        else
        {
            //We are sending what we received or set on it earlier.
            //Ensure the body is a Data section.
            amqpMessage.convertBodyToDataSectionIfNecessary();
        }

        //ensure content type is set
        amqpMessage.setContentType(AmqpBytesMessage.CONTENT_TYPE);

        return amqpMessage;
    }

    private JMSException createInputException(final IOException e)
    {
        JMSException ex;
        if(e instanceof EOFException)
        {
            ex = new MessageEOFException(e.getMessage());
        }
        else
        {
            ex = new MessageFormatException(e.getMessage());
        }
        ex.initCause(e);
        ex.setLinkedException(e);
        return ex;
    }

    private JMSException createOutputException(final IOException e)
    {
        return new QpidJmsException(e.getMessage(), e);
    }

    void checkBodyReadable() throws MessageNotReadableException
    {
        if(isBodyWritable())
        {
            throw new MessageNotReadableException("Message body is currently in write-only mode");
        }
    }

    //======= JMS Methods =======

    @Override
    public long getBodyLength() throws JMSException
    {
        checkBodyReadable();

        return getUnderlyingAmqpMessage(false).getBytesLength();
    }

    @Override
    public boolean readBoolean() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readBoolean();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public byte readByte() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readByte();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readUnsignedByte();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public short readShort() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readShort();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readUnsignedShort();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public char readChar() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readChar();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public int readInt() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readInt();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public long readLong() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readLong();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public float readFloat() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readFloat();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public double readDouble() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readDouble();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public String readUTF() throws JMSException
    {
        checkBodyReadable();

        try
        {
            return _inputStreamHelper.readUTF();
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException
    {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException
    {
        checkBodyReadable();

        if(length < 0 || value.length < length)
        {
            throw new IndexOutOfBoundsException("length must not be negative or larger than the size of the provided array");
        }

        try
        {
            int offset = 0;
            while(offset < length)
            {
                int read = _inputStreamHelper.read(value, offset, length - offset);
                if(read < 0)
                {
                    break;
                }
                offset += read;
            }

            if(offset == 0 && length != 0)
            {
                return -1;
            }
            else
            {
                return offset;
            }
        }
        catch (IOException e)
        {
            throw createInputException(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeBoolean(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeByte(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeShort(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeChar(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeInt(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeLong(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeFloat(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeDouble(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException
    {
        checkBodyWritable();

        try
        {
            _outputStreamHelper.writeUTF(value);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeBytes(byte[] bytes) throws JMSException
    {
        writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException
    {
        checkBodyWritable();
        try
        {
            _outputStreamHelper.write(value, offset, length);
        }
        catch (IOException e)
        {
            throw createOutputException(e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException
    {
        checkBodyWritable();
        if(value == null)
        {
            throw new NullPointerException("Value passed to BytesMessage.writeObject() must not be null");
        }
        else if (value instanceof Boolean)
        {
            writeBoolean((Boolean)value);
        }
        else if (value instanceof Byte)
        {
            writeByte((Byte)value);
        }
        else if (value instanceof Short)
        {
            writeShort((Short)value);
        }
        else if (value instanceof Character)
        {
            writeChar((Character)value);
        }
        else if (value instanceof Integer)
        {
            writeInt((Integer)value);
        }
        else if(value instanceof Long)
        {
            writeLong((Long)value);
        }
        else if(value instanceof Float)
        {
            writeFloat((Float) value);
        }
        else if(value instanceof Double)
        {
            writeDouble((Double) value);
        }
        else if(value instanceof String)
        {
            writeUTF((String) value);
        }
        else if(value instanceof byte[])
        {
            writeBytes((byte[])value);
        }
        else
        {
            throw new MessageFormatException("Value passed to BytesMessage.writeObject() must be of primitive type.  Type passed was " + value.getClass().getName());
        }
    }

    @Override
    public void reset() throws JMSException
    {
        //If we have created an output stream previously, this is either
        //a new message or we cleared the body of a received message
        if(_outputStreamHelper.hasOutputStreams())
        {
            //update the underlying message and create new input stream based on the current output
            byte[] data = _outputStreamHelper.getByteOutput();
            getUnderlyingAmqpMessage(false).setBytes(data);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            _inputStreamHelper.createNewInputStream(bais);

            //clear the current output streams
            _outputStreamHelper.clearOutputStreams();
        }
        else
        {
            //This is a received message that has not
            //yet been cleared, recreate the input stream
            createDataInputStreamFromUnderlyingMessage();
        }

        setBodyWritable(false);
    }

    @Override
    public void clearBody() throws JMSException
    {
        //clear any prior input stream, and the underlying message body
        getUnderlyingAmqpMessage(false).setBytes(null);
        _inputStreamHelper.clearInputStream();

        //reset the output streams
        _outputStreamHelper.createOutputStreams();

        setBodyWritable(true);
    }
}
