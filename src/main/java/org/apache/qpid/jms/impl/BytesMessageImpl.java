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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.engine.AmqpBytesMessage;

public class BytesMessageImpl extends MessageImpl<AmqpBytesMessage> implements BytesMessage
{
    private ByteArrayOutputStream _bytesOut;
    private DataOutputStream _dataAsOutput;
    private DataInputStream _dataIn;
    private ByteArrayInputStream _bytesIn;

    //message to be sent
    public BytesMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        this(new AmqpBytesMessage(), sessionImpl, connectionImpl);
        _bytesOut = new ByteArrayOutputStream();
        _dataAsOutput = new DataOutputStream(_bytesOut);
    }

    //message just received
    public BytesMessageImpl(AmqpBytesMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl);
        _dataIn = new DataInputStream(amqpMessage.getByteArrayInputStream());
    }

    @Override
    protected AmqpBytesMessage prepareUnderlyingAmqpMessageForSending(AmqpBytesMessage amqpMessage)
    {
        amqpMessage.setBytes(_bytesOut.toByteArray());

        //TODO: do we need to do anything later with properties/headers etc?
        return amqpMessage;
    }

    private JMSException handleInputException(final IOException e)
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

    private JMSException handleOutputException(final IOException e)
    {
        return new QpidJmsException(e.getMessage(), e);
    }

    //======= JMS Methods =======

    @Override
    public long getBodyLength() throws JMSException
    {
        return getUnderlyingAmqpMessage(false).getBytesLength();
    }

    @Override
    public boolean readBoolean() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public byte readByte() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int readUnsignedByte() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public short readShort() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int readUnsignedShort() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public char readChar() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int readInt() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long readLong() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public float readFloat() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public double readDouble() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String readUTF() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int readBytes(byte[] value) throws JMSException
    {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException
    {
        //TODO: checkReadable();

        try
        {
            int offset = 0;
            while(offset < length)
            {
                int read = _dataIn.read(value, offset, length - offset);
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
            throw handleInputException(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeByte(byte value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeShort(short value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeChar(char value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeInt(int value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeLong(long value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeFloat(float value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeDouble(double value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeUTF(String value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeBytes(byte[] bytes) throws JMSException
    {
        //TODO: checkWritable();
        try
        {
            _dataAsOutput.write(bytes);
        }
        catch (IOException e)
        {
            throw handleOutputException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void writeObject(Object value) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void reset() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }
}
