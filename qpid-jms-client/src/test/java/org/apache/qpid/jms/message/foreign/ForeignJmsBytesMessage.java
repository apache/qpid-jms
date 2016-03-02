/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.message.foreign;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestBytesMessageFacade;

/**
 * Foreign JMS BytesMessage type.
 */
public class ForeignJmsBytesMessage extends ForeignJmsMessage implements BytesMessage {

    private final JmsBytesMessage message;

    public ForeignJmsBytesMessage() {
        super(new JmsBytesMessage(new JmsTestBytesMessageFacade()));
        this.message = (JmsBytesMessage) super.message;
    }

    @Override
    public long getBodyLength() throws JMSException {
        return message.getBodyLength();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return message.readBoolean();
    }

    @Override
    public byte readByte() throws JMSException {
        return message.readByte();
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        return message.readUnsignedByte();
    }

    @Override
    public short readShort() throws JMSException {
        return message.readShort();
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        return message.readUnsignedShort();
    }

    @Override
    public char readChar() throws JMSException {
        return message.readChar();
    }

    @Override
    public int readInt() throws JMSException {
        return message.readInt();
    }

    @Override
    public long readLong() throws JMSException {
        return message.readLong();
    }

    @Override
    public float readFloat() throws JMSException {
        return message.readFloat();
    }

    @Override
    public double readDouble() throws JMSException {
        return message.readDouble();
    }

    @Override
    public String readUTF() throws JMSException {
        return message.readUTF();
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return message.readBytes(value);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        return message.readBytes(value, length);
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        message.writeBoolean(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        message.writeByte(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        message.writeShort(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        message.writeChar(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        message.writeInt(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        message.writeLong(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        message.writeFloat(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        message.writeDouble(value);
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        message.writeUTF(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        message.writeBytes(value);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        message.writeBytes(value, offset, length);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        message.writeObject(value);
    }

    @Override
    public void reset() throws JMSException {
        message.reset();
    }
}
