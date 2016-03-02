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

import javax.jms.JMSException;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestStreamMessageFacade;

/**
 * Foreign JMS StreamMessage class
 */
public class ForeignJmsStreamMessage extends ForeignJmsMessage implements StreamMessage {

    private final JmsStreamMessage message;

    public ForeignJmsStreamMessage() {
        super(new JmsStreamMessage(new JmsTestStreamMessageFacade()));
        this.message = (JmsStreamMessage) super.message;
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
    public short readShort() throws JMSException {
        return message.readShort();
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
    public String readString() throws JMSException {
        return message.readString();
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return message.readBytes(value);
    }

    @Override
    public Object readObject() throws JMSException {
        return message.readObject();
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
    public void writeString(String value) throws JMSException {
        message.writeString(value);
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
