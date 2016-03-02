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

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.MapMessage;

import org.apache.qpid.jms.message.JmsMapMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestMapMessageFacade;

/**
 * Foreign JMS MapMessage class
 */
public class ForeignJmsMapMessage extends ForeignJmsMessage implements MapMessage {

    private final JmsMapMessage message;

    public ForeignJmsMapMessage() {
        super(new JmsMapMessage(new JmsTestMapMessageFacade()));
        this.message = (JmsMapMessage) super.message;
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return message.getBoolean(name);
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return message.getByte(name);
    }

    @Override
    public short getShort(String name) throws JMSException {
        return message.getShort(name);
    }

    @Override
    public char getChar(String name) throws JMSException {
        return message.getChar(name);
    }

    @Override
    public int getInt(String name) throws JMSException {
        return message.getInt(name);
    }

    @Override
    public long getLong(String name) throws JMSException {
        return message.getLong(name);
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return message.getFloat(name);
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return message.getDouble(name);
    }

    @Override
    public String getString(String name) throws JMSException {
        return message.getString(name);
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return message.getBytes(name);
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return message.getObject(name);
    }

    @Override
    public Enumeration<String> getMapNames() throws JMSException {
        return message.getMapNames();
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        message.setBoolean(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        message.setByteProperty(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        message.setShort(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        message.setChar(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        message.setInt(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        message.setLong(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        message.setFloat(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        message.setDouble(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        message.setString(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        message.setBytes(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        message.setBytes(name, value, offset, length);
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        message.setObject(name, value);
    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return message.itemExists(name);
    }
}
