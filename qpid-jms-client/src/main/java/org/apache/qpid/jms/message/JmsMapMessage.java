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
package org.apache.qpid.jms.message;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.message.facade.JmsMapMessageFacade;

/**
 * Implementation of the JMS MapMessage.
 */
@SuppressWarnings("unchecked")
public class JmsMapMessage extends JmsMessage implements MapMessage {

    JmsMapMessageFacade facade;

    public JmsMapMessage(JmsMapMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsMapMessage copy() throws JMSException {
        JmsMapMessage other = new JmsMapMessage(facade.copy());
        other.copy(this);
        return other;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        facade.clearBody();
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        } else if (value instanceof String || value == null) {
            return Boolean.valueOf((String) value).booleanValue();
        } else {
            throw new MessageFormatException("Cannot read a boolean from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public byte getByte(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Byte) {
            return ((Byte) value).byteValue();
        } else if (value instanceof String || value == null) {
            return Byte.valueOf((String) value).byteValue();
        } else {
            throw new MessageFormatException("Cannot read a byte from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public short getShort(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Short) {
            return ((Short) value).shortValue();
        } else if (value instanceof Byte) {
            return ((Byte) value).shortValue();
        } else if (value instanceof String || value == null) {
            return Short.valueOf((String) value).shortValue();
        } else {
            throw new MessageFormatException("Cannot read a short from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public char getChar(String name) throws JMSException {
        Object value = getObject(name);

        if (value == null) {
            throw new NullPointerException();
        } else if (value instanceof Character) {
            return ((Character) value).charValue();
        } else {
            throw new MessageFormatException("Cannot read a char from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public int getInt(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Integer) {
            return ((Integer) value).intValue();
        } else if (value instanceof Short) {
            return ((Short) value).intValue();
        } else if (value instanceof Byte) {
            return ((Byte) value).intValue();
        } else if (value instanceof String || value == null) {
            return Integer.valueOf((String) value).intValue();
        } else {
            throw new MessageFormatException("Cannot read an int from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public long getLong(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Long) {
            return ((Long) value).longValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Short) {
            return ((Short) value).longValue();
        } else if (value instanceof Byte) {
            return ((Byte) value).longValue();
        } else if (value instanceof String || value == null) {
            return Long.valueOf((String) value).longValue();
        } else {
            throw new MessageFormatException("Cannot read a long from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public float getFloat(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Float) {
            return ((Float) value).floatValue();
        } else if (value instanceof String || value == null) {
            return Float.valueOf((String) value).floatValue();
        } else {
            throw new MessageFormatException("Cannot read a float from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public double getDouble(String name) throws JMSException {
        Object value = getObject(name);

        if (value instanceof Double) {
            return ((Double) value).doubleValue();
        } else if (value instanceof Float) {
            return ((Float) value).floatValue();
        } else if (value instanceof String || value == null) {
            return Double.valueOf((String) value).doubleValue();
        } else {
            throw new MessageFormatException("Cannot read a double from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public String getString(String name) throws JMSException {
        Object value = getObject(name);

        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
            throw new MessageFormatException("Use getBytes to read a byte array");
        } else {
            return value.toString();
        }
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        Object value = getObject(name);

        if (value == null) {
            return null;
        } else if (value instanceof byte[]) {
            byte[] original = (byte[]) value;
            byte[] clone = new byte[original.length];
            System.arraycopy(original, 0, clone, 0, original.length);
            return clone;
        } else {
            throw new MessageFormatException("Cannot read a byte[] from " + value.getClass().getSimpleName());
        }
    }

    @Override
    public Object getObject(String name) throws JMSException {
        checkKeyNameIsValid(name);
        return facade.get(name);
    }

    @Override
    public Enumeration<String> getMapNames() throws JMSException {
        return facade.getMapNames();
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name, value ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name, Byte.valueOf(value));
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        put(name, Short.valueOf(value));
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        put(name, Character.valueOf(value));
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        put(name, Integer.valueOf(value));
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        put(name, Long.valueOf(value));
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        checkReadOnlyBody();
        put(name, Float.valueOf(value));
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        put(name, Double.valueOf(value));
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        setBytes(name, value, 0, (value != null ? value.length : 0));
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        // Fail early to avoid unnecessary array copy.
        checkReadOnlyBody();
        checkKeyNameIsValid(name);

        byte[] clone = null;
        if (value != null) {
            clone = new byte[length];
            System.arraycopy(value, offset, clone, 0, length);
        }

        put(name, clone);
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        checkValidObject(value);
        put(name, value);
    }

    /**
     * Indicates whether an item exists in this <CODE>MapMessage</CODE> object.
     *
     * @param name
     *        the name of the item to test
     * @return true if the item exists
     * @throws JMSException
     *         if the JMS provider fails to determine if the item exists due to
     *         some internal error.
     */
    @Override
    public boolean itemExists(String name) throws JMSException {
        return facade.itemExists(name);
    }

    @Override
    public String toString() {
        return "JmsMapMessage { " + facade + " }";
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return facade.hasBody() ? target.isAssignableFrom(Map.class) : true;
    }

    @Override
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        if (!facade.hasBody()) {
            return null;
        }

        Map<String, Object> copy = new HashMap<String, Object>();
        Enumeration<String> keys = facade.getMapNames();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            copy.put(key, getObject(key));
        }

        return (T) copy;
    }

    private void put(String name, Object value) throws JMSException {
        checkReadOnlyBody();
        checkKeyNameIsValid(name);
        facade.put(name, value);
    }

    private void checkKeyNameIsValid(String name) throws IllegalArgumentException {
        if (name == null) {
            throw new IllegalArgumentException("Map key name must not be null");
        } else if (name.length() == 0) {
            throw new IllegalArgumentException("Map key name must not be the empty string");
        }
    }

    private void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean ||
                        value instanceof Byte ||
                        value instanceof Short ||
                        value instanceof Integer ||
                        value instanceof Long ||
                        value instanceof Float ||
                        value instanceof Double ||
                        value instanceof Character ||
                        value instanceof String ||
                        value instanceof byte[] ||
                        value == null;

        if (!valid) {
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }
}
