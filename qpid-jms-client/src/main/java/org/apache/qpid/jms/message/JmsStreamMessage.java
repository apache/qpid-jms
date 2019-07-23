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

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.message.facade.JmsStreamMessageFacade;

/**
 * JMS Stream message implementation.
 */
public class JmsStreamMessage extends JmsMessage implements StreamMessage {

    private static final int NO_BYTES_IN_FLIGHT = -1;

    private final JmsStreamMessageFacade facade;

    private byte[] bytes;
    private int remainingBytes = NO_BYTES_IN_FLIGHT;

    public JmsStreamMessage(JmsStreamMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsStreamMessage copy() throws JMSException {
        JmsStreamMessage other = new JmsStreamMessage(facade.copy());
        other.copy(this);
        return other;
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        reset();
        super.onSend(producerTtl);
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        bytes = null;
        remainingBytes = -1;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Boolean result = null;
        Object value;
        value = facade.peek();

        if (value instanceof Boolean) {
            result = (Boolean) value;
        } else if (value instanceof String || value == null) {
            result = Boolean.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public byte readByte() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Byte result = null;
        Object value = facade.peek();

        if (value instanceof Byte) {
            result = (Byte) value;
        } else if (value instanceof String || value == null) {
            result = Byte.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public short readShort() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Short result = null;
        Object value = facade.peek();

        if (value instanceof Short) {
            result = (Short) value;
        } else if (value instanceof Byte) {
            result = ((Byte) value).shortValue();
        } else if (value instanceof String || value == null) {
            result = Short.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public char readChar() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Character result = null;
        Object value = facade.peek();

        if (value instanceof Character) {
            result = (Character) value;
        } else if (value == null) {
            throw new NullPointerException("Cannot convert NULL value to char.");
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public int readInt() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Integer result = null;
        Object value = facade.peek();

        if (value instanceof Integer) {
            result = (Integer) value;
        } else if (value instanceof Short) {
            result = ((Short) value).intValue();
        } else if (value instanceof Byte) {
            result = ((Byte) value).intValue();
        } else if (value instanceof String || value == null) {
            result = Integer.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public long readLong() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Long result = null;
        Object value = facade.peek();

        if (value instanceof Long) {
            result = (Long) value;
        } else if (value instanceof Integer) {
            result = ((Integer) value).longValue();
        } else if (value instanceof Short) {
            result = ((Short) value).longValue();
        } else if (value instanceof Byte) {
            result = ((Byte) value).longValue();
        } else if (value instanceof String || value == null) {
            result = Long.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public float readFloat() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Float result = null;
        Object value = facade.peek();

        if (value instanceof Float) {
            result = (Float) value;
        } else if (value instanceof String || value == null) {
            result = Float.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public double readDouble() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Double result = null;
        Object value = facade.peek();

        if (value instanceof Double) {
            result = (Double) value;
        } else if (value instanceof Float) {
            result = ((Float) value).doubleValue();
        } else if (value instanceof String || value == null) {
            result = Double.valueOf((String) value);
        } else {
            throw new MessageFormatException(
                "stream value: " + value.getClass().getSimpleName() + " cannot be converted to a boolean.");
        }

        facade.pop();
        return result;
    }

    @Override
    public String readString() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        String result = null;
        Object value = facade.peek();

        if (value == null) {
            result = null;
        } else if (value instanceof String) {
            result = (String) value;
        } else if (value instanceof Float) {
            result = value.toString();
        } else if (value instanceof Double) {
            result = value.toString();
        } else if (value instanceof Long) {
            result = value.toString();
        } else if (value instanceof Integer) {
            result = value.toString();
        } else if (value instanceof Short) {
            result = value.toString();
        } else if (value instanceof Byte) {
            result = value.toString();
        } else if (value instanceof Boolean) {
            result = value.toString();
        } else if (value instanceof Character) {
            result = value.toString();
        } else {
            throw new MessageFormatException("stream cannot convert byte array to String");
        }

        facade.pop();
        return result;
    }

    @Override
    public int readBytes(byte[] target) throws JMSException {
        checkWriteOnlyBody();

        if (target == null) {
            throw new NullPointerException("target byte array was null");
        }

        if (remainingBytes == NO_BYTES_IN_FLIGHT) {
            Object data = facade.peek();
            if (data == null) {
                facade.pop();
                return -1;
            } else if (!(data instanceof byte[])) {
                throw new MessageFormatException("Next stream value is not a byte array");
            }

            bytes = (byte[]) data;
            remainingBytes = bytes.length;
        } else if (remainingBytes == 0) {
            // We previously read all the bytes, but must have filled the destination array.
            remainingBytes = NO_BYTES_IN_FLIGHT;
            bytes = null;
            facade.pop();
            return -1;
        }

        int previouslyRead = bytes.length - remainingBytes;
        int lengthToCopy = Math.min(target.length, remainingBytes);

        if (lengthToCopy > 0) {
            System.arraycopy(bytes, previouslyRead, target, 0, lengthToCopy);
        }

        remainingBytes -= lengthToCopy;

        if (remainingBytes == 0 && lengthToCopy < target.length) {
            // All bytes have been read and the destination array was not filled on this
            // call, so the return will enable the caller to determine completion immediately.
            remainingBytes = NO_BYTES_IN_FLIGHT;
            bytes = null;
            facade.pop();
        }

        return lengthToCopy;
    }

    @Override
    public Object readObject() throws JMSException {
        checkWriteOnlyBody();
        checkBytesInFlight();

        Object result = null;
        Object value = facade.peek();

        if (value == null) {
            result = null;
        } else if (value instanceof String) {
            result = value;
        } else if (value instanceof Float) {
            result = value;
        } else if (value instanceof Double) {
            result = value;
        } else if (value instanceof Long) {
            result = value;
        } else if (value instanceof Integer) {
            result = value;
        } else if (value instanceof Short) {
            result = value;
        } else if (value instanceof Byte) {
            result = value;
        } else if (value instanceof Boolean) {
            result = value;
        } else if (value instanceof Character) {
            result = value;
        } else if (value instanceof byte[]) {
            byte[] original = (byte[]) value;
            result = new byte[original.length];
            System.arraycopy(original, 0, result, 0, original.length);
        } else {
            throw new MessageFormatException("Unknown type found in stream");
        }

        facade.pop();
        return result;
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeString(String value) throws JMSException {
        checkReadOnlyBody();
        facade.put(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        writeBytes(value, 0, value.length);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkReadOnlyBody();
        byte[] copy = new byte[length];
        System.arraycopy(value, offset, copy, 0, length);
        facade.put(copy);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        checkReadOnlyBody();
        if (value == null) {
            facade.put(null);
        } else if (value instanceof String) {
            facade.put(value);
        } else if (value instanceof Character) {
            facade.put(value);
        } else if (value instanceof Boolean) {
            facade.put(value);
        } else if (value instanceof Byte) {
            facade.put(value);
        } else if (value instanceof Short) {
            facade.put(value);
        } else if (value instanceof Integer) {
            facade.put(value);
        } else if (value instanceof Long) {
            facade.put(value);
        } else if (value instanceof Float) {
            facade.put(value);
        } else if (value instanceof Double) {
            facade.put(value);
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Unsupported Object type: " + value.getClass().getSimpleName());
        }
    }

    @Override
    public void reset() throws JMSException {
        checkReadOnly();
        bytes = null;
        remainingBytes = NO_BYTES_IN_FLIGHT;
        setReadOnlyBody(true);
        facade.reset();
    }

    @Override
    public String toString() {
        return "JmsStreamMessage { " + facade.toString() + " }";
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return false;
    }

    private void checkBytesInFlight() throws MessageFormatException {
        if (remainingBytes != NO_BYTES_IN_FLIGHT) {
            throw new MessageFormatException(
                "Partially read byte[] entry still being retrieved using readBytes(byte[] dest)");
        }
    }
}
