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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;

@SuppressWarnings("unchecked")
public class JmsBytesMessage extends JmsMessage implements BytesMessage {

    protected transient DataOutputStream dataOut;
    protected transient DataInputStream dataIn;

    private final JmsBytesMessageFacade facade;

    public JmsBytesMessage(JmsBytesMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsBytesMessage copy() throws JMSException {
        JmsBytesMessage other = new JmsBytesMessage(facade.copy());
        other.copy(this);
        return other;
    }

    private void copy(JmsBytesMessage other) throws JMSException {
        super.copy(other);
        this.dataOut = null;
        this.dataIn = null;
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        this.dataOut = null;
        this.dataIn = null;
    }

    @Override
    public long getBodyLength() throws JMSException {
        initializeReading();
        return facade.getBodyLength();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readBoolean();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readByte();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUnsignedByte();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readShort();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUnsignedShort();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readChar();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readInt();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readLong();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readFloat();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readDouble();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUTF();
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        initializeReading();

        if (length < 0 || value.length < length) {
            throw new IndexOutOfBoundsException(
                "length must not be negative or larger than the size of the provided array");
        }

        try {
            int n = 0;
            while (n < length) {
                int count = this.dataIn.read(value, n, length - n);
                if (count < 0) {
                    break;
                }
                n += count;
            }
            if (n == 0 && length > 0) {
                n = -1;
            }
            return n;
        } catch (EOFException e) {
            throw JmsExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeByte(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeShort(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeChar(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeInt(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeLong(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeFloat(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeDouble(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeUTF(value);
        } catch (IOException ioe) {
            throw JmsExceptionSupport.create(ioe);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw JmsExceptionSupport.createMessageFormatException(e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException();
        }
        initializeWriting();
        if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass());
        }
    }

    @Override
    public void reset() throws JMSException {
        this.facade.reset();
        this.dataOut = null;
        this.dataIn = null;
        setReadOnlyBody(true);
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        reset();
        super.onSend(producerTtl);
    }

    @Override
    public String toString() {
        return "JmsBytesMessage { " + facade + " }";
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return facade.hasBody() ? target.isAssignableFrom(byte[].class) : true;
    }

    @Override
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        reset();

        if (!facade.hasBody()) {
            return null;
        }

        return (T) facade.copyBody();
    }

    private void initializeWriting() throws JMSException {
        checkReadOnlyBody();
        if (this.dataOut == null) {
            this.dataOut = new DataOutputStream(this.facade.getOutputStream());
        }
    }

    private void initializeReading() throws JMSException {
        checkWriteOnlyBody();
        if (dataIn == null) {
            dataIn = new DataInputStream(this.facade.getInputStream());
        }
    }
}
