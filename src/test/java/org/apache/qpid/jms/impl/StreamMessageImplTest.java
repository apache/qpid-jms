/*
 *
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
 *
 */
package org.apache.qpid.jms.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.util.Arrays;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamMessageImplTest extends QpidJmsTestCase
{
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
    }

    // ======= general =========

    @Test
    public void testReadWithEmptyStreamThrowsMEOFE() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        //make it readable
        streamMessageImpl.reset();

        try
        {
            streamMessageImpl.readBoolean();
            fail("Expected exception to be thrown as message has no content");
        }
        catch(MessageEOFException meofe)
        {
            //expected
        }
    }

    @Test
    public void testClearBodyOnNewMessageRemovesExistingValues() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        streamMessageImpl.writeBoolean(true);

        streamMessageImpl.clearBody();

        streamMessageImpl.writeBoolean(false);
        streamMessageImpl.reset();

        //check we get only the value added after the clear
        assertFalse("expected value added after the clear", streamMessageImpl.readBoolean());

        try
        {
            streamMessageImpl.readBoolean();
            fail("Expected exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //expected
        }
    }

    // ======= object =========

    @Test
    public void testWriteObjectWithIllegalTypeThrowsMFE() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        try
        {
            streamMessageImpl.writeObject(new Exception());
            fail("Expected exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    // ======= bytes =========

    /**
     * Write bytes, then retrieve them as all of the legal type combinations
     */
    @Test
    public void testWriteBytesReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] value =  new byte[]{(byte)0, (byte)255, (byte)78};

        streamMessageImpl.writeBytes(value);
        streamMessageImpl.reset();

        byte[] dest = new byte[value.length];

        int readBytesLength = streamMessageImpl.readBytes(dest);
        assertEquals("Number of bytes read did not match expectation", value.length, readBytesLength);
        assertArrayEquals("value not as expected", value, dest);
    }

    /**
     * Write bytes, then retrieve them as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteBytesReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] value =  new byte[]{(byte)0, (byte)255, (byte)78};

        streamMessageImpl.writeBytes(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        /* TODO: enable when implementing
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, String.class);
        */
    }

    @Test
    public void testReadObjectForBytesReturnsNewArray() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)11, (byte)44, (byte)99};
        streamMessageImpl.writeBytes(bytes);

        streamMessageImpl.reset();

        byte[] retrievedBytes = (byte[]) streamMessageImpl.readObject();

        assertNotSame("Expected different array objects", bytes, retrievedBytes);
        assertArrayEquals("Expected arrays to be equal", bytes, retrievedBytes);
    }

    @Test
    public void testReadBytesFullWithUndersizedDestinationArrayUsingMultipleReads() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)3, (byte)78, (byte)253, (byte) 26, (byte) 8};
        assertEquals("bytes should be odd length", 1, bytes.length % 2);
        int undersizedLength = 2;
        int remaining = 1;

        streamMessageImpl.writeBytes(bytes);
        streamMessageImpl.reset();

        byte[] undersizedDestination = new byte[undersizedLength];
        byte[] fullRetrievedBytes = new byte[bytes.length];

        assertEquals("Number of bytes read did not match destination array length", undersizedLength, streamMessageImpl.readBytes(undersizedDestination));
        int read = undersizedLength;
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, 0, undersizedLength);
        assertEquals("Number of bytes read did not match destination array length", undersizedLength, streamMessageImpl.readBytes(undersizedDestination));
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, read, undersizedLength);
        read += undersizedLength;
        assertEquals("Number of bytes read did not match expectation", remaining, streamMessageImpl.readBytes(undersizedDestination));
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, read, remaining);
        read += remaining;
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, fullRetrievedBytes);
    }

    @Test
    public void testReadBytesFullWithPreciselySizedDestinationArray() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)11, (byte)44, (byte)99};
        streamMessageImpl.writeBytes(bytes);

        streamMessageImpl.reset();

        byte[] retrievedByteArray = new byte[bytes.length];
        int readBytesLength = streamMessageImpl.readBytes(retrievedByteArray);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, retrievedByteArray);
        assertEquals("Expected completion return value", -1, streamMessageImpl.readBytes(retrievedByteArray));
    }

    @Test
    public void testReadBytesFullWithOversizedDestinationArray() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)4, (byte)115, (byte)255};
        streamMessageImpl.writeBytes(bytes);

        streamMessageImpl.reset();

        byte[] oversizedDestination = new byte[bytes.length + 1];
        int readBytesLength = streamMessageImpl.readBytes(oversizedDestination);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes", bytes, Arrays.copyOfRange(oversizedDestination, 0, readBytesLength));
    }

    /**
     * {@link StreamMessage#readBytes(byte[])} indicates:
     *
     * "Once the first readBytes call on a byte[] field value has been made, the full value of the field must be read
     * before it is valid to read the next field. An attempt to read the next field before that has been done will
     * throw a MessageFormatException."
     *
     * {@link StreamMessage#readObject()} indicates:
     * "An attempt to call readObject to read a byte field value into a new byte[] object before the full value
     * of the byte field has been read will throw a MessageFormatException."
     *
     * Test that these restrictions are met, and don't interfere with completing the readBytes usage.
     */
    @Test
    public void testReadObjectAfterPartialReadBytesThrowsMFE() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)11, (byte)44, (byte)99};
        streamMessageImpl.writeBytes(bytes);

        streamMessageImpl.reset();

        //start reading via readBytes
        int partialLength = 2;
        byte[] retrievedByteArray = new byte[partialLength];
        int readBytesLength = streamMessageImpl.readBytes(retrievedByteArray);

        assertEquals(partialLength, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes", Arrays.copyOf(bytes, partialLength), retrievedByteArray);

        //check that using readObject does not return the full/remaining bytes as a new array
        try
        {
            streamMessageImpl.readObject();
            fail("expected exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }

        //finish reading via reaBytes to ensure it can be completed
        readBytesLength = streamMessageImpl.readBytes(retrievedByteArray);
        assertEquals(bytes.length - partialLength, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes",
                Arrays.copyOfRange(bytes, partialLength, bytes.length), Arrays.copyOfRange(retrievedByteArray, 0, readBytesLength));
    }

    //========= boolean ========

    /**
     * Set a boolean, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteBooleanReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        boolean value = true;

        streamMessageImpl.writeBoolean(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, value, Boolean.class);

        /* TODO: enable when implementing
        assertGetStreamEntryEquals(streamMessageImpl, String.valueOf(value), String.class);
        */
    }

    /**
     * Set a boolean, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetBooleanGetIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        boolean value = true;

        streamMessageImpl.writeBoolean(value);
        streamMessageImpl.reset();

        /* TODO: enable when implementing
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
        */
    }

    //========= utility methods ========

    private void assertGetStreamEntryEquals(StreamMessageImpl testMessage,
                                            Object expectedValue,
                                            Class<?> clazz) throws JMSException
    {
        if(clazz == byte[].class)
        {
            throw new IllegalArgumentException("byte[] values not suported");
        }

        Object actualValue = getStreamEntryUsingTypeMethod(testMessage, clazz, null);
        assertEquals(expectedValue, actualValue);
    }

    private void assertGetStreamEntryThrowsMessageFormatException(StreamMessageImpl testMessage,
                                                                  Class<?> clazz) throws JMSException
    {
        try
        {
            getStreamEntryUsingTypeMethod(testMessage, clazz, new byte[0]);

            fail("expected exception to be thrown");
        }
        catch(MessageFormatException jmsMFE)
        {
            //expected
        }
    }

    private void assertGetStreamEntryThrowsNumberFormatException(StreamMessageImpl testMessage,
                                                                 Class<?> clazz) throws JMSException
    {
        assertGetStreamEntryThrowsNumberFormatException(testMessage, clazz, null);
    }

    private void assertGetStreamEntryThrowsNumberFormatException(StreamMessageImpl testMessage,
                                                                 Class<?> clazz,
                                                                 byte[] destination) throws JMSException
    {
        if(clazz == byte[].class && destination == null)
        {
            throw new IllegalArgumentException("Destinatinon byte[] must be supplied");
        }

        try
        {
            getStreamEntryUsingTypeMethod(testMessage, clazz, destination);

            fail("expected exception to be thrown");
        }
        catch(NumberFormatException nfe)
        {
            //expected
        }
    }

    private Object getStreamEntryUsingTypeMethod(StreamMessageImpl testMessage, Class<?> clazz, byte[] destination) throws JMSException
    {
        if(clazz == Boolean.class)
        {
            return testMessage.readBoolean();
        }
        else if(clazz == Byte.class)
        {
            return testMessage.readByte();
        }
        else if(clazz == Character.class)
        {
            return testMessage.readChar();
        }
        else if(clazz == Short.class)
        {
            return testMessage.readShort();
        }
        else if(clazz == Integer.class)
        {
            return testMessage.readInt();
        }
        else if(clazz == Long.class)
        {
            return testMessage.readLong();
        }
        else if(clazz == Float.class)
        {
            return testMessage.readFloat();
        }
        else if(clazz == Double.class)
        {
            return testMessage.readDouble();
        }
        else if(clazz == String.class)
        {
            return testMessage.readString();
        }
        else if(clazz == byte[].class)
        {
            return testMessage.readBytes(destination);
        }
        else
        {
            throw new RuntimeException("Unexpected entry type class");
        }
    }
}
