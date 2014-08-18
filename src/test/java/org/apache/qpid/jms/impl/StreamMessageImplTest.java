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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Arrays;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;

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

    @Test
    public void testNewMessageIsWriteOnlyThrowsMNRE() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            streamMessageImpl.readBoolean();
            fail("Expected exception to be thrown as message is not readable");
        }
        catch(MessageNotReadableException mnre)
        {
            //expected
        }
    }

    /**
     * Verify the stream position is not incremented during illegal type conversion failure.
     * This covers every read method except readObject (which doesn't do type conversion)
     * and readBytes(), which is tested by {@link #testIllegalTypeConvesionFailureDoesNotIncrementPosition2}
     *
     * Write bytes, then deliberately try to retrieve them as illegal types, then
     * check they can be successfully read.
     */
    @Test
    public void testIllegalTypeConvesionFailureDoesNotIncrementPosition1() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes =  new byte[]{(byte)0, (byte)255, (byte)78};

        streamMessageImpl.writeBytes(bytes);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, String.class);

        byte[] retrievedByteArray = new byte[bytes.length];
        int readBytesLength = streamMessageImpl.readBytes(retrievedByteArray);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, retrievedByteArray);
        assertEquals("Expected completion return value", -1, streamMessageImpl.readBytes(retrievedByteArray));
    }

    /**
     * Verify the stream position is not incremented during illegal type conversion failure.
     * This test covers only readBytes, other methods are tested by
     * {@link #testIllegalTypeConvesionFailureDoesNotIncrementPosition1}
     *
     * Write String, then deliberately try illegal retrieval as bytes, then
     * check it can be successfully read.
     */
    @Test
    public void testIllegalTypeConvesionFailureDoesNotIncrementPosition2() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String stringVal = "myString";
        streamMessageImpl.writeString(stringVal);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);

        assertEquals("Expected written string", stringVal, streamMessageImpl.readString());
    }

    /**
     * When a null stream entry is encountered, the accessor methods is type dependent and should
     * either return null, throw NPE, or behave in the same fashion as <primitive>.valueOf(String).
     *
     * Test that this is the case, and in doing show demonstrate that primitive type conversion failure
     * does not increment the stream position, as shown by not hitting the end of the stream unexpectedly.
     */
    @Test
    public void testNullStreamEntryResultsInExpectedBehaviour() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        streamMessageImpl.writeObject(null);
        streamMessageImpl.reset();

        //expect an NFE from the primitive integral <type>.valueOf(null) conversions
        assertGetStreamEntryThrowsNumberFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessageImpl, Long.class);

        //expect an NPE from the primitive float, double, and char <type>.valuleOf(null) conversions
        assertGetStreamEntryThrowsNullPointerException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsNullPointerException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsNullPointerException(streamMessageImpl, Character.class);

        //expect null
        assertNull(streamMessageImpl.readObject());
        streamMessageImpl.reset(); //need to reset as read was a success
        assertNull(streamMessageImpl.readString());
        streamMessageImpl.reset(); //need to reset as read was a success

        //expect completion value.
        assertEquals(-1, streamMessageImpl.readBytes(new byte[1]));
        streamMessageImpl.reset(); //need to reset as read was a success

        //expect false from Boolean.valueOf(null).
        assertFalse(streamMessageImpl.readBoolean());
        streamMessageImpl.reset(); //need to reset as read was a success
    }

    // ======= object =========

    @Test
    public void testWriteObjectWithIllegalTypeThrowsMFE() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        try
        {
            streamMessageImpl.writeObject(BigInteger.ONE);
            fail("Expected exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testWriteReadObject() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        Object nullEntryValue = null;
        Boolean boolEntryValue = Boolean.valueOf(false);
        Byte byteEntryValue = Byte.valueOf((byte)1);
        Short shortEntryValue = Short.valueOf((short)2);
        Integer intEntryValue = Integer.valueOf(3);
        Long longEntryValue = Long.valueOf(4);
        Float floatEntryValue = Float.valueOf(5.01F);
        Double doubleEntryValue = Double.valueOf(6.01);
        String stringEntryValue = "string";
        Character charEntryValue = Character.valueOf('c');
        byte[] bytes = new byte[] { (byte)1, (byte) 170, (byte)65};

        streamMessageImpl.writeObject(nullEntryValue);
        streamMessageImpl.writeObject(boolEntryValue);
        streamMessageImpl.writeObject(byteEntryValue);
        streamMessageImpl.writeObject(shortEntryValue);
        streamMessageImpl.writeObject(intEntryValue);
        streamMessageImpl.writeObject(longEntryValue);
        streamMessageImpl.writeObject(floatEntryValue);
        streamMessageImpl.writeObject(doubleEntryValue);
        streamMessageImpl.writeObject(stringEntryValue);
        streamMessageImpl.writeObject(charEntryValue);
        streamMessageImpl.writeObject(bytes);

        streamMessageImpl.reset();

        assertEquals("Got unexpected value from stream", nullEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", boolEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", byteEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", shortEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", intEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", longEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", floatEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", doubleEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", stringEntryValue, streamMessageImpl.readObject());
        assertEquals("Got unexpected value from stream", charEntryValue, streamMessageImpl.readObject());
        assertArrayEquals("Got unexpected value from stream", bytes, (byte[])streamMessageImpl.readObject());
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
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, String.class);
    }

    @Test
    public void testReadBytesWithNullSignalsCompletion() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        streamMessageImpl.writeObject(null);

        streamMessageImpl.reset();

        assertEquals("Expected immediate completion signal", -1, streamMessageImpl.readBytes(new byte[1]));
    }

    @Test
    public void testReadBytesWithZeroLengthSource() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        streamMessageImpl.writeBytes(new byte[0]);

        streamMessageImpl.reset();

        byte[] fullRetrievedBytes = new byte[1];

        assertEquals("Expected no bytes to be read, as none were written", 0, streamMessageImpl.readBytes(fullRetrievedBytes));
    }

    @Test
    public void testReadBytesWithZeroLengthDestination() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] bytes = new byte[]{(byte)11, (byte)44, (byte)99};
        streamMessageImpl.writeBytes(bytes);

        streamMessageImpl.reset();

        byte[] zeroDestination = new byte[0];
        byte[] fullRetrievedBytes = new byte[bytes.length];

        assertEquals("Expected no bytes to be read", 0, streamMessageImpl.readBytes(zeroDestination));
        assertEquals("Expected all bytes to be read", bytes.length, streamMessageImpl.readBytes(fullRetrievedBytes));
        assertArrayEquals("Expected arrays to be equal", bytes, fullRetrievedBytes);
        assertEquals("Expected completion signal", -1, streamMessageImpl.readBytes(zeroDestination));
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

    /**
     * Verify that setting bytes takes a copy of the array.
     * Set bytes subset, then retrieve the entry and verify the are different arrays and the subsets are equal.
     */
    @Test
    public void testWriteBytesWithOffsetAndLength() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte[] orig = "myBytesAll".getBytes();

        //extract the segment containing 'Bytes'
        int offset = 2;
        int length = 5;
        byte[] segment = Arrays.copyOfRange(orig, offset, offset + length);

        //set the same section from the original bytes
        streamMessageImpl.writeBytes(orig, offset, length);
        streamMessageImpl.reset();

        byte[] retrieved = (byte[]) streamMessageImpl.readObject();

        //verify the retrieved bytes from the stream equal the segment but are not the same
        assertNotSame(orig, retrieved);
        assertNotSame(segment, retrieved);
        assertArrayEquals(segment, retrieved);
    }

    //========= boolean ========

    @Test
    public void testWriteReadBoolean() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        boolean value = true;

        streamMessageImpl.writeBoolean(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readBoolean());
    }

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

        assertGetStreamEntryEquals(streamMessageImpl, true, value, Boolean.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
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

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= string ========

    @Test
    public void testWriteReadString() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String value = "myString";

        streamMessageImpl.writeString(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readString());
    }

    /**
     * Set a string, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteStringReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String integralValue = String.valueOf(Byte.MAX_VALUE);
        streamMessageImpl.writeString(integralValue);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(integralValue), String.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Boolean.valueOf(integralValue), Boolean.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Byte.valueOf(integralValue), Byte.class);

        streamMessageImpl.clearBody();
        integralValue = String.valueOf(Short.MAX_VALUE);
        streamMessageImpl.writeString(integralValue);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Short.valueOf(integralValue), Short.class);

        streamMessageImpl.clearBody();
        integralValue = String.valueOf(Integer.MAX_VALUE);
        streamMessageImpl.writeString(integralValue);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Integer.valueOf(integralValue), Integer.class);

        streamMessageImpl.clearBody();
        integralValue = String.valueOf(Long.MAX_VALUE);
        streamMessageImpl.writeString(integralValue);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Long.valueOf(integralValue), Long.class);

        streamMessageImpl.clearBody();
        String fpValue = String.valueOf(Float.MAX_VALUE);
        streamMessageImpl.writeString(fpValue);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Float.valueOf(fpValue), Float.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Double.valueOf(fpValue), Double.class);
    }

    /**
     * Set a string, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteStringReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String stringValue = "myString";

        streamMessageImpl.writeString(stringValue);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= byte ========

    @Test
    public void testWriteReadByte() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte value = (byte)6;

        streamMessageImpl.writeByte(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readByte());
    }

    /**
     * Set a byte, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteByteReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte value = (byte)6;

        streamMessageImpl.writeByte(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Byte.valueOf(value), Byte.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Short.valueOf(value), Short.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a byte, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteByteReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        byte value = (byte)6;

        streamMessageImpl.writeByte(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= short ========

    @Test
    public void testWriteReadShort() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        short value = (short)302;

        streamMessageImpl.writeShort(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readShort());
    }

    /**
     * Set a short, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteShortReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        short value = (short)302;

        streamMessageImpl.writeShort(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Short.valueOf(value), Short.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a short, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteShortReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        short value = (short)302;

        streamMessageImpl.writeShort(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= char ========

    @Test
    public void testWriteReadChar() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        char value = 'c';

        streamMessageImpl.writeChar(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readChar());
    }

    /**
     * Set a char, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteCharReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        char value = 'c';

        streamMessageImpl.writeChar(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, value, Character.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a char, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteCharReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        char value = 'c';

        streamMessageImpl.writeChar(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= int ========

    @Test
    public void testWriteReadInt() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        int value = Integer.MAX_VALUE;

        streamMessageImpl.writeInt(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readInt());
    }

    /**
     * Set an int, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteIntReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        int value = Integer.MAX_VALUE;

        streamMessageImpl.writeInt(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set an int, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteIntReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        int value = Integer.MAX_VALUE;

        streamMessageImpl.writeInt(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= long ========

    @Test
    public void testWriteReadLong() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        long value = Long.MAX_VALUE;

        streamMessageImpl.writeLong(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readLong());
    }

    /**
     * Set a long, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteLongReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        long value = Long.MAX_VALUE;

        streamMessageImpl.writeLong(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a long, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteLongReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        long value = Long.MAX_VALUE;

        streamMessageImpl.writeLong(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= float ========

    @Test
    public void testWriteReadFloat() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        float value = Float.MAX_VALUE;

        streamMessageImpl.writeFloat(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readFloat(), 0.0);
    }

    /**
     * Set a float, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteFloatReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        float value = Float.MAX_VALUE;

        streamMessageImpl.writeFloat(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Float.valueOf(value), Float.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, Double.valueOf(value), Double.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a float, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteFloatReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        float value = Float.MAX_VALUE;

        streamMessageImpl.writeFloat(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= double ========

    @Test
    public void testWriteReadDouble() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        double value = Double.MAX_VALUE;

        streamMessageImpl.writeDouble(value);
        streamMessageImpl.reset();

        assertEquals("Value not as expected", value, streamMessageImpl.readDouble(), 0.0);
    }

    /**
     * Set a double, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testWriteDoubleReadLegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        double value = Double.MAX_VALUE;

        streamMessageImpl.writeDouble(value);
        streamMessageImpl.reset();

        assertGetStreamEntryEquals(streamMessageImpl, true, Double.valueOf(value), Double.class);
        assertGetStreamEntryEquals(streamMessageImpl, true, String.valueOf(value), String.class);
    }

    /**
     * Set a double, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testWriteDoubleReadIllegal() throws Exception
    {
        StreamMessageImpl streamMessageImpl = new StreamMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        double value = Double.MAX_VALUE;

        streamMessageImpl.writeDouble(value);
        streamMessageImpl.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessageImpl, byte[].class);
    }

    //========= utility methods ========

    private void assertGetStreamEntryEquals(StreamMessageImpl testMessage,
                                            boolean resetStreamAfter,
                                            Object expectedValue,
                                            Class<?> clazz) throws JMSException
    {
        if(clazz == byte[].class)
        {
            throw new IllegalArgumentException("byte[] values not suported");
        }

        Object actualValue = getStreamEntryUsingTypeMethod(testMessage, clazz, null);
        assertEquals(expectedValue, actualValue);

        if(resetStreamAfter)
        {
            testMessage.reset();
        }
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

    private void assertGetStreamEntryThrowsNullPointerException(StreamMessageImpl testMessage, Class<?> clazz) throws JMSException
    {
        try
        {
            getStreamEntryUsingTypeMethod(testMessage, clazz, new byte[0]);

            fail("expected exception to be thrown");
        }
        catch(NullPointerException npe)
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
