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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.net.URI;
import java.util.Arrays;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

import org.apache.qpid.jms.message.facade.JmsStreamMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.apache.qpid.jms.message.facade.test.JmsTestStreamMessageFacade;
import org.junit.Test;

public class JmsStreamMessageTest {

    private final JmsMessageFactory factory = new JmsTestMessageFactory();

    // ======= general =========

    @Test
    public void testToString() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        streamMessage.onDispatch();
        assertTrue(streamMessage.toString().startsWith("JmsStreamMessage"));
    }

    @Test
    public void testReadWithEmptyStreamThrowsMEOFE() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        streamMessage.reset();

        try {
            streamMessage.readBoolean();
            fail("Expected exception to be thrown as message has no content");
        } catch (MessageEOFException meofe) {
            // expected
        }
    }

    @Test
    public void testClearBodyOnNewMessageRemovesExistingValues() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        streamMessage.writeBoolean(true);

        streamMessage.clearBody();

        streamMessage.writeBoolean(false);
        streamMessage.reset();

        // check we get only the value added after the clear
        assertFalse("expected value added after the clear", streamMessage.readBoolean());

        try {
            streamMessage.readBoolean();
            fail("Expected exception to be thrown");
        } catch (MessageEOFException meofe) {
            // expected
        }
    }

    @Test
    public void testNewMessageIsWriteOnlyThrowsMNRE() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        try {
            streamMessage.readBoolean();
            fail("Expected exception to be thrown as message is not readable");
        } catch (MessageNotReadableException mnre) {
            // expected
        }
    }

    /**
     * Verify the stream position is not incremented during illegal type conversion failure.
     * This covers every read method except readObject (which doesn't do type conversion) and
     * readBytes(), which is tested by
     * {@link #testIllegalTypeConvesionFailureDoesNotIncrementPosition2}
     *
     * Write bytes, then deliberately try to retrieve them as illegal types, then check they can
     * be successfully read.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testIllegalTypeConvesionFailureDoesNotIncrementPosition1() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 0, (byte) 255, (byte) 78 };

        streamMessage.writeBytes(bytes);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, String.class);

        byte[] retrievedByteArray = new byte[bytes.length];
        int readBytesLength = streamMessage.readBytes(retrievedByteArray);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, retrievedByteArray);
        assertEquals("Expected completion return value", -1, streamMessage.readBytes(retrievedByteArray));
    }

    /**
     * Verify the stream position is not incremented during illegal type conversion failure.
     * This test covers only readBytes, other methods are tested by
     * {@link #testIllegalTypeConvesionFailureDoesNotIncrementPosition1}
     *
     * Write String, then deliberately try illegal retrieval as bytes, then check it can be
     * successfully read.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testIllegalTypeConvesionFailureDoesNotIncrementPosition2() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        String stringVal = "myString";
        streamMessage.writeString(stringVal);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);

        assertEquals("Expected written string", stringVal, streamMessage.readString());
    }

    /**
     * When a null stream entry is encountered, the accessor methods is type dependent and
     * should either return null, throw NPE, or behave in the same fashion as
     * <primitive>.valueOf(String).
     *
     * Test that this is the case, and in doing show demonstrate that primitive type conversion
     * failure does not increment the stream position, as shown by not hitting the end of the
     * stream unexpectedly.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testNullStreamEntryResultsInExpectedBehaviour() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        streamMessage.writeObject(null);
        streamMessage.reset();

        // expect an NFE from the primitive integral <type>.valueOf(null) conversions
        assertGetStreamEntryThrowsNumberFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsNumberFormatException(streamMessage, Long.class);

        // expect an NPE from the primitive float, double, and char <type>.valuleOf(null)
        // conversions
        assertGetStreamEntryThrowsNullPointerException(streamMessage, Float.class);
        assertGetStreamEntryThrowsNullPointerException(streamMessage, Double.class);
        assertGetStreamEntryThrowsNullPointerException(streamMessage, Character.class);

        // expect null
        assertNull(streamMessage.readObject());
        streamMessage.reset(); // need to reset as read was a success
        assertNull(streamMessage.readString());
        streamMessage.reset(); // need to reset as read was a success

        // expect completion value.
        assertEquals(-1, streamMessage.readBytes(new byte[1]));
        streamMessage.reset(); // need to reset as read was a success

        // expect false from Boolean.valueOf(null).
        assertFalse(streamMessage.readBoolean());
        streamMessage.reset(); // need to reset as read was a success
    }

    @Test
    public void testClearBodyAppliesCorrectState() throws JMSException {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        try {
            streamMessage.writeObject(new Long(2));
            streamMessage.clearBody();
            assertFalse(streamMessage.isReadOnlyBody());
            streamMessage.writeObject(new Long(2));
            streamMessage.readObject();
            fail("should throw exception");
        } catch (MessageNotReadableException mnwe) {
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    @Test
    public void testResetAppliesCorrectState() throws JMSException {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        try {
            streamMessage.writeDouble(24.5);
            streamMessage.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        streamMessage.reset();
        try {
            assertTrue(streamMessage.isReadOnlyBody());
            assertEquals(streamMessage.readDouble(), 24.5, 0);
            assertEquals(streamMessage.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
        try {
            streamMessage.writeInt(33);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    // ======= object =========

    @Test
    public void testWriteObjectWithIllegalTypeThrowsMFE() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        try {
            streamMessage.writeObject(BigInteger.ONE);
            fail("Expected exception to be thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }
    }

    @Test
    public void testWriteReadObject() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        Object nullEntryValue = null;
        Boolean boolEntryValue = Boolean.valueOf(false);
        Byte byteEntryValue = Byte.valueOf((byte) 1);
        Short shortEntryValue = Short.valueOf((short) 2);
        Integer intEntryValue = Integer.valueOf(3);
        Long longEntryValue = Long.valueOf(4);
        Float floatEntryValue = Float.valueOf(5.01F);
        Double doubleEntryValue = Double.valueOf(6.01);
        String stringEntryValue = "string";
        Character charEntryValue = Character.valueOf('c');
        byte[] bytes = new byte[] { (byte) 1, (byte) 170, (byte) 65 };

        streamMessage.writeObject(nullEntryValue);
        streamMessage.writeObject(boolEntryValue);
        streamMessage.writeObject(byteEntryValue);
        streamMessage.writeObject(shortEntryValue);
        streamMessage.writeObject(intEntryValue);
        streamMessage.writeObject(longEntryValue);
        streamMessage.writeObject(floatEntryValue);
        streamMessage.writeObject(doubleEntryValue);
        streamMessage.writeObject(stringEntryValue);
        streamMessage.writeObject(charEntryValue);
        streamMessage.writeObject(bytes);

        streamMessage.reset();

        assertEquals("Got unexpected value from stream", nullEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", boolEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", byteEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", shortEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", intEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", longEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", floatEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", doubleEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", stringEntryValue, streamMessage.readObject());
        assertEquals("Got unexpected value from stream", charEntryValue, streamMessage.readObject());
        assertArrayEquals("Got unexpected value from stream", bytes, (byte[]) streamMessage.readObject());
    }

    // ======= bytes =========

    /**
     * Write bytes, then retrieve them as all of the legal type combinations
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteBytesReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] value = new byte[] { (byte) 0, (byte) 255, (byte) 78 };

        streamMessage.writeBytes(value);
        streamMessage.reset();

        byte[] dest = new byte[value.length];

        int readBytesLength = streamMessage.readBytes(dest);
        assertEquals("Number of bytes read did not match expectation", value.length, readBytesLength);
        assertArrayEquals("value not as expected", value, dest);
    }

    /**
     * Write bytes, then retrieve them as all of the illegal type combinations to verify it
     * fails as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteBytesReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] value = new byte[] { (byte) 0, (byte) 255, (byte) 78 };

        streamMessage.writeBytes(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, String.class);
    }

    @Test
    public void testReadBytesWithNullSignalsCompletion() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        streamMessage.writeObject(null);

        streamMessage.reset();

        assertEquals("Expected immediate completion signal", -1, streamMessage.readBytes(new byte[1]));
    }

    @Test
    public void testReadBytesWithZeroLengthSource() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        streamMessage.writeBytes(new byte[0]);

        streamMessage.reset();

        byte[] fullRetrievedBytes = new byte[1];

        assertEquals("Expected no bytes to be read, as none were written", 0, streamMessage.readBytes(fullRetrievedBytes));
    }

    @Test
    public void testReadBytesWithZeroLengthDestination() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 11, (byte) 44, (byte) 99 };
        streamMessage.writeBytes(bytes);

        streamMessage.reset();

        byte[] zeroDestination = new byte[0];
        byte[] fullRetrievedBytes = new byte[bytes.length];

        assertEquals("Expected no bytes to be read", 0, streamMessage.readBytes(zeroDestination));
        assertEquals("Expected all bytes to be read", bytes.length, streamMessage.readBytes(fullRetrievedBytes));
        assertArrayEquals("Expected arrays to be equal", bytes, fullRetrievedBytes);
        assertEquals("Expected completion signal", -1, streamMessage.readBytes(zeroDestination));
    }

    @Test
    public void testReadObjectForBytesReturnsNewArray() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 11, (byte) 44, (byte) 99 };
        streamMessage.writeBytes(bytes);

        streamMessage.reset();

        byte[] retrievedBytes = (byte[]) streamMessage.readObject();

        assertNotSame("Expected different array objects", bytes, retrievedBytes);
        assertArrayEquals("Expected arrays to be equal", bytes, retrievedBytes);
    }

    @Test
    public void testReadBytesFullWithUndersizedDestinationArrayUsingMultipleReads() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 3, (byte) 78, (byte) 253, (byte) 26, (byte) 8 };
        assertEquals("bytes should be odd length", 1, bytes.length % 2);
        int undersizedLength = 2;
        int remaining = 1;

        streamMessage.writeBytes(bytes);
        streamMessage.reset();

        byte[] undersizedDestination = new byte[undersizedLength];
        byte[] fullRetrievedBytes = new byte[bytes.length];

        assertEquals("Number of bytes read did not match destination array length", undersizedLength, streamMessage.readBytes(undersizedDestination));
        int read = undersizedLength;
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, 0, undersizedLength);
        assertEquals("Number of bytes read did not match destination array length", undersizedLength, streamMessage.readBytes(undersizedDestination));
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, read, undersizedLength);
        read += undersizedLength;
        assertEquals("Number of bytes read did not match expectation", remaining, streamMessage.readBytes(undersizedDestination));
        System.arraycopy(undersizedDestination, 0, fullRetrievedBytes, read, remaining);
        read += remaining;
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, fullRetrievedBytes);
    }

    @Test
    public void testReadBytesFullWithPreciselySizedDestinationArray() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 11, (byte) 44, (byte) 99 };
        streamMessage.writeBytes(bytes);

        streamMessage.reset();

        byte[] retrievedByteArray = new byte[bytes.length];
        int readBytesLength = streamMessage.readBytes(retrievedByteArray);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array to equal retrieved bytes", bytes, retrievedByteArray);
        assertEquals("Expected completion return value", -1, streamMessage.readBytes(retrievedByteArray));
    }

    @Test
    public void testReadBytesFullWithOversizedDestinationArray() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 4, (byte) 115, (byte) 255 };
        streamMessage.writeBytes(bytes);

        streamMessage.reset();

        byte[] oversizedDestination = new byte[bytes.length + 1];
        int readBytesLength = streamMessage.readBytes(oversizedDestination);

        assertEquals("Number of bytes read did not match original array length", bytes.length, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes", bytes, Arrays.copyOfRange(oversizedDestination, 0, readBytesLength));
    }

    /**
     * {@link StreamMessage#readBytes(byte[])} indicates:
     *
     * "Once the first readBytes call on a byte[] field value has been made, the full value of
     * the field must be read before it is valid to read the next field. An attempt to read the
     * next field before that has been done will throw a MessageFormatException."
     *
     * {@link StreamMessage#readObject()} indicates: "An attempt to call readObject to read a
     * byte field value into a new byte[] object before the full value of the byte field has
     * been read will throw a MessageFormatException."
     *
     * Test that these restrictions are met, and don't interfere with completing the readBytes
     * usage.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testReadObjectAfterPartialReadBytesThrowsMFE() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] bytes = new byte[] { (byte) 11, (byte) 44, (byte) 99 };
        streamMessage.writeBytes(bytes);

        streamMessage.reset();

        // start reading via readBytes
        int partialLength = 2;
        byte[] retrievedByteArray = new byte[partialLength];
        int readBytesLength = streamMessage.readBytes(retrievedByteArray);

        assertEquals(partialLength, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes", Arrays.copyOf(bytes, partialLength), retrievedByteArray);

        // check that using readObject does not return the full/remaining bytes as a new array
        try {
            streamMessage.readObject();
            fail("expected exception to be thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        // finish reading via reaBytes to ensure it can be completed
        readBytesLength = streamMessage.readBytes(retrievedByteArray);
        assertEquals(bytes.length - partialLength, readBytesLength);
        assertArrayEquals("Expected array subset to equal retrieved bytes", Arrays.copyOfRange(bytes, partialLength, bytes.length),
            Arrays.copyOfRange(retrievedByteArray, 0, readBytesLength));
    }

    /**
     * Verify that setting bytes takes a copy of the array. Set bytes subset, then retrieve the
     * entry and verify the are different arrays and the subsets are equal.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteBytesWithOffsetAndLength() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte[] orig = "myBytesAll".getBytes();

        // extract the segment containing 'Bytes'
        int offset = 2;
        int length = 5;
        byte[] segment = Arrays.copyOfRange(orig, offset, offset + length);

        // set the same section from the original bytes
        streamMessage.writeBytes(orig, offset, length);
        streamMessage.reset();

        byte[] retrieved = (byte[]) streamMessage.readObject();

        // verify the retrieved bytes from the stream equal the segment but are not the same
        assertNotSame(orig, retrieved);
        assertNotSame(segment, retrieved);
        assertArrayEquals(segment, retrieved);
    }

    // ========= boolean ========

    @Test
    public void testWriteReadBoolean() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        boolean value = true;

        streamMessage.writeBoolean(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readBoolean());
    }

    /**
     * Set a boolean, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteBooleanReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        boolean value = true;

        streamMessage.writeBoolean(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, value, Boolean.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a boolean, then retrieve it as all of the illegal type combinations to verify it
     * fails as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBooleanGetIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        boolean value = true;

        streamMessage.writeBoolean(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= string ========

    @Test
    public void testWriteReadString() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        String value = "myString";

        streamMessage.writeString(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readString());
    }

    /**
     * Set a string, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteStringReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        String integralValue = String.valueOf(Byte.MAX_VALUE);
        streamMessage.writeString(integralValue);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(integralValue), String.class);
        assertGetStreamEntryEquals(streamMessage, true, Boolean.valueOf(integralValue), Boolean.class);
        assertGetStreamEntryEquals(streamMessage, true, Byte.valueOf(integralValue), Byte.class);

        streamMessage.clearBody();
        integralValue = String.valueOf(Short.MAX_VALUE);
        streamMessage.writeString(integralValue);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Short.valueOf(integralValue), Short.class);

        streamMessage.clearBody();
        integralValue = String.valueOf(Integer.MAX_VALUE);
        streamMessage.writeString(integralValue);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Integer.valueOf(integralValue), Integer.class);

        streamMessage.clearBody();
        integralValue = String.valueOf(Long.MAX_VALUE);
        streamMessage.writeString(integralValue);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Long.valueOf(integralValue), Long.class);

        streamMessage.clearBody();
        String fpValue = String.valueOf(Float.MAX_VALUE);
        streamMessage.writeString(fpValue);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Float.valueOf(fpValue), Float.class);
        assertGetStreamEntryEquals(streamMessage, true, Double.valueOf(fpValue), Double.class);
    }

    /**
     * Set a string, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteStringReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        String stringValue = "myString";

        streamMessage.writeString(stringValue);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    @Test
    public void testReadBigString() throws JMSException {
        JmsStreamMessage msg = factory.createStreamMessage();
        // Test with a 1Meg String
        StringBuffer bigSB = new StringBuffer(1024 * 1024);
        for (int i = 0; i < 1024 * 1024; i++) {
            bigSB.append('a' + i % 26);
        }
        String bigString = bigSB.toString();

        msg.writeString(bigString);
        msg.reset();
        assertEquals(bigString, msg.readString());
    }

    // ========= byte ========

    @Test
    public void testWriteReadByte() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte value = (byte) 6;

        streamMessage.writeByte(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readByte());
    }

    /**
     * Set a byte, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteByteReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte value = (byte) 6;

        streamMessage.writeByte(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Byte.valueOf(value), Byte.class);
        assertGetStreamEntryEquals(streamMessage, true, Short.valueOf(value), Short.class);
        assertGetStreamEntryEquals(streamMessage, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessage, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a byte, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteByteReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        byte value = (byte) 6;

        streamMessage.writeByte(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= short ========

    @Test
    public void testWriteReadShort() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        short value = (short) 302;

        streamMessage.writeShort(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readShort());
    }

    /**
     * Set a short, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteShortReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        short value = (short) 302;

        streamMessage.writeShort(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Short.valueOf(value), Short.class);
        assertGetStreamEntryEquals(streamMessage, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessage, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a short, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteShortReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        short value = (short) 302;

        streamMessage.writeShort(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= char ========

    @Test
    public void testWriteReadChar() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        char value = 'c';

        streamMessage.writeChar(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readChar());
    }

    /**
     * Set a char, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteCharReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        char value = 'c';

        streamMessage.writeChar(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, value, Character.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a char, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteCharReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        char value = 'c';

        streamMessage.writeChar(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= int ========

    @Test
    public void testWriteReadInt() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        int value = Integer.MAX_VALUE;

        streamMessage.writeInt(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readInt());
    }

    /**
     * Set an int, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteIntReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        int value = Integer.MAX_VALUE;

        streamMessage.writeInt(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Integer.valueOf(value), Integer.class);
        assertGetStreamEntryEquals(streamMessage, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set an int, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteIntReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        int value = Integer.MAX_VALUE;

        streamMessage.writeInt(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= long ========

    @Test
    public void testWriteReadLong() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        long value = Long.MAX_VALUE;

        streamMessage.writeLong(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readLong());
    }

    /**
     * Set a long, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteLongReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        long value = Long.MAX_VALUE;

        streamMessage.writeLong(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Long.valueOf(value), Long.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a long, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteLongReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        long value = Long.MAX_VALUE;

        streamMessage.writeLong(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Double.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= float ========

    @Test
    public void testWriteReadFloat() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        float value = Float.MAX_VALUE;

        streamMessage.writeFloat(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readFloat(), 0.0);
    }

    /**
     * Set a float, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteFloatReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        float value = Float.MAX_VALUE;

        streamMessage.writeFloat(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Float.valueOf(value), Float.class);
        assertGetStreamEntryEquals(streamMessage, true, Double.valueOf(value), Double.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a float, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteFloatReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        float value = Float.MAX_VALUE;

        streamMessage.writeFloat(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= double ========

    @Test
    public void testWriteReadDouble() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        double value = Double.MAX_VALUE;

        streamMessage.writeDouble(value);
        streamMessage.reset();

        assertEquals("Value not as expected", value, streamMessage.readDouble(), 0.0);
    }

    /**
     * Set a double, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteDoubleReadLegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        double value = Double.MAX_VALUE;

        streamMessage.writeDouble(value);
        streamMessage.reset();

        assertGetStreamEntryEquals(streamMessage, true, Double.valueOf(value), Double.class);
        assertGetStreamEntryEquals(streamMessage, true, String.valueOf(value), String.class);
    }

    /**
     * Set a double, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWriteDoubleReadIllegal() throws Exception {
        JmsStreamMessage streamMessage = factory.createStreamMessage();

        double value = Double.MAX_VALUE;

        streamMessage.writeDouble(value);
        streamMessage.reset();

        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Boolean.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Byte.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Short.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Character.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Integer.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Long.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, Float.class);
        assertGetStreamEntryThrowsMessageFormatException(streamMessage, byte[].class);
    }

    // ========= read failures ========

    @Test(expected=NullPointerException.class)
    public void testReadBytesWithNullArrayThrowsNPE() throws JMSException {
        JmsStreamMessage streamMessage = factory.createStreamMessage();
        streamMessage.reset();
        streamMessage.readBytes(null);
    }

    @Test
    public void testReadObjectGetsInvalidObjectThrowsMFE() throws Exception {
        JmsStreamMessageFacade facade = new JmsTestStreamMessageFacade();
        JmsStreamMessage streamMessage = new JmsStreamMessage(facade);
        facade.put(new URI("test://test"));
        streamMessage.reset();

        try {
            streamMessage.readObject();
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
        }
    }

    // ========= utility methods ========

    private void assertGetStreamEntryEquals(JmsStreamMessage testMessage, boolean resetStreamAfter, Object expectedValue, Class<?> clazz) throws JMSException {
        if (clazz == byte[].class) {
            throw new IllegalArgumentException("byte[] values not suported");
        }

        Object actualValue = getStreamEntryUsingTypeMethod(testMessage, clazz, null);
        assertEquals(expectedValue, actualValue);

        if (resetStreamAfter) {
            testMessage.reset();
        }
    }

    private void assertGetStreamEntryThrowsMessageFormatException(JmsStreamMessage testMessage, Class<?> clazz) throws JMSException {
        try {
            getStreamEntryUsingTypeMethod(testMessage, clazz, new byte[0]);

            fail("expected exception to be thrown");
        } catch (MessageFormatException jmsMFE) {
            // expected
        }
    }

    private void assertGetStreamEntryThrowsNullPointerException(JmsStreamMessage testMessage, Class<?> clazz) throws JMSException {
        try {
            getStreamEntryUsingTypeMethod(testMessage, clazz, new byte[0]);

            fail("expected exception to be thrown");
        } catch (NullPointerException npe) {
            // expected
        }
    }

    private void assertGetStreamEntryThrowsNumberFormatException(JmsStreamMessage testMessage, Class<?> clazz) throws JMSException {
        assertGetStreamEntryThrowsNumberFormatException(testMessage, clazz, null);
    }

    private void assertGetStreamEntryThrowsNumberFormatException(JmsStreamMessage testMessage, Class<?> clazz, byte[] destination) throws JMSException {
        if (clazz == byte[].class && destination == null) {
            throw new IllegalArgumentException("Destinatinon byte[] must be supplied");
        }

        try {
            getStreamEntryUsingTypeMethod(testMessage, clazz, destination);

            fail("expected exception to be thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    private Object getStreamEntryUsingTypeMethod(JmsStreamMessage testMessage, Class<?> clazz, byte[] destination) throws JMSException {
        if (clazz == Boolean.class) {
            return testMessage.readBoolean();
        } else if (clazz == Byte.class) {
            return testMessage.readByte();
        } else if (clazz == Character.class) {
            return testMessage.readChar();
        } else if (clazz == Short.class) {
            return testMessage.readShort();
        } else if (clazz == Integer.class) {
            return testMessage.readInt();
        } else if (clazz == Long.class) {
            return testMessage.readLong();
        } else if (clazz == Float.class) {
            return testMessage.readFloat();
        } else if (clazz == Double.class) {
            return testMessage.readDouble();
        } else if (clazz == String.class) {
            return testMessage.readString();
        } else if (clazz == byte[].class) {
            return testMessage.readBytes(destination);
        } else {
            throw new RuntimeException("Unexpected entry type class");
        }
    }
}
