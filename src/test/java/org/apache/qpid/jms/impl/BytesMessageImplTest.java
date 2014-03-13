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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpBytesMessage;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BytesMessageImplTest extends QpidJmsTestCase
{
    private static final int END_OF_STREAM = -1;
    private static final String INPUT_STREAM_HELPER = "_inputStreamHelper";
    private static final String OUTPUT_STREAM_HELPER = "_outputStreamHelper";

    private Delivery _mockDelivery;
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private AmqpConnection _mockAmqpConnection;
    private AmqpBytesMessage _mockAmqpMessage;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpMessage = Mockito.mock(AmqpBytesMessage.class);
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithNoBodySection() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage testAmqpMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(testAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertEquals(0, bytesMessageImpl.getBodyLength());
    }

    /**
     * Test that calling {@link BytesMessage#getBodyLength()} on a new message which has been
     * populated and {@link BytesMessage#reset()} causes the length to be reported correctly.
     */
    @Test
    public void testResetOnNewlyPopulatedBytesMessageUpdatesBodyLength() throws Exception
    {
        byte[] bytes = "newResetTestBytes".getBytes();

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeBytes(bytes);
        bytesMessageImpl.reset();
        assertEquals("Message reports unexpected length", bytes.length, bytesMessageImpl.getBodyLength());
    }

    /**
     * Test that attempting to call {@link BytesMessage#getBodyLength()} on a new message causes a
     * {@link MessageNotReadableException} to be thrown due to being write-only.
     */
    @Test
    public void testGetBodyLengthOnNewMessageThrowsMessageNotReadableException() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            bytesMessageImpl.getBodyLength();
            fail("expected exception to be thrown");
        }
        catch(MessageNotReadableException mnre)
        {
            //expected
        }
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithNoBodySectionReturnsEOS() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage testAmqpMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(testAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);

        //verify attempting to read bytes returns -1, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesMessageImpl.readBytes(new byte[1]));
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithDataSectionReturnsBytes() throws Exception
    {
        byte[] bytes = "myBytesData".getBytes();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesMessageImpl.readBytes(new byte[1]));

        assertEquals("Message reports unexpected length", bytes.length, bytesMessageImpl.getBodyLength());
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithAmpValueSectionReturnsBytes() throws Exception
    {
        byte[] bytes = "myBytesAmqpValue".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesMessageImpl.readBytes(receivedBytes));

        assertEquals("Message reports unexpected length", bytes.length, bytesMessageImpl.getBodyLength());
    }

    /**
     * Test that attempting to write bytes to a received message (without calling {@link BytesMessage#clearBody()} first)
     * causes a {@link MessageNotWriteableException} to be thrown due to being read-only.
     */
    @Test
    public void testReceivedBytesMessageThrowsMessageNotWriteableExceptionOnWriteBytes() throws Exception
    {
        byte[] bytes = "myBytesAmqpValue".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        try
        {
            bytesMessageImpl.writeBytes(bytes);
            fail("expected exception to be thrown");
        }
        catch(MessageNotWriteableException mnwe)
        {
            //expected
        }
    }

    /**
     * Test that attempting to read bytes from a new message (without calling {@link BytesMessage#reset()} first) causes a
     * {@link MessageNotReadableException} to be thrown due to being write-only.
     */
    @Test
    public void testNewBytesMessageThrowsMessageNotReadableOnReadBytes() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[1];
        try
        {
            bytesMessageImpl.readBytes(receivedBytes);
            fail("expected exception to be thrown");
        }
        catch(MessageNotReadableException mnre)
        {
            //expected
        }
    }

    /**
     * Test that calling {@link BytesMessage#clearBody()} causes a received
     * message to become writable
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageMakesMessageWritable() throws Exception
    {
        byte[] bytes = "myBytesAmqpValue".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertFalse("Message should not be writable", bytesMessageImpl.isBodyWritable());

        bytesMessageImpl.clearBody();

        assertTrue("Message should be writable", bytesMessageImpl.isBodyWritable());
    }

    /**
     * Test that calling {@link BytesMessage#clearBody()} of a received message
     * causes the body of the underlying {@link AmqpBytesMessage} to be emptied.
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageClearsUnderlyingMessageBody() throws Exception
    {
        byte[] bytes = "myBytesAmqpValue".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertNotNull("Expected body section but none was present", message.getBody());

        bytesMessageImpl.clearBody();

        //check that the returned BAIS returns no data and reports 0 length
        ByteArrayInputStream bais = amqpBytesMessage.getByteArrayInputStream();
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bais.read(new byte[1]));
        assertEquals("Underlying message should report 0 length", 0, amqpBytesMessage.getBytesLength());

        //verify the underlying message has no body section
        //TODO: this test assumes we can omit the body section. If we decide otherwise
        //it should instead check for e.g. a data section containing 0 length binary
        assertNull("Expected no body section", message.getBody());
    }

    /**
     * Test that attempting to call {@link BytesMessage#getBodyLength()} on a received message after calling
     * {@link BytesMessage#clearBody()} causes {@link MessageNotReadableException} to be thrown due to being write-only.
     */
    @Test
    public void testGetBodyLengthOnClearedReceivedMessageThrowsMessageNotReadableException() throws Exception
    {
        byte[] bytes = "myBytesData".getBytes();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertEquals("Unexpected message length", bytes.length, bytesMessageImpl.getBodyLength());

        bytesMessageImpl.clearBody();

        try
        {
            bytesMessageImpl.getBodyLength();
            fail("expected exception to be thrown");
        }
        catch(MessageNotReadableException mnre)
        {
            //expected
        }
    }

    /**
     * Test that calling {@link BytesMessage#reset()} causes a write-only
     * message to become read-only
     */
    @Test
    public void testResetOnReceivedBytesMessageResetsMarker() throws Exception
    {
        byte[] bytes = "resetTestBytes".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(_mockDelivery, message, _mockAmqpConnection);
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(amqpBytesMessage, _mockSessionImpl,_mockConnectionImpl, null);

        //retrieve a few bytes, check they match the first few expected bytes
        byte[] partialBytes = new byte[3];
        bytesMessageImpl.readBytes(partialBytes);
        byte[] partialOriginalBytes = Arrays.copyOf(bytes, 3);
        assertTrue(Arrays.equals(partialOriginalBytes, partialBytes));

        bytesMessageImpl.reset();

        //retrieve all the expected bytes, check they match
        byte[] resetBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(resetBytes);
        assertTrue(Arrays.equals(bytes, resetBytes));
    }

    /**
     * Test that calling {@link BytesMessage#reset()} on a new message which has been
     * populated causes the marker to be reset and makes the message read-only
     */
    @Test
    public void testResetOnNewlyPopulatedBytesMessageResetsMarkerAndMakesReadable() throws Exception
    {
        byte[] bytes = "newResetTestBytes".getBytes();

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        assertTrue("Message should be writable", bytesMessageImpl.isBodyWritable());
        bytesMessageImpl.writeBytes(bytes);
        bytesMessageImpl.reset();
        assertFalse("Message should not be writable", bytesMessageImpl.isBodyWritable());

        //retrieve the bytes, check they match
        byte[] resetBytes = new byte[bytes.length];
        bytesMessageImpl.readBytes(resetBytes);
        assertTrue(Arrays.equals(bytes, resetBytes));
    }

    /**
     * Test that writing a variety of type values into a new message, resetting the
     * message to make it readable, and then reading back the values works as expected.
     */
    @Test
    public void testWriteValuesThenResetAndReadValues() throws Exception
    {
        boolean myBool = true;
        byte myByte = 4;
        byte[] myBytes = "myBytes".getBytes();
        char myChar = 'd';
        double myDouble = 1234567890123456789.1234;
        float myFloat = 1.1F;
        int myInt = Integer.MAX_VALUE;
        long myLong = Long.MAX_VALUE;
        short myShort = 25;
        String myUTF = "myString";

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeBoolean(myBool);
        bytesMessageImpl.writeByte(myByte);
        bytesMessageImpl.writeBytes(myBytes);
        int offset = 1;
        int adjustedLength = myBytes.length - offset;
        bytesMessageImpl.writeBytes(myBytes, offset, adjustedLength);
        bytesMessageImpl.writeChar(myChar);
        bytesMessageImpl.writeDouble(myDouble);
        bytesMessageImpl.writeFloat(myFloat);
        bytesMessageImpl.writeInt(myInt);
        bytesMessageImpl.writeLong(myLong);
        bytesMessageImpl.writeShort(myShort);
        bytesMessageImpl.writeUTF(myUTF);

        bytesMessageImpl.reset();

        assertEquals("Unexpected boolean value", myBool, bytesMessageImpl.readBoolean());
        assertEquals("Unexpected byte value", myByte, bytesMessageImpl.readByte());
        //retrieve the bytes, check they match
        byte[] readBytes = new byte[myBytes.length];
        assertEquals("Did not read the expected number of bytes", myBytes.length, bytesMessageImpl.readBytes(readBytes));
        assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
        //retrieve the partial bytes, check they match
        readBytes = new byte[adjustedLength];
        assertEquals("Did not read the expected number of bytes", adjustedLength, bytesMessageImpl.readBytes(readBytes));
        byte[] adjustedBytes = Arrays.copyOfRange(myBytes, offset, myBytes.length);
        assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(adjustedBytes, readBytes));
        assertEquals("Unexpected char value", myChar, bytesMessageImpl.readChar());
        assertEquals("Unexpected double value", myDouble, bytesMessageImpl.readDouble(), 0.0);
        assertEquals("Unexpected float value", myFloat, bytesMessageImpl.readFloat(), 0.0);
        assertEquals("Unexpected int value", myInt, bytesMessageImpl.readInt());
        assertEquals("Unexpected long value", myLong, bytesMessageImpl.readLong());
        assertEquals("Unexpected short value", myShort, bytesMessageImpl.readShort());
        assertEquals("Unexpected UTF value", myUTF, bytesMessageImpl.readUTF());
    }

    /**
     * Test that writing a byte using {@link BytesMessage#writeByte(Object)}, resetting the
     * message to make it readable, and then reading back the value using
     * {@link BytesMessage#readUnsignedByte()} instead produces the expected the comes
     * from interpreting the sign bit as part of the value.
     */
    @Test
    public void testReadUnsignedByte() throws Exception
    {
        byte myByte = -5;
        int expectedUnsigned = 251;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeByte(myByte);

        bytesMessageImpl.reset();

        assertEquals("Did not get the expected unsigned value", expectedUnsigned, bytesMessageImpl.readUnsignedByte());
    }

    /**
     * Verify that nothing is read when {@link BytesMessage#readBytes(byte[])} is
     * called with a zero length destination array.
     */
    @Test
    public void testReadBytesWithZeroLengthDestination() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        bytesMessageImpl.reset();

        assertEquals("Did not expect any bytes to be read", 0, bytesMessageImpl.readBytes(new byte[0]));
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int))} is called
     * with a negative length that an {@link IndexOutOfBoundsException} is thrown.
     */
    @Test
    public void testReadBytesWithNegativeLengthThrowsIOOBE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        bytesMessageImpl.reset();

        try
        {
            bytesMessageImpl.readBytes(new byte[0], -1);
            fail("expected exception to be thrown");
        }
        catch(IndexOutOfBoundsException ioobe)
        {
            //expected
        }
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int))} is called
     * with a length that is greater than the size of the provided array,
     * an {@link IndexOutOfBoundsException} is thrown.
     */
    @Test
    public void testReadBytesWithLengthGreatThanArraySizeThrowsIOOBE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        bytesMessageImpl.reset();

        try
        {
            bytesMessageImpl.readBytes(new byte[1], 2);
            fail("expected exception to be thrown");
        }
        catch(IndexOutOfBoundsException ioobe)
        {
            //expected
        }
    }

    /**
     * Test that writing a short using {@link BytesMessage#writeShort(Object)}, resetting the
     * message to make it readable, and then reading back the value using
     * {@link BytesMessage#readUnsignedShort()} instead produces the expected the comes
     * from interpreting the sign bit as part of the value.
     */
    @Test
    public void testReadUnsignedShort() throws Exception
    {
        short myShort = -130;
        int expectedUnsigned = 65406;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeShort(myShort);

        bytesMessageImpl.reset();

        assertEquals("Did not get the expected unsigned value", expectedUnsigned, bytesMessageImpl.readUnsignedShort());
    }

    /**
     * Test that writing a null using {@link BytesMessage#writeObject(Object)}
     * results in a NPE being thrown.
     */
    @Test
    public void testWriteObjectWithNullThrowsNPE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            bytesMessageImpl.writeObject(null);
            fail("Expected an exception to be thrown");
        }
        catch(NullPointerException npe)
        {
            //expected
        }
    }

    /**
     * Test that writing a null using {@link BytesMessage#writeObject(Object)}
     * results in an {@link MessageFormatException} being thrown.
     */
    @Test
    public void testWriteObjectWithIllegalTypeThrowsMFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            bytesMessageImpl.writeObject(new Object());
            fail("Expected an exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    /**
     * Test that writing a boolean using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithBoolean() throws Exception
    {
        boolean myBool = true;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myBool);

        bytesMessageImpl.reset();

        assertEquals("Unexpected boolean value", myBool, bytesMessageImpl.readBoolean());
    }

    /**
     * Test that writing a byte using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithByte() throws Exception
    {
        byte myByte = 5;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myByte);

        bytesMessageImpl.reset();

        assertEquals("Unexpected byte value", myByte, bytesMessageImpl.readByte());
    }

    /**
     * Test that writing a byte[] using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithByteArray() throws Exception
    {
        byte[] myBytes = "myObjectBytes".getBytes();

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myBytes);

        bytesMessageImpl.reset();

        //retrieve the bytes, check they match
        byte[] readBytes = new byte[myBytes.length];
        assertEquals("Did not read the expected number of bytes", myBytes.length, bytesMessageImpl.readBytes(readBytes));
        assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
    }

    /**
     * Test that writing a char using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithChar() throws Exception
    {
        char myChar = 'e';

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myChar);

        bytesMessageImpl.reset();

        assertEquals("Unexpected char value", myChar, bytesMessageImpl.readChar());
    }

    /**
     * Test that writing a double using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithDouble() throws Exception
    {
        double myDouble = 1234567890123456789.1234;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myDouble);

        bytesMessageImpl.reset();

        assertEquals("Unexpected double value", myDouble, bytesMessageImpl.readDouble(), 0.0);
    }

    /**
     * Test that writing a float using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithFloat() throws Exception
    {
        float myFloat = 1.1F;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myFloat);

        bytesMessageImpl.reset();

        assertEquals("Unexpected float value", myFloat, bytesMessageImpl.readFloat(), 0.0);
    }

    /**
     * Test that writing an int using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithInt() throws Exception
    {
        int myInt = Integer.MAX_VALUE;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myInt);

        bytesMessageImpl.reset();

        assertEquals("Unexpected int value", myInt, bytesMessageImpl.readInt());
    }

    /**
     * Test that writing a long using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithLong() throws Exception
    {
        long myLong = Long.MAX_VALUE;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myLong);

        bytesMessageImpl.reset();

        assertEquals("Unexpected long value", myLong, bytesMessageImpl.readLong());
    }

    /**
     * Test that writing a short using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithShort() throws Exception
    {
        short myShort = Short.MAX_VALUE;

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myShort);

        bytesMessageImpl.reset();

        assertEquals("Unexpected short value", myShort, bytesMessageImpl.readShort());
    }

    /**
     * Test that writing a UTF string using {@link BytesMessage#writeObject(Object)}, resetting the
     * message to make it readable, and then reading back the value works as expected.
     */
    @Test
    public void testWriteObjectWithUTF() throws Exception
    {
        String myUTF = "myUTFString";

        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        bytesMessageImpl.writeObject(myUTF);

        bytesMessageImpl.reset();

        assertEquals("Unexpected UTF value", myUTF, bytesMessageImpl.readUTF());
    }

    /**
     * Test that {@link BytesMessage#writeBoolean(boolean)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteBooleanEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeBoolean(Mockito.anyBoolean());

        try
        {
            bytesMessageImpl.writeBoolean(Boolean.TRUE);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeByte(byte)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteByteEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeByte(Mockito.anyByte());

        try
        {
            bytesMessageImpl.writeByte(Byte.MAX_VALUE);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeShort(short)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteShortEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeShort(Mockito.anyShort());

        try
        {
            bytesMessageImpl.writeShort(Short.MAX_VALUE);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeChar(char)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteCharEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeChar(Mockito.anyChar());

        try
        {
            bytesMessageImpl.writeChar('f');
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeInt(int)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteIntEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeInt(Mockito.anyInt());

        try
        {
            bytesMessageImpl.writeInt(Integer.MAX_VALUE);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeLong(long)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteLongEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeLong(Mockito.anyInt());

        try
        {
            bytesMessageImpl.writeLong(Long.MAX_VALUE);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeFloat(float)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteFloatEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeFloat(Mockito.anyFloat());

        try
        {
            bytesMessageImpl.writeFloat(1.1F);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeDouble(double)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteDoubleEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeDouble(Mockito.anyDouble());

        try
        {
            bytesMessageImpl.writeDouble(1.1);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeUTF(String)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteUTFEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).writeUTF(Mockito.anyString());

        try
        {
            bytesMessageImpl.writeUTF("myUTF");
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeBytes(byte[])} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteBytesEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.writeBytes(new byte[1]);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#writeBytes(byte[], int, int)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testWriteBytesOffsetEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Class<IOException> typeToBeThrown = IOException.class;

        OutputStreamHelper mockOSH = substituteMockOutputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockOSH).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.writeBytes(new byte[1], 0, 1);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBoolean()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadBooleanEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readBoolean();

        try
        {
            bytesMessageImpl.readBoolean();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readByte()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadByteEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readByte();

        try
        {
            bytesMessageImpl.readByte();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUnsignedByte()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadUnsignedByteEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUnsignedByte();

        try
        {
            bytesMessageImpl.readUnsignedByte();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readShort()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadShortEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readShort();

        try
        {
            bytesMessageImpl.readShort();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUnsignedShort()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadUnsignedShortEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUnsignedShort();

        try
        {
            bytesMessageImpl.readUnsignedShort();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readChar()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadCharEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readChar();

        try
        {
            bytesMessageImpl.readChar();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readInt()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadIntEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readInt();

        try
        {
            bytesMessageImpl.readInt();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readLong()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadLongEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readLong();

        try
        {
            bytesMessageImpl.readLong();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readFloat()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadFloatEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readFloat();

        try
        {
            bytesMessageImpl.readFloat();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readDouble()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadDoubleEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readDouble();

        try
        {
            bytesMessageImpl.readDouble();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUTF()} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadUTFEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUTF();

        try
        {
            bytesMessageImpl.readUTF();
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBytes(byte[])} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadBytesEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.readBytes(new byte[1]);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBytes(byte[], int)} throws a {@link JMSException}
     * when the implementation encounters an {@link IOException}
     */
    @Test
    public void testReadBytesLengthEncounteringIOECausesJMSE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<IOException> typeToBeThrown = IOException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.readBytes(new byte[1], 1);
            fail("Expected an exception to be thrown");
        }
        catch(JMSException jmse)
        {
            //Expected
            assertCauseAndLinkedException(jmse, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBoolean()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadBooleanEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readBoolean();

        try
        {
            bytesMessageImpl.readBoolean();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readByte()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadByteEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readByte();

        try
        {
            bytesMessageImpl.readByte();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUnsignedByte()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadUnsignedByteEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUnsignedByte();

        try
        {
            bytesMessageImpl.readUnsignedByte();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readShort()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadShortEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readShort();

        try
        {
            bytesMessageImpl.readShort();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUnsignedShort()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadUnsignedShortEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUnsignedShort();

        try
        {
            bytesMessageImpl.readUnsignedShort();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readChar()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadCharEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readChar();

        try
        {
            bytesMessageImpl.readChar();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readInt()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadIntEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readInt();

        try
        {
            bytesMessageImpl.readInt();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readLong()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadLongEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readLong();

        try
        {
            bytesMessageImpl.readLong();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readFloat()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadFloatEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readFloat();

        try
        {
            bytesMessageImpl.readFloat();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readDouble()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadDoubleEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readDouble();

        try
        {
            bytesMessageImpl.readDouble();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readUTF()} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadUTFEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).readUTF();

        try
        {
            bytesMessageImpl.readUTF();
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBytes(byte[])} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadBytesEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.readBytes(new byte[1]);
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    /**
     * Test that {@link BytesMessage#readBytes(byte[], int)} throws a {@link MessageEOFException}
     * when the implementation encounters an {@link EOFException}
     */
    @Test
    public void testReadBytesLengthEncounteringEOFECausesJMSMEOFE() throws Exception
    {
        BytesMessageImpl bytesMessageImpl = new BytesMessageImpl(_mockAmqpMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Class<EOFException> typeToBeThrown = EOFException.class;

        InputStreamHelper mockISH = substituteMockInputStreamHelper(bytesMessageImpl);
        Mockito.doThrow(typeToBeThrown).when(mockISH).read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());

        try
        {
            bytesMessageImpl.readBytes(new byte[1], 1);
            fail("Expected an exception to be thrown");
        }
        catch(MessageEOFException meofe)
        {
            //Expected
            assertCauseAndLinkedException(meofe, typeToBeThrown);
        }
    }

    private void assertCauseAndLinkedException(JMSException jmse, Class<?> typeToBeThrown)
    {
        Exception linked = jmse.getLinkedException();
        Throwable cause = jmse.getCause();

        if(typeToBeThrown == null)
        {
            assertNull("Linked exception was not expected", linked);
            assertNull("Cause was not expected", cause);
        }
        else
        {
            assertNotNull("Linked exception was not set", linked);
            assertNotNull("Cause was not set", cause);

            assertEquals("Unexpected linked exception type", typeToBeThrown, linked.getClass());
            assertEquals("Unexpected cause type", typeToBeThrown, cause.getClass());
        }
    }

    private InputStreamHelper substituteMockInputStreamHelper(BytesMessageImpl bytesMessage) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        InputStreamHelper mock = Mockito.mock(InputStreamHelper.class);

        Field ishField = bytesMessage.getClass().getDeclaredField(INPUT_STREAM_HELPER);
        ishField.setAccessible(true);
        ishField.set(bytesMessage, mock);

        return mock;
    }

    private OutputStreamHelper substituteMockOutputStreamHelper(BytesMessageImpl bytesMessage) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        OutputStreamHelper mock = Mockito.mock(OutputStreamHelper.class);

        Field oshField = bytesMessage.getClass().getDeclaredField(OUTPUT_STREAM_HELPER);
        oshField.setAccessible(true);
        oshField.set(bytesMessage, mock);

        return mock;
    }

}
