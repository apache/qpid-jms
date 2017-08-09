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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestBytesMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for JMS Spec compliance for the JmsBytesMessage class using the default message facade.
 */
public class JmsBytesMessageTest {

    private static final int END_OF_STREAM = -1;

    private final JmsMessageFactory factory = new JmsTestMessageFactory();

    @Test
    public void testToString() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        assertTrue(bytesMessage.toString().startsWith("JmsBytesMessage"));
    }

    /**
     * Test that calling {@link BytesMessage#getBodyLength()} on a new message which has been
     * populated and {@link BytesMessage#reset()} causes the length to be reported correctly.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testResetOnNewlyPopulatedBytesMessageUpdatesBodyLength() throws Exception {
        byte[] bytes = "newResetTestBytes".getBytes();
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.writeBytes(bytes);
        bytesMessage.reset();
        assertEquals("Message reports unexpected length", bytes.length, bytesMessage.getBodyLength());
    }

    /**
     * Test that attempting to call {@link BytesMessage#getBodyLength()} on a new message causes
     * a {@link MessageNotReadableException} to be thrown due to being write-only.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected = MessageNotReadableException.class)
    public void testGetBodyLengthOnNewMessageThrowsMessageNotReadableException() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.getBodyLength();
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithNoBodyReturnsEOS() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.onDispatch();
        //verify attempting to read bytes returns -1, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesMessage.readBytes(new byte[1]));
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithBodyReturnsBytes() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();

        // retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[content.length];
        bytesMessage.readBytes(receivedBytes);
        assertTrue(Arrays.equals(content, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned",
                     END_OF_STREAM, bytesMessage.readBytes(new byte[1]));

        assertEquals("Message reports unexpected length", content.length, bytesMessage.getBodyLength());
    }

    /**
     * Test that attempting to write bytes to a received message (without calling {@link BytesMessage#clearBody()} first)
     * causes a {@link MessageNotWriteableException} to be thrown due to being read-only.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected = MessageNotWriteableException.class)
    public void testReceivedBytesMessageThrowsMessageNotWriteableExceptionOnWriteBytes() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();
        bytesMessage.writeBytes(content);
    }

    /**
     * Test that attempting to read bytes from a new message (without calling {@link BytesMessage#reset()} first) causes a
     * {@link MessageNotReadableException} to be thrown due to being write-only.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected = MessageNotReadableException.class)
    public void testNewBytesMessageThrowsMessageNotReadableOnReadBytes() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        byte[] receivedBytes = new byte[1];
        bytesMessage.readBytes(receivedBytes);
    }

    /**
     * Test that calling {@link BytesMessage#clearBody()} causes a received
     * message to become writable
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageMakesMessageWritable() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();
        assertTrue("Message should not be writable", bytesMessage.isReadOnlyBody());
        bytesMessage.clearBody();
        assertFalse("Message should be writable", bytesMessage.isReadOnlyBody());
    }

    /**
     * Test that calling {@link BytesMessage#clearBody()} of a received message
     * causes the facade input stream to be empty and body length to return 0.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageClearsFacadeInputStream() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();

        assertTrue("Expected message content but none was present", facade.getBodyLength() > 0);
        assertEquals("Expected data from facade", 1, facade.getInputStream().read(new byte[1]));
        bytesMessage.clearBody();
        assertTrue("Expected no message content from facade", facade.getBodyLength() == 0);
        assertEquals("Expected no data from facade, but got some", END_OF_STREAM, facade.getInputStream().read(new byte[1]));
    }

    /**
     * Test that attempting to call {@link BytesMessage#getBodyLength()} on a received message after calling
     * {@link BytesMessage#clearBody()} causes {@link MessageNotReadableException} to be thrown due to being write-only.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetBodyLengthOnClearedReceivedMessageThrowsMessageNotReadableException() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();
        assertEquals("Unexpected message length", content.length, bytesMessage.getBodyLength());
        bytesMessage.clearBody();

        try {
            bytesMessage.getBodyLength();
            fail("expected exception to be thrown");
        } catch (MessageNotReadableException mnre) {
            // expected
        }
    }

    /**
     * Test that calling {@link BytesMessage#reset()} causes a write-only
     * message to become read-only
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testResetOnReceivedBytesMessageResetsMarker() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onDispatch();

        // retrieve a few bytes, check they match the first few expected bytes
        byte[] partialBytes = new byte[3];
        bytesMessage.readBytes(partialBytes);
        byte[] partialOriginalBytes = Arrays.copyOf(content, 3);
        assertTrue(Arrays.equals(partialOriginalBytes, partialBytes));

        bytesMessage.reset();

        // retrieve all the expected bytes, check they match
        byte[] resetBytes = new byte[content.length];
        bytesMessage.readBytes(resetBytes);
        assertTrue(Arrays.equals(content, resetBytes));
    }

    /**
     * Test that calling {@link BytesMessage#reset()} on a new message which has been populated
     * causes the marker to be reset and makes the message read-only
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testResetOnNewlyPopulatedBytesMessageResetsMarkerAndMakesReadable() throws Exception {
        byte[] content = "myBytesData".getBytes();
        JmsTestBytesMessageFacade facade = new JmsTestBytesMessageFacade(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);

        assertFalse("Message should be writable", bytesMessage.isReadOnlyBody());
        bytesMessage.writeBytes(content);
        bytesMessage.reset();
        assertTrue("Message should not be writable", bytesMessage.isReadOnlyBody());

        // retrieve the bytes, check they match
        byte[] resetBytes = new byte[content.length];
        bytesMessage.readBytes(resetBytes);
        assertTrue(Arrays.equals(content, resetBytes));
    }

    /**
     * Verify that nothing is read when {@link BytesMessage#readBytes(byte[])} is
     * called with a zero length destination array.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testReadBytesWithZeroLengthDestination() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.reset();
        assertEquals("Did not expect any bytes to be read", 0, bytesMessage.readBytes(new byte[0]));
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int)} is called
     * with a negative length that an {@link IndexOutOfBoundsException} is thrown.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=IndexOutOfBoundsException.class)
    public void testReadBytesWithNegativeLengthThrowsIOOBE() throws Exception
    {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.reset();
        bytesMessage.readBytes(new byte[0], -1);
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int)} is called
     * with a length that is greater than the size of the provided array,
     * an {@link IndexOutOfBoundsException} is thrown.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=IndexOutOfBoundsException.class)
    public void testReadBytesWithLengthGreatThanArraySizeThrowsIOOBE() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.reset();
        bytesMessage.readBytes(new byte[1], 2);
    }

    /**
     * Test that writing a null using {@link BytesMessage#writeObject(Object)}
     * results in a NPE being thrown.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=NullPointerException.class)
    public void testWriteObjectWithNullThrowsNPE() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.writeObject(null);
    }

    /**
     * Test that writing a null using {@link BytesMessage#writeObject(Object)}
     * results in an {@link MessageFormatException} being thrown.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=MessageFormatException.class)
    public void testWriteObjectWithIllegalTypeThrowsMFE() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.writeObject(new Object());
    }

    @Test
    public void testGetBodyLength() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        int len = 10;
        for (int i = 0; i < len; i++) {
            msg.writeLong(5L);
        }

        msg.reset();
        assertTrue(msg.getBodyLength() == (len * 8));
    }

    @Test
    public void testReadBoolean() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeBoolean(true);
        msg.reset();
        assertTrue(msg.readBoolean());
    }

    @Test
    public void testReadByte() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeByte((byte) 2);
        msg.reset();
        assertTrue(msg.readByte() == 2);
    }

    @Test
    public void testReadUnsignedByte() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeByte((byte) 2);
        msg.reset();
        assertTrue(msg.readUnsignedByte() == 2);
    }

    @Test
    public void testReadShort() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeShort((short) 3000);
        msg.reset();
        assertTrue(msg.readShort() == 3000);
    }

    @Test
    public void testReadUnsignedShort() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeShort((short) 3000);
        msg.reset();
        assertTrue(msg.readUnsignedShort() == 3000);
    }

    @Test
    public void testReadChar() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeChar('a');
        msg.reset();
        assertTrue(msg.readChar() == 'a');
    }

    @Test
    public void testReadInt() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeInt(3000);
        msg.reset();
        assertTrue(msg.readInt() == 3000);
    }

    @Test
    public void testReadLong() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeLong(3000);
        msg.reset();
        assertTrue(msg.readLong() == 3000);
    }

    @Test
    public void testReadFloat() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeFloat(3.3f);
        msg.reset();
        assertTrue(msg.readFloat() == 3.3f);
    }

    @Test
    public void testReadDouble() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        msg.writeDouble(3.3d);
        msg.reset();
        assertTrue(msg.readDouble() == 3.3d);
    }

    @Test
    public void testReadUTF() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        String str = "this is a test";
        msg.writeUTF(str);
        msg.reset();
        assertTrue(msg.readUTF().equals(str));
    }

    @Test
    public void testReadBytesbyteArray() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        byte[] data = new byte[50];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        msg.writeBytes(data);
        msg.reset();
        byte[] test = new byte[data.length];
        msg.readBytes(test);
        for (int i = 0; i < test.length; i++) {
            assertTrue(test[i] == i);
        }
    }

    @Test
    public void testWriteObject() throws JMSException {
        JmsBytesMessage msg = factory.createBytesMessage();
        try {
            msg.writeObject("fred");
            msg.writeObject(Boolean.TRUE);
            msg.writeObject(Character.valueOf('q'));
            msg.writeObject(Byte.valueOf((byte) 1));
            msg.writeObject(Short.valueOf((short) 3));
            msg.writeObject(Integer.valueOf(3));
            msg.writeObject(Long.valueOf(300L));
            msg.writeObject(Float.valueOf(3.3f));
            msg.writeObject(Double.valueOf(3.3));
            msg.writeObject(new byte[3]);
        } catch (MessageFormatException mfe) {
            fail("objectified primitives should be allowed");
        }
        try {
            msg.writeObject(new Object());
            fail("only objectified primitives are allowed");
        } catch (MessageFormatException mfe) {
        }
    }

    @Test
    public void testClearBodyOnNewMessage() throws JMSException {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.writeInt(1);
        bytesMessage.clearBody();
        assertFalse(bytesMessage.isReadOnlyBody());
        bytesMessage.reset();
        assertEquals(0, bytesMessage.getBodyLength());
    }

    @Test
    public void testReset() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        try {
            message.writeDouble(24.5);
            message.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        message.reset();
        try {
            assertTrue(message.isReadOnlyBody());
            assertEquals(message.readDouble(), 24.5, 0);
            assertEquals(message.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
        try {
            message.writeInt(33);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        try {
            message.writeBoolean(true);
            message.writeByte((byte) 1);
            message.writeByte((byte) 1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float) 1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short) 1);
            message.writeShort((short) 1);
            message.writeUTF("utfstring");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        message.reset();
        try {
            message.readBoolean();
            message.readByte();
            message.readUnsignedByte();
            message.readBytes(new byte[1]);
            message.readBytes(new byte[2], 2);
            message.readChar();
            message.readDouble();
            message.readFloat();
            message.readInt();
            message.readLong();
            message.readUTF();
            message.readShort();
            message.readUnsignedShort();
            message.readUTF();
        } catch (MessageNotReadableException mnwe) {
            fail("Should be readable");
        }
        try {
            message.writeBoolean(true);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeByte((byte) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[3], 0, 2);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeChar('a');
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeDouble(1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeFloat((float) 1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeInt(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeLong(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeObject("stringobj");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeShort((short) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeUTF("utfstring");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        message.clearBody();
        try {
            message.writeBoolean(true);
            message.writeByte((byte) 1);
            message.writeByte((byte) 1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float) 1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short) 1);
            message.writeShort((short) 1);
            message.writeUTF("utfstring");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        try {
            message.readBoolean();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException mnwe) {
        }
        try {
            message.readByte();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUnsignedByte();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[2], 2);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readChar();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readDouble();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readFloat();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readInt();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readLong();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUTF();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readShort();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUnsignedShort();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readUTF();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
    }

    //---------- Test that errors are trapped correctly ----------------------//

    @Test
    public void testReadMethodsCaptureEOFExceptionThrowsMessageEOFEx() throws Exception {
        JmsBytesMessageFacade facade = Mockito.mock(JmsBytesMessageFacade.class);
        InputStream bytesIn = Mockito.mock(InputStream.class);
        Mockito.when(facade.getInputStream()).thenReturn(bytesIn);

        Mockito.when(bytesIn.read()).thenThrow(new EOFException());
        Mockito.when(bytesIn.read(Mockito.any(byte[].class))).thenThrow(new EOFException());
        Mockito.when(bytesIn.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new EOFException());

        JmsBytesMessage message = new JmsBytesMessage(facade);
        message.reset();

        try {
            message.readBoolean();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readByte();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readBytes(new byte[10]);
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readBytes(new byte[10], 10);
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readChar();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readDouble();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readFloat();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readInt();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readLong();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readShort();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readUnsignedByte();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readUnsignedShort();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }

        try {
            message.readUTF();
        } catch (MessageEOFException ex) {
            assertTrue(ex.getCause() instanceof EOFException);
        }
    }

    @Test
    public void testReadMethodsCaptureIOExceptionThrowsJMSEx() throws Exception {
        JmsBytesMessageFacade facade = Mockito.mock(JmsBytesMessageFacade.class);
        InputStream bytesIn = Mockito.mock(InputStream.class);
        Mockito.when(facade.getInputStream()).thenReturn(bytesIn);

        Mockito.when(bytesIn.read()).thenThrow(new IOException());
        Mockito.when(bytesIn.read(Mockito.any(byte[].class))).thenThrow(new IOException());
        Mockito.when(bytesIn.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new IOException());

        JmsBytesMessage message = new JmsBytesMessage(facade);
        message.reset();

        try {
            message.readBoolean();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readByte();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readBytes(new byte[10]);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readBytes(new byte[10], 10);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readChar();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readDouble();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readFloat();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readInt();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readLong();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readShort();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readUnsignedByte();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readUnsignedShort();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.readUTF();
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void testWriteMethodsCaptureIOExceptionThrowsJMSEx() throws Exception {
        JmsBytesMessageFacade facade = Mockito.mock(JmsBytesMessageFacade.class);
        OutputStream bytesOut = Mockito.mock(OutputStream.class);
        Mockito.when(facade.getOutputStream()).thenReturn(bytesOut);

        Mockito.doThrow(new IOException()).when(bytesOut).write(Mockito.anyByte());
        Mockito.doThrow(new IOException()).when(bytesOut).write(Mockito.any(byte[].class));
        Mockito.doThrow(new IOException()).when(bytesOut).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        JmsBytesMessage message = new JmsBytesMessage(facade);

        try {
            message.writeBoolean(false);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeByte((byte) 128);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeBytes(new byte[10]);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeBytes(new byte[10], 0, 10);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeChar('a');
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeDouble(100.0);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeFloat(10.2f);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeInt(125);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeLong(65536L);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeObject("");
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeShort((short) 32768);
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }

        try {
            message.writeUTF("");
        } catch (JMSException ex) {
            assertTrue(ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void testGetBodyThrowsMessageFormatException() throws JMSException {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.setStringProperty("property", "value");
        bytesMessage.writeByte((byte) 1);
        bytesMessage.writeInt(22);

        try {
            bytesMessage.getBody(StringBuffer.class);
            fail("should have thrown MessageFormatException");
        } catch (MessageFormatException mfe) {
        } catch (Exception e) {
            fail("should have thrown MessageFormatException");
        }

        try {
            bytesMessage.getBody(String.class);
            fail("should have thrown MessageFormatException");
        } catch (MessageFormatException mfe) {
        } catch (Exception e) {
            fail("should have thrown MessageFormatException");
        }

        try {
            bytesMessage.getBody(Map.class);
            fail("should have thrown MessageFormatException");
        } catch (MessageFormatException mfe) {
        } catch (Exception e) {
            fail("should have thrown MessageFormatException");
        }

        try {
            bytesMessage.getBody(List.class);
            fail("should have thrown MessageFormatException");
        } catch (MessageFormatException mfe) {
        } catch (Exception e) {
            fail("should have thrown MessageFormatException");
        }

        try {
            bytesMessage.getBody(Array.class);
            fail("should have thrown MessageFormatException");
        } catch (MessageFormatException mfe) {
        } catch (Exception e) {
            fail("should have thrown MessageFormatException");
        }

        byte[] read1 = bytesMessage.getBody(byte[].class);
        assertNotNull(read1);

        byte[] read2 = (byte[]) bytesMessage.getBody(Object.class);
        assertNotNull(read2);
    }

    //---------- Test for misc message methods -------------------------------//

    @Test
    public void testHashCode() throws Exception {
        String messageId = "ID:SOME-ID:0:1:1";
        JmsBytesMessage message = factory.createBytesMessage();
        message.setJMSMessageID(messageId);
        assertEquals(message.getJMSMessageID().hashCode(), messageId.hashCode());
        assertEquals(message.hashCode(), messageId.hashCode());
    }

    @Test
    public void testEqualsObject() throws Exception {
        String messageId = "ID:SOME-ID:0:1:1";
        JmsBytesMessage message1 = factory.createBytesMessage();
        JmsBytesMessage message2 = factory.createBytesMessage();
        message1.setJMSMessageID(messageId);
        assertTrue(!message1.equals(message2));
        assertTrue(!message2.equals(message1));
        message2.setJMSMessageID(messageId);
        assertTrue(message1.equals(message2));
        assertTrue(message2.equals(message1));
        message2.setJMSMessageID(messageId + "More");
        assertTrue(!message1.equals(message2));
        assertTrue(!message2.equals(message1));

        assertTrue(message1.equals(message1));
        assertFalse(message1.equals(null));
        assertFalse(message1.equals(""));
    }
}
