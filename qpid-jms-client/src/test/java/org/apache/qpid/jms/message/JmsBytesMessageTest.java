/**
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsDefaultMessageFactory;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.facade.defaults.JmsDefaultBytesMessageFacade;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Test;

/**
 * Test for JMS Spec compliance for the JmsBytesMessage class using the default message facade.
 */
public class JmsBytesMessageTest {

    private static final int END_OF_STREAM = -1;

    private final JmsMessageFactory factory = new JmsDefaultMessageFactory();

    /**
     * Test that calling {@link BytesMessage#getBodyLength()} on a new message which has been
     * populated and {@link BytesMessage#reset()} causes the length to be reported correctly.
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
     */
    @Test(expected = MessageNotReadableException.class)
    public void testGetBodyLengthOnNewMessageThrowsMessageNotReadableException() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.getBodyLength();
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithNoBodyReturnsEOS() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.onSend();
        //verify attempting to read bytes returns -1, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesMessage.readBytes(new byte[1]));
    }

    @Test
    public void testReadBytesUsingReceivedMessageWithBodyReturnsBytes() throws Exception {
        Buffer content = new Buffer("myBytesData".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();

        // retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[content.length];
        bytesMessage.readBytes(receivedBytes);
        assertTrue(Arrays.equals(content.data, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned",
                     END_OF_STREAM, bytesMessage.readBytes(new byte[1]));

        assertEquals("Message reports unexpected length", content.length, bytesMessage.getBodyLength());
    }

    /**
     * Test that attempting to write bytes to a received message (without calling {@link BytesMessage#clearBody()} first)
     * causes a {@link MessageNotWriteableException} to be thrown due to being read-only.
     */
    @Test(expected = MessageNotWriteableException.class)
    public void testReceivedBytesMessageThrowsMessageNotWriteableExceptionOnWriteBytes() throws Exception {
        Buffer content = new Buffer("myBytesData".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();
        bytesMessage.writeBytes(content.data);
    }

    /**
     * Test that attempting to read bytes from a new message (without calling {@link BytesMessage#reset()} first) causes a
     * {@link MessageNotReadableException} to be thrown due to being write-only.
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
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageMakesMessageWritable() throws Exception {
        Buffer content = new Buffer("myBytesData".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();
        assertTrue("Message should not be writable", bytesMessage.isReadOnlyBody());
        bytesMessage.clearBody();
        assertFalse("Message should be writable", bytesMessage.isReadOnlyBody());
    }

    /**
     * Test that calling {@link BytesMessage#clearBody()} of a received message
     * causes the body of the underlying {@link AmqpBytesMessage} to be emptied.
     */
    @Test
    public void testClearBodyOnReceivedBytesMessageClearsUnderlyingMessageBody() throws Exception {
        Buffer content = new Buffer("myBytesData".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();

        assertNotNull("Expected message content but none was present", facade.getContent());
        bytesMessage.clearBody();
        assertNull("Expected no message content but was present", facade.getContent());
    }

    /**
     * Test that attempting to call {@link BytesMessage#getBodyLength()} on a received message after calling
     * {@link BytesMessage#clearBody()} causes {@link MessageNotReadableException} to be thrown due to being write-only.
     */
    @Test
    public void testGetBodyLengthOnClearedReceivedMessageThrowsMessageNotReadableException() throws Exception {
        Buffer content = new Buffer("myBytesData".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();
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
     */
    @Test
    public void testResetOnReceivedBytesMessageResetsMarker() throws Exception {
        Buffer content = new Buffer("resetTestBytes".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);
        bytesMessage.onSend();

        // retrieve a few bytes, check they match the first few expected bytes
        byte[] partialBytes = new byte[3];
        bytesMessage.readBytes(partialBytes);
        byte[] partialOriginalBytes = Arrays.copyOf(content.data, 3);
        assertTrue(Arrays.equals(partialOriginalBytes, partialBytes));

        bytesMessage.reset();

        // retrieve all the expected bytes, check they match
        byte[] resetBytes = new byte[content.length];
        bytesMessage.readBytes(resetBytes);
        assertTrue(Arrays.equals(content.data, resetBytes));
    }

    /**
     * Test that calling {@link BytesMessage#reset()} on a new message which has been populated
     * causes the marker to be reset and makes the message read-only
     */
    @Test
    public void testResetOnNewlyPopulatedBytesMessageResetsMarkerAndMakesReadable() throws Exception {
        Buffer content = new Buffer("newResetTestBytes".getBytes());
        JmsDefaultBytesMessageFacade facade = new JmsDefaultBytesMessageFacade();
        facade.setContent(content);

        JmsBytesMessage bytesMessage = new JmsBytesMessage(facade);

        assertFalse("Message should be writable", bytesMessage.isReadOnlyBody());
        bytesMessage.writeBytes(content.data);
        bytesMessage.reset();
        assertTrue("Message should not be writable", bytesMessage.isReadOnlyBody());

        // retrieve the bytes, check they match
        byte[] resetBytes = new byte[content.length];
        bytesMessage.readBytes(resetBytes);
        assertTrue(Arrays.equals(content.data, resetBytes));
    }

    /**
     * Verify that nothing is read when {@link BytesMessage#readBytes(byte[])} is
     * called with a zero length destination array.
     */
    @Test
    public void testReadBytesWithZeroLengthDestination() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.reset();
        assertEquals("Did not expect any bytes to be read", 0, bytesMessage.readBytes(new byte[0]));
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int))} is called
     * with a negative length that an {@link IndexOutOfBoundsException} is thrown.
     */
    @Test(expected=IndexOutOfBoundsException.class)
    public void testReadBytesWithNegativeLengthThrowsIOOBE() throws Exception
    {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.reset();
        bytesMessage.readBytes(new byte[0], -1);
    }

    /**
     * Verify that when {@link BytesMessage#readBytes(byte[], int))} is called
     * with a length that is greater than the size of the provided array,
     * an {@link IndexOutOfBoundsException} is thrown.
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
     */
    @Test(expected=NullPointerException.class)
    public void testWriteObjectWithNullThrowsNPE() throws Exception {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        bytesMessage.writeObject(null);
    }

    /**
     * Test that writing a null using {@link BytesMessage#writeObject(Object)}
     * results in an {@link MessageFormatException} being thrown.
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
            msg.writeObject(new Float(3.3f));
            msg.writeObject(new Double(3.3));
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
    public void testClearBody() throws JMSException {
        JmsBytesMessage bytesMessage = factory.createBytesMessage();
        try {
            bytesMessage.writeInt(1);
            bytesMessage.clearBody();
            assertFalse(bytesMessage.isReadOnlyBody());
            bytesMessage.writeInt(1);
            bytesMessage.readInt();
        } catch (MessageNotReadableException mnwe) {
        } catch (MessageNotWriteableException mnwe) {
            assertTrue(false);
        }
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
}
