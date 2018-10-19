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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * Tests for class AmqpJmsBytesMessageFacade
 */
public class AmqpJmsBytesMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    private static final int END_OF_STREAM = -1;

    // ---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageDoesNotContainMessageTypeAnnotation() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        MessageAnnotations annotations = amqpBytesMessageFacade.getMessageAnnotations();

        assertNull("MessageAnnotations section was present", annotations);

        assertEquals(JMS_BYTES_MESSAGE, amqpBytesMessageFacade.getJmsMsgType());
    }

    @Test
    public void testGetInputStreamWithNewMessageReturnsEmptyInputStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        InputStream byteArrayInputStream = amqpBytesMessageFacade.getInputStream();
        assertNotNull(byteArrayInputStream);

        // try to read a byte, it should return -1 bytes read, i.e EOS.
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, byteArrayInputStream.read(new byte[1]));
    }

    @Test
    public void testGetBodyLengthUsingNewMessage() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertEquals("Message reports unexpected length", 0, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testNewMessageHasContentType() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        Properties properties = amqpBytesMessageFacade.getProperties();
        assertNotNull(properties);
        assertNotNull(properties.getContentType());

        String contentType = properties.getContentType().toString();
        assertNotNull("content type should be set", contentType);
        assertEquals("application/octet-stream", contentType);
    }

    // ---------- test for normal message operations -------------------------//

    @Test
    public void testGetBodyLengthUsingPopulatedMessageToSend() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        byte[] bytes = "myBytes".getBytes();
        OutputStream os = amqpBytesMessageFacade.getOutputStream();
        os.write(bytes);

        amqpBytesMessageFacade.reset();

        assertEquals("Message reports unexpected length", bytes.length, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testGetOutputStreamReturnsSameStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        OutputStream os1 = amqpBytesMessageFacade.getOutputStream();
        OutputStream os2 = amqpBytesMessageFacade.getOutputStream();
        assertSame("Got different output streams", os1, os2);
    }

    @Test
    public void testGetInputStreamReturnsSameStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        InputStream is1 = amqpBytesMessageFacade.getInputStream();
        InputStream is2 = amqpBytesMessageFacade.getInputStream();
        assertSame("Got different input streams", is1, is2);
    }

    /**
     * Test that copying a new messages which has been written to creates the
     * non-empty data section of the underlying message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testCopyOnPopulatedNewMessageCreatesDataSection() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);

        byte[] bytes = "myBytes".getBytes();
        OutputStream os = amqpBytesMessageFacade.getOutputStream();
        os.write(bytes);

        AmqpJmsBytesMessageFacade copy = amqpBytesMessageFacade.copy();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), bytes.length);
        assertDataBodyAsExpected(copy.getBody(), bytes.length);
    }

    /**
     * Test that copying a new messages which has not been written to creates the
     * (empty) data section of the underlying message on the copy.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testCopyOfNewMessageDoesNotCreateDataSection() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);

        AmqpJmsBytesMessageFacade copy = amqpBytesMessageFacade.copy();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);
        assertDataBodyAsExpected(copy.getBody(), 0);
    }

    /**
     * Test that copying a new messages which has a body and the copy has getOutputStream
     * called which clears the message body doesn't affect the original.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetOutputStreamOnCopiedMessageLeavesOriginalUntouched() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);

        byte[] bytes = "myBytes".getBytes();
        OutputStream os = amqpBytesMessageFacade.getOutputStream();
        os.write(bytes);

        AmqpJmsBytesMessageFacade copy = amqpBytesMessageFacade.copy();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), bytes.length);
        assertDataBodyAsExpected(copy.getBody(), bytes.length);

        copy.getOutputStream();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), bytes.length);
        assertDataBodyAsExpected(copy.getBody(), 0);
    }

    @Test
    public void testClearBodySetsBodyLength0AndCausesEmptyInputStream() throws Exception {
        byte[] bytes = "myBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        amqpBytesMessageFacade.clearBody();

        assertTrue("Expected no message content from facade", amqpBytesMessageFacade.getBodyLength() == 0);
        assertEquals("Expected no data from facade, but got some", END_OF_STREAM, amqpBytesMessageFacade.getInputStream().read(new byte[1]));

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);
    }

    @Test
    public void testClearBodyWithExistingInputStream() throws Exception {
        byte[] bytes = "myBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        @SuppressWarnings("unused")
        InputStream unused = amqpBytesMessageFacade.getInputStream();

        amqpBytesMessageFacade.clearBody();

        assertEquals("Expected no data from facade, but got some", END_OF_STREAM, amqpBytesMessageFacade.getInputStream().read(new byte[1]));

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);
    }

    @Test
    public void testClearBodyWithExistingOutputStream() throws Exception {
        byte[] bytes = "myBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        @SuppressWarnings("unused")
        OutputStream unused = amqpBytesMessageFacade.getOutputStream();

        amqpBytesMessageFacade.clearBody();

        assertDataBodyAsExpected(amqpBytesMessageFacade.getBody(), 0);
    }

    @Test
    public void testGetInputStreamThrowsJMSISEWhenFacadeBeingWrittenTo() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        amqpBytesMessageFacade.getOutputStream();
        try {
            amqpBytesMessageFacade.getInputStream();
            fail("expected exception not thrown");
        } catch (javax.jms.IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testGetOutputStreamThrowsJMSISEWhenFacadeBeingReadFrom() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        amqpBytesMessageFacade.getInputStream();
        try {
            amqpBytesMessageFacade.getOutputStream();
            fail("expected exception not thrown");
        } catch (javax.jms.IllegalStateException ise) {
            // expected
        }
    }

    // ---------- test handling of received messages -------------------------//

    @Test
    public void testGetInputStreamUsingReceivedMessageWithNoBodySectionReturnsEmptyInputStream() throws Exception {
        Message message = Message.Factory.create();
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        InputStream byteArrayInputStream = amqpBytesMessageFacade.getInputStream();
        assertNotNull(byteArrayInputStream);

        // try to read a byte, it should return -1 bytes read, i.e EOS.
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, byteArrayInputStream.read(new byte[1]));
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithDataSectionContainingNonZeroLengthBinary() throws Exception {
        Message message = Message.Factory.create();
        int length = 5;
        message.setBody(new Data(new Binary(new byte[length])));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Message reports unexpected length", length, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingNonZeroLengthBinary() throws Exception {
        Message message = Message.Factory.create();
        int length = 10;
        message.setBody(new AmqpValue(new Binary(new byte[length])));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Message reports unexpected length", length, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingZeroLengthBinary() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(new Binary(new byte[0])));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Message reports unexpected length", 0, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingNull() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(null));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Message reports unexpected length", 0, amqpBytesMessageFacade.getBodyLength());
    }

    @Test
    public void testInputStreamUsingReceivedMessageWithAmqpValueSectionContainingBinary() throws Exception {
        byte[] bytes = "myBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();

        // retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesStream.read(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testInputStreamUsingReceivedMessageWithDataSection() throws Exception {
        byte[] bytes = "myBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bytes)));

        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();
        assertNotNull(bytesStream);

        // retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesStream.read(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testGetInputStreamUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyStream() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new Data(null));

        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();
        assertNotNull(bytesStream);

        assertEquals("Message reports unexpected length", 0, amqpBytesMessageFacade.getBodyLength());
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testGetMethodsWithNonAmqpValueNonDataSectionThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpBytesMessageFacade.getInputStream();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }

        try {
            amqpBytesMessageFacade.getBodyLength();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testGetMethodsWithAmqpValueContainingNonNullNonBinaryValueThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(true));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpBytesMessageFacade.getInputStream();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }

        try {
            amqpBytesMessageFacade.getBodyLength();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    /**
     * Test that setting bytes on a received message results in the expected content in the body section
     * of the underlying message and returned by a new InputStream requested from the message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetGetBodyOnReceivedMessage() throws Exception {
        byte[] orig = "myOrigBytes".getBytes();
        byte[] replacement = "myReplacementBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(orig)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        OutputStream os = amqpBytesMessageFacade.getOutputStream();
        os.write(replacement);

        amqpBytesMessageFacade.reset();

        // Retrieve the new Binary from the underlying message, check they match
        // (the backing arrays may be different length so not checking arrayEquals)
        Data body = (Data) amqpBytesMessageFacade.getBody();
        assertEquals("Underlying message data section did not contain the expected bytes", new Binary(replacement), body.getValue());

        assertEquals("expected body length to match replacement bytes", replacement.length, amqpBytesMessageFacade.getBodyLength());

        // retrieve the new bytes via an InputStream, check they match expected
        byte[] receivedBytes = new byte[replacement.length];
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();
        bytesStream.read(receivedBytes);
        assertTrue("Retrieved bytes from input steam did not match expected bytes", Arrays.equals(replacement, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testClearBodyHandlesErrorFromOutputStream() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        OutputStream outputStream = amqpBytesMessageFacade.getOutputStream();
        outputStream = substituteMockOutputStream(amqpBytesMessageFacade);
        Mockito.doThrow(new IOException()).when(outputStream).close();

        amqpBytesMessageFacade.clearBody();
    }

    @Test
    public void testClearBodyHandlesErrorFromInputStream() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        InputStream inputStream = amqpBytesMessageFacade.getInputStream();
        inputStream = substituteMockInputStream(amqpBytesMessageFacade);
        Mockito.doThrow(new IOException()).when(inputStream).close();

        amqpBytesMessageFacade.clearBody();
    }

    @Test
    public void testResetHandlesErrorFromOutputStream() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        OutputStream outputStream = amqpBytesMessageFacade.getOutputStream();
        outputStream = substituteMockOutputStream(amqpBytesMessageFacade);
        Mockito.doThrow(new IOException()).when(outputStream).close();

        amqpBytesMessageFacade.reset();
    }

    @Test
    public void testResetHandlesErrorFromInputStream() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        InputStream inputStream = amqpBytesMessageFacade.getInputStream();
        inputStream = substituteMockInputStream(amqpBytesMessageFacade);
        Mockito.doThrow(new IOException()).when(inputStream).close();

        amqpBytesMessageFacade.reset();
    }

    //--------- hasBody tests ---------------

    @Test
    public void testHasBodyOnNewMessage() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertFalse(amqpBytesMessageFacade.hasBody());
    }

    @Test
    public void testHasBodyWithContent() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpBytesMessageFacade.hasBody());
    }

    @Test
    public void testHasBodyAfterClear() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpBytesMessageFacade.hasBody());

        amqpBytesMessageFacade.clearBody();

        assertFalse(amqpBytesMessageFacade.hasBody());
    }

    @Test
    public void testHasBodyWithActiveInputStream() throws Exception {
        byte[] bodyBytes = "myOrigBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(bodyBytes)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpBytesMessageFacade.hasBody());

        amqpBytesMessageFacade.getInputStream();

        assertTrue(amqpBytesMessageFacade.hasBody());
    }

    @Test
    public void testHasBodyWithActiveOutputStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        assertFalse(amqpBytesMessageFacade.hasBody());

        OutputStream output = amqpBytesMessageFacade.getOutputStream();

        assertFalse(amqpBytesMessageFacade.hasBody());

        output.write(1);

        // Body exists after some data written.
        assertTrue(amqpBytesMessageFacade.hasBody());
        amqpBytesMessageFacade.reset();
        assertTrue(amqpBytesMessageFacade.hasBody());
    }

    //--------- utility methods ----------

    private void assertDataBodyAsExpected(Section body, int length) {
        assertNotNull("Expected body section to be present", body);
        assertEquals("Unexpected body section type", Data.class, body.getClass());
        Binary value = ((Data) body).getValue();
        assertNotNull(value);
        assertEquals("Unexpected body length", length, value.getLength());
    }

    private InputStream substituteMockInputStream(AmqpJmsBytesMessageFacade bytesMessage) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        InputStream mock = Mockito.mock(ByteBufInputStream.class);

        Field ishField = bytesMessage.getClass().getDeclaredField("bytesIn");
        ishField.setAccessible(true);
        ishField.set(bytesMessage, mock);

        return mock;
    }

    private OutputStream substituteMockOutputStream(AmqpJmsBytesMessageFacade bytesMessage) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        ByteBufOutputStream mock = Mockito.mock(ByteBufOutputStream.class);
        Mockito.when(mock.buffer()).thenReturn(Unpooled.EMPTY_BUFFER);

        Field oshField = bytesMessage.getClass().getDeclaredField("bytesOut");
        oshField.setAccessible(true);
        oshField.set(bytesMessage, mock);

        return mock;
    }
}
