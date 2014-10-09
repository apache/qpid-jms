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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.getSymbol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for class AmqpJmsBytesMessageFacade
 */
public class AmqpJmsBytesMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    private static final int END_OF_STREAM = -1;

    // ---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageContainsMessageTypeAnnotation() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();
        MessageAnnotations annotations = protonMessage.getMessageAnnotations();
        Map<Symbol, Object> annotationsMap = annotations.getValue();

        assertNotNull("MessageAnnotations section was not present", annotations);
        assertNotNull("MessageAnnotations section value was not present", annotationsMap);

        assertTrue("expected message type annotation to be present", annotationsMap.containsKey(AmqpMessageSupport.getSymbol(JMS_MSG_TYPE)));
        assertEquals("unexpected value for message type annotation value", JMS_BYTES_MESSAGE, annotationsMap.get(getSymbol(JMS_MSG_TYPE)));
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
    public void testNewMessageHasContentTypeButNoBodySection() throws Exception {
        // TODO: this test assumes we can omit the body section. If we decide otherwise
        // it should instead check for e.g. a data section containing 0 length binary
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();
        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();

        assertNotNull(protonMessage);
        assertNull(protonMessage.getBody());

        String contentType = protonMessage.getContentType();
        assertNotNull("content type should be set", contentType);
        assertEquals("application/octet-stream", contentType);
    }

    // ---------- test for normal message operations -------------------------//

    @Test
    public void testGetBodyLengthUsingPopulatedMessageToSend() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        byte[] bytes = "myBytes".getBytes();
        amqpBytesMessageFacade.setBody(bytes);

        assertEquals("Message reports unexpected length", bytes.length, amqpBytesMessageFacade.getBodyLength());
    }

    /**
     * Test that setting bytes on a new messages creates the data section of the underlying message,
     * which as tested by {@link #testNewMessageHasContentTypeButNoBodySection} does not exist initially.
     */
    @Test
    public void testSetBodyOnNewMessageCreatesDataSection() throws Exception {
        byte[] testBytes = "myTestBytes".getBytes();
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();
        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();

        assertNotNull("underlying proton message was null", protonMessage);
        assertNull("Expected no body section to be present", protonMessage.getBody());

        amqpBytesMessageFacade.setBody(testBytes);

        assertNotNull("Expected body section to be present", protonMessage.getBody());
        assertEquals("Unexpected body section type", Data.class, protonMessage.getBody().getClass());
    }

    /**
     * Test that setting bytes on a new message results in the expected content in the body section
     * of the underlying message and returned by a new InputStream requested from the message.
     */
    @Test
    public void testSetGetBytesOnNewMessage() throws Exception {
        byte[] bytes = "myTestBytes".getBytes();
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();
        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();

        amqpBytesMessageFacade.setBody(bytes);

        // retrieve the bytes from the underlying message, check they match
        Data body = (Data) protonMessage.getBody();
        assertTrue("Underlying message data section did not contain the expected bytes", Arrays.equals(bytes, body.getValue().getArray()));

        // retrieve the bytes via an InputStream, check they match expected
        byte[] receivedBytes = new byte[bytes.length];
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();
        bytesStream.read(receivedBytes);
        assertTrue("Retrieved bytes from input steam did not match expected bytes", Arrays.equals(bytes, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testClearBodySetsBodyLength0AndCausesEmptyInputStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        byte[] bytes = "myBytes".getBytes();
        amqpBytesMessageFacade.setBody(bytes);
        amqpBytesMessageFacade.clearBody();

        amqpBytesMessageFacade.clearBody();
        assertTrue("Expected no message content from facade", amqpBytesMessageFacade.getBodyLength() == 0);
        assertEquals("Expected no data from facade, but got some", END_OF_STREAM, amqpBytesMessageFacade.getInputStream().read(new byte[1]));

        Section body = amqpBytesMessageFacade.getAmqpMessage().getBody();
        assertTrue(body instanceof Data);
        Binary value = ((Data) body).getValue();
        assertNotNull(value);
        assertEquals(0, value.getLength());
    }

    @Test
    public void testClearBodyWithExistingInputStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        byte[] bytes = "myBytes".getBytes();
        amqpBytesMessageFacade.setBody(bytes);

        @SuppressWarnings("unused")
        InputStream unused = amqpBytesMessageFacade.getInputStream();

        amqpBytesMessageFacade.clearBody();

        assertEquals("Expected no data from facade, but got some", END_OF_STREAM, amqpBytesMessageFacade.getInputStream().read(new byte[1]));

        Section body = amqpBytesMessageFacade.getAmqpMessage().getBody();
        assertTrue(body instanceof Data);
        Binary value = ((Data) body).getValue();
        assertNotNull(value);
        assertEquals(0, value.getLength());
    }

    @Test
    public void testClearBodyWithExistingOutputStream() throws Exception {
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createNewBytesMessageFacade();

        byte[] bytes = "myBytes".getBytes();
        amqpBytesMessageFacade.setBody(bytes);

        @SuppressWarnings("unused")
        OutputStream unused = amqpBytesMessageFacade.getOutputStream();

        amqpBytesMessageFacade.clearBody();

        Section body = amqpBytesMessageFacade.getAmqpMessage().getBody();
        assertTrue(body instanceof Data);
        Binary value = ((Data) body).getValue();
        assertNotNull(value);
        assertEquals(0, value.getLength());
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
     */
    @Test
    public void testSetGetBodyOnReceivedMessage() throws Exception {
        byte[] orig = "myOrigBytes".getBytes();
        byte[] replacement = "myReplacementBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(orig)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);
        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();

        amqpBytesMessageFacade.setBody(replacement);

        // retrieve the new bytes from the underlying message, check they match
        Data body = (Data) protonMessage.getBody();
        assertTrue("Underlying message data section did not contain the expected bytes", Arrays.equals(replacement, body.getValue().getArray()));

        assertEquals("expected length to match replacement bytes", replacement.length, amqpBytesMessageFacade.getBodyLength());

        // retrieve the new bytes via an InputStream, check they match expected
        byte[] receivedBytes = new byte[replacement.length];
        InputStream bytesStream = amqpBytesMessageFacade.getInputStream();
        bytesStream.read(receivedBytes);
        assertTrue("Retrieved bytes from input steam did not match expected bytes", Arrays.equals(replacement, receivedBytes));

        // verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    /**
     * Test that setting bytes on a received message results which had no content type
     * results in the content type being set.
     */
    @Test
    @Ignore
    // TODO: failing because we dont set the content type except at creation. Decide if we actually care.
    public void testSetBytesOnReceivedMessageSetsContentTypeIfBodyTypeChanged() throws Exception {
        byte[] orig = "myOrigBytes".getBytes();
        byte[] replacement = "myReplacementBytes".getBytes();

        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(new Binary(orig)));
        AmqpJmsBytesMessageFacade amqpBytesMessageFacade = createReceivedBytesMessageFacade(createMockAmqpConsumer(), message);
        Message protonMessage = amqpBytesMessageFacade.getAmqpMessage();

        amqpBytesMessageFacade.setBody(replacement);

        String contentType = protonMessage.getContentType();
        assertNotNull("content type should be set", contentType);
        assertEquals("application/octet-stream", contentType);
    }
}
