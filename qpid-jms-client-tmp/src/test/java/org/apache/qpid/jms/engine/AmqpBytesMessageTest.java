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
package org.apache.qpid.jms.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpBytesMessageTest extends QpidJmsTestCase
{
    private static final int END_OF_STREAM = -1;

    private AmqpConnection _mockAmqpConnection;
    private Delivery _mockDelivery;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockDelivery = Mockito.mock(Delivery.class);
    }

    @Test
    public void testNewMessageToSendContainsMessageTypeAnnotation() throws Exception
    {
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();
        assertTrue("expected message type annotation to be present", amqpBytesMessage.messageAnnotationExists(ClientProperties.X_OPT_JMS_MSG_TYPE));
        assertEquals("unexpected value for message type annotation value", ClientProperties.BYTES_MESSAGE_TYPE, amqpBytesMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE));
    }

    @Test
    public void testGetInputStreamWithNewMessageToSendReturnsEmptyInputStream() throws Exception
    {
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();

        ByteArrayInputStream byteArrayInputStream = amqpBytesMessage.getByteArrayInputStream();
        assertNotNull(byteArrayInputStream);

        //try to read a byte, it should return -1 bytes read, i.e EOS.
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, byteArrayInputStream.read(new byte[1]));
    }

    @Test
    public void testGetInputStreamUsingReceivedMessageWithNoBodySectionReturnsEmptyInputStream() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        ByteArrayInputStream byteArrayInputStream = amqpBytesMessage.getByteArrayInputStream();
        assertNotNull(byteArrayInputStream);

        //try to read a byte, it should return -1 bytes read, i.e EOS.
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, byteArrayInputStream.read(new byte[1]));
    }

    @Test
    public void testGetBytesLengthUsingNewMessageToSend() throws Exception
    {
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();

        assertEquals("Message reports unexpected length", 0, amqpBytesMessage.getBytesLength());
    }

    @Test
    public void testNewMessageToSendHasContentTypeButNoBodySection() throws Exception
    {
        //TODO: this test assumes we can omit the body section. If we decide otherwise
        //it should instead check for e.g. a data section containing 0 length binary
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();
        Message protonMessage = amqpBytesMessage.getMessage();

        assertNotNull(protonMessage);
        assertNull(protonMessage.getBody());

        String contentType = protonMessage.getContentType();
        assertNotNull("content type should be set", contentType);
        assertEquals(AmqpBytesMessage.CONTENT_TYPE, contentType);
    }

    @Test
    public void testGetBytesLengthUsingPopulatedMessageToSend() throws Exception
    {
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();

        byte[] bytes = "myBytes".getBytes();
        amqpBytesMessage.setBytes(bytes);

        assertEquals("Message reports unexpected length", bytes.length, amqpBytesMessage.getBytesLength());
    }

    @Test
    public void testGetBytesLengthUsingReceivedMessageWithDataSectionContainingNonZeroLengthBinary() throws Exception
    {
        Message message = Proton.message();
        int length = 5;
        message.setBody(new Data(new Binary(new byte[length])));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("Message reports unexpected length", length, amqpBytesMessage.getBytesLength());
    }

    @Test
    public void testGetBytesLengthUsingReceivedMessageWithAmqpValueSectionContainingNonZeroLengthBinary() throws Exception
    {
        Message message = Proton.message();
        int length = 10;
        message.setBody(new AmqpValue(new Binary(new byte[length])));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("Message reports unexpected length", length, amqpBytesMessage.getBytesLength());
    }

    @Test
    public void testGetBytesLengthUsingReceivedMessageWithAmqpValueSectionContainingNull() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("Message reports unexpected length", 0, amqpBytesMessage.getBytesLength());
    }

    @Test
    public void testInputStreamUsingReceivedMessageWithAmqpValueSectionContainingBinary() throws Exception
    {
        byte[] bytes = "myBytes".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        ByteArrayInputStream bytesStream = amqpBytesMessage.getByteArrayInputStream();

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesStream.read(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testInputStreamUsingReceivedMessageWithDataSection() throws Exception
    {
        byte[] bytes = "myBytes".getBytes();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        ByteArrayInputStream bytesStream = amqpBytesMessage.getByteArrayInputStream();
        assertNotNull(bytesStream);

        //retrieve the expected bytes, check they match
        byte[] receivedBytes = new byte[bytes.length];
        bytesStream.read(receivedBytes);
        assertTrue(Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyBAIS() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new Data(null));

        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        ByteArrayInputStream bytesStream = amqpBytesMessage.getByteArrayInputStream();
        assertNotNull(bytesStream);

        assertEquals("Message reports unexpected length", 0, amqpBytesMessage.getBytesLength());
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    @Test
    public void testGetMethodsWithNonAmqpValueNonDataSectionThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpBytesMessage.getByteArrayInputStream();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }

        try
        {
            amqpBytesMessage.getBytesLength();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    @Test
    public void testGetMethodsWithAmqpValueContainingNonNullNonBinaryValueThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(true));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpBytesMessage.getByteArrayInputStream();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }

        try
        {
            amqpBytesMessage.getBytesLength();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    /**
     * Test that setting bytes on a new messages creates the data section of the underlying message,
     * which as tested by {@link testNewMessageToSendHasContentTypeButNoBodySection} does not exist initially.
     */
    @Test
    public void testSetBytesOnNewMessageCreatesDataSection() throws Exception
    {
        byte[] testBytes = "myTestBytes".getBytes();
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();
        Message protonMessage = amqpBytesMessage.getMessage();

        assertNotNull("underlying proton message was null", protonMessage);
        assertNull("Expected no body section to be present", protonMessage.getBody());

        amqpBytesMessage.setBytes(testBytes);

        assertNotNull("Expected body section to be present", protonMessage.getBody());
        assertEquals("Unexpected body section type", Data.class, protonMessage.getBody().getClass());
    }

    /**
     * Test that setting bytes on a new message results in the expected content in the body section of the
     * underlying message and returned by a new InputStream requested from the message.
     */
    @Test
    public void testSetBytesOnNewMessage() throws Exception
    {
        byte[] bytes = "myTestBytes".getBytes();
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage();
        Message protonMessage = amqpBytesMessage.getMessage();

        amqpBytesMessage.setBytes(bytes);

        //retrieve the bytes from the underlying message, check they match
        Data body = (Data) protonMessage.getBody();
        assertTrue("Underlying message data section did not contain the expected bytes", Arrays.equals(bytes, body.getValue().getArray()));

        //retrieve the bytes via an InputStream, check they match expected
        byte[] receivedBytes = new byte[bytes.length];
        ByteArrayInputStream bytesStream = amqpBytesMessage.getByteArrayInputStream();
        bytesStream.read(receivedBytes);
        assertTrue("Retrieved bytes from input steam did not match expected bytes", Arrays.equals(bytes, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    /**
     * Test that setting bytes on a received message results in the expected content in the body section of the
     * underlying message and returned by a new InputStream requested from the message.
     */
    @Test
    public void testSetBytesOnReceivedMessage() throws Exception
    {
        byte[] orig = "myOrigBytes".getBytes();
        byte[] replacement = "myReplacementBytes".getBytes();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(orig)));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        amqpBytesMessage.setBytes(replacement);

        //retrieve the new bytes from the underlying message, check they match
        Data body = (Data) protonMessage.getBody();
        assertTrue("Underlying message data section did not contain the expected bytes", Arrays.equals(replacement, body.getValue().getArray()));

        assertEquals("expected length to match replacement bytes", replacement.length, amqpBytesMessage.getBytesLength());

        //retrieve the new bytes via an InputStream, check they match expected
        byte[] receivedBytes = new byte[replacement.length];
        ByteArrayInputStream bytesStream = amqpBytesMessage.getByteArrayInputStream();
        bytesStream.read(receivedBytes);
        assertTrue("Retrieved bytes from input steam did not match expected bytes", Arrays.equals(replacement, receivedBytes));

        //verify no more bytes remain, i.e EOS
        assertEquals("Expected input stream to be at end but data was returned", END_OF_STREAM, bytesStream.read(new byte[1]));
    }

    /**
     * Test that setting bytes on a received message results which had no content type
     * results in the content type being set.
     */
    @Test
    public void testSetBytesOnReceivedMessageSetsContentTypeIfBodyTypeChanged() throws Exception
    {
        byte[] orig = "myOrigBytes".getBytes();
        byte[] replacement = "myReplacementBytes".getBytes();

        Message message = Proton.message();
        message.setBody(new AmqpValue(new Binary(orig)));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        amqpBytesMessage.setBytes(replacement);

        String contentType = protonMessage.getContentType();
        assertNotNull("content type should be set", contentType);
        assertEquals(AmqpBytesMessage.CONTENT_TYPE, contentType);
    }

    /**
     * Test that a non-data body section of a received message can be converted to a data section as expected.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithNullBody() throws Exception
    {
        Message message = Proton.message();
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        assertNull("expected no body", protonMessage.getBody());

        amqpBytesMessage.convertBodyToDataSectionIfNecessary();

        Section body = protonMessage.getBody();
        assertNotNull("expected body section not present", body);
        assertEquals("unexpected type of body section", Data.class, body.getClass());
        assertEquals("unexpected length of body", 0, ((Data) body).getValue().getLength());
    }

    /**
     * Test that a non-data body section of a received message can be converted to a data section as expected.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithAmqpValueContainingNull() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        assertNotNull("expected body", protonMessage.getBody());
        assertEquals("unexpected type of body section", AmqpValue.class, protonMessage.getBody().getClass());

        amqpBytesMessage.convertBodyToDataSectionIfNecessary();

        Section body = protonMessage.getBody();
        assertNotNull("expected body section not present", body);
        assertEquals("unexpected type of body section", Data.class, body.getClass());
        assertEquals("unexpected length of body", 0, ((Data) body).getValue().getLength());
    }

    /**
     * Test that a non-data body section of a received message can be converted to a data section as expected.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithAmqpValueContainingBinary() throws Exception
    {
        Message message = Proton.message();
        byte[] bytes = "asdjsdadad".getBytes();
        message.setBody(new AmqpValue(new Binary(bytes)));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        assertNotNull("expected body", protonMessage.getBody());
        assertEquals("unexpected type of body section", AmqpValue.class, protonMessage.getBody().getClass());

        amqpBytesMessage.convertBodyToDataSectionIfNecessary();

        Section body = protonMessage.getBody();
        assertNotNull("expected body section not present", body);
        assertEquals("unexpected type of body section", Data.class, body.getClass());
        assertEquals("unexpected length of body", bytes.length, ((Data) body).getValue().getLength());
        assertTrue("unexpected bytes", Arrays.equals(bytes, ((Data) body).getValue().getArray()));
    }

    /**
     * Test that a data body section of a received message with content type set remains
     *  unchanged when attempting to convert to a data section.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithDataSection() throws Exception
    {
        Message message = Proton.message();
        byte[] bytes = "asdjsdadad".getBytes();
        Data origBody = new Data(new Binary(bytes));
        message.setBody(origBody);
        message.setContentType(AmqpBytesMessage.CONTENT_TYPE);
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);
        Message protonMessage = amqpBytesMessage.getMessage();

        amqpBytesMessage.convertBodyToDataSectionIfNecessary();

        Section body = protonMessage.getBody();
        assertNotNull("expected body section not present", body);
        assertSame("Expected original body to be present", origBody, body);
    }

    /**
     * Test that a non-data body section of a received message which can't be converted to a data
     * section causes an ISE to be thrown.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithAmqpValueContainingUnexpectedValueThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(new ArrayList<Object>()));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpBytesMessage.convertBodyToDataSectionIfNecessary();
            fail("expected exception to be thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    /**
     * Test that a non-data body section of a received message which can't be converted to a data
     * section causes an ISE to be thrown.
     */
    @Test
    public void testConvertBodyToDataSectionIfNecessaryWithUnexpectedBodyValueThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpBytesMessage amqpBytesMessage = new AmqpBytesMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpBytesMessage.convertBodyToDataSectionIfNecessary();
            fail("expected exception to be thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }
}
