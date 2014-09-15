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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.codec.impl.DataImpl;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpTextMessageTest extends QpidJmsTestCase
{
    private static final String UTF_8 = "UTF-8";
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
    public void testGetTextWithNewMessageToSendReturnsNull() throws Exception
    {
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage();

        assertNull("expected null string", amqpTextMessage.getText());
    }

    @Test
    public void testNewMessageToSendContainsAmqpValueBodyWithNull() throws Exception
    {
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage();
        assertTrue(amqpTextMessage.getMessage().getBody() instanceof AmqpValue);
        assertNull(((AmqpValue)amqpTextMessage.getMessage().getBody()).getValue());
    }

    @Test
    public void testSetGetTextWithNewMessageToSend() throws Exception
    {
        String text = "myTestText";
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage();

        amqpTextMessage.setText(text);
        assertNotNull(amqpTextMessage.getMessage().getBody());
        assertTrue(amqpTextMessage.getMessage().getBody() instanceof AmqpValue);
        assertEquals(text, ((AmqpValue)amqpTextMessage.getMessage().getBody()).getValue());

        assertEquals(text, amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithNoBodySectionReturnsNull() throws Exception
    {
        Message message = Proton.message();
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull("expected null string", amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithAmqpValueSectionContainingString() throws Exception
    {
        String text = "myString";

        Message message = Proton.message();
        message.setBody(new AmqpValue(text));

        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(text, amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithAmqpValueSectionContainingNull() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));

        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull("expected null string", amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyString() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new Data(null));

        //This shouldn't happen with actual received messages, since Data sections can't really
        //have a null value in them, they would have an empty byte array, but just in case...

        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected zero-length string", "", amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingZeroLengthBinaryReturnsEmptyString() throws Exception
    {
        org.apache.qpid.proton.codec.Data payloadData = new DataImpl();
        payloadData.putDescribedType(new DataDescribedType(new Binary(new byte[0])));
        Binary b = payloadData.encode();

        System.out.println("Using encoded AMQP message payload: " + b);

        Message message = Proton.message();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected zero-length string", "", amqpTextMessage.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingStringBytes() throws Exception
    {
        String encodedString = "myEncodedString";
        byte[] encodedBytes = encodedString.getBytes(Charset.forName(UTF_8));

        org.apache.qpid.proton.codec.Data payloadData = new DataImpl();
        payloadData.putDescribedType(new DataDescribedType(new Binary(encodedBytes)));
        Binary b = payloadData.encode();

        System.out.println("Using encoded AMQP message payload: " + b);

        Message message = Proton.message();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(encodedString, amqpTextMessage.getText());
    }

    @Test
    public void testGetTextWithNonAmqpValueOrDataSectionThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpTextMessage.getText();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    @Test
    public void testGetTextWithAmqpValueContainingNonNullNonStringValueThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(true));
        AmqpTextMessage amqpTextMessage = new AmqpTextMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            amqpTextMessage.getText();
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }
}
