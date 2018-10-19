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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.util.ArrayList;

import javax.jms.JMSException;

import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

/**
 * Tests for class AmqpJmsTextMessageFacade
 */
public class AmqpJmsTextMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    //---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageToSendDoesNotContainMessageTypeAnnotation() throws Exception {
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();

        assertNull("MessageAnnotations section was not present", amqpTextMessageFacade.getMessageAnnotations());
        assertEquals(JMS_TEXT_MESSAGE, amqpTextMessageFacade.getJmsMsgType());
    }

    @Test
    public void testNewMessageToSendClearBodyDoesNotFail() throws Exception {
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();
        amqpTextMessageFacade.clearBody();
    }

    @Test
    public void testNewMessageToSendReturnsNullText() throws Exception {
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();
        amqpTextMessageFacade.clearBody();
        assertNull(amqpTextMessageFacade.getText());
    }

    // ---------- test for normal message operations -------------------------//

    @Test
    public void testMessageClearBodyWorks() throws Exception {
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();
        assertNull(amqpTextMessageFacade.getText());
        amqpTextMessageFacade.setText("SomeTextForMe");
        assertNotNull(amqpTextMessageFacade.getText());
        amqpTextMessageFacade.clearBody();
        assertNull(amqpTextMessageFacade.getText());
    }

    @Test
    public void testMessageCopy() throws Exception {
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();
        amqpTextMessageFacade.setText("SomeTextForMe");

        AmqpJmsTextMessageFacade copy = amqpTextMessageFacade.copy();
        assertEquals("SomeTextForMe", copy.getText());
    }

    @Test
    public void testSetGetTextWithNewMessageToSend() throws Exception {
        String text = "myTestText";
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createNewTextMessageFacade();

        amqpTextMessageFacade.setText(text);
        assertNotNull(amqpTextMessageFacade.getBody());
        assertTrue(amqpTextMessageFacade.getBody() instanceof AmqpValue);
        assertEquals(text, ((AmqpValue) amqpTextMessageFacade.getBody()).getValue());

        assertEquals(text, amqpTextMessageFacade.getText());
    }

    // ---------- test handling of received messages -------------------------//

    @Test
    public void testCreateWithEmptyAmqpValue() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(null));

        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpTextMessageFacade.clearBody();
        amqpTextMessageFacade.setText("TEST");
        assertEquals("TEST", amqpTextMessageFacade.getText());
    }

    @Test
    public void testCreateWithNonEmptyAmqpValue() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("TEST"));

        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("TEST", amqpTextMessageFacade.getText());

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpTextMessageFacade.clearBody();
        amqpTextMessageFacade.setText("TEST-CLEARED");
        assertEquals("TEST-CLEARED", amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithNoBodySectionReturnsNull() throws Exception {
        Message message = Message.Factory.create();
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertNull("expected null string", amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithAmqpValueSectionContainingNull() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(null));

        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertNull("expected null string", amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyString() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new Data(null));

        // This shouldn't happen with actual received messages, since Data sections can't really
        // have a null value in them, they would have an empty byte array, but just in case...
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected zero-length string", "", amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithZeroLengthDataSectionReturnsEmptyString() throws Exception {
        org.apache.qpid.proton.codec.Data payloadData = org.apache.qpid.proton.codec.Data.Factory.create();
        payloadData.putDescribedType(new DataDescribedType(new Binary(new byte[0])));
        Binary b = payloadData.encode();

        Message message = Message.Factory.create();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected zero-length string", "", amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextUsingReceivedMessageWithDataSectionContainingStringBytes() throws Exception {
        String encodedString = "myEncodedString";
        byte[] encodedBytes = encodedString.getBytes(Charset.forName("UTF-8"));

        org.apache.qpid.proton.codec.Data payloadData = org.apache.qpid.proton.codec.Data.Factory.create();
        payloadData.putDescribedType(new DataDescribedType(new Binary(encodedBytes)));
        Binary b = payloadData.encode();

        Message message = Message.Factory.create();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(encodedString, amqpTextMessageFacade.getText());
    }

    @Test
    public void testGetTextWithNonAmqpValueOrDataSectionReportsNoBody() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);
        assertFalse(amqpTextMessageFacade.hasBody());
    }

    @Test
    public void testGetTextWithNonAmqpValueOrDataSectionThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpTextMessageFacade.getText();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testGetTextWithAmqpValueContainingNonNullNonStringValueThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(true));
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpTextMessageFacade.getText();
            fail("expected exception not thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testGetTextWithUnknownEncodedDataThrowsJMSException() throws Exception {
        String encodedString = "myEncodedString";
        byte[] encodedBytes = encodedString.getBytes(Charset.forName("UTF-16"));

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(encodedBytes)));
        AmqpJmsTextMessageFacade amqpTextMessageFacade = createReceivedTextMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpTextMessageFacade.getText();
            fail("expected exception not thrown");
        } catch (JMSException ise) {
            // expected
        }
    }
}
