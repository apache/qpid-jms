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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import javax.jms.MessageEOFException;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Test;

public class AmqpJmsStreamMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    @Test
    public void testNewMessageToSendReportsNoBody() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();
        assertFalse(amqpStreamMessageFacade.hasBody(), "Message should report no body");
    }

    @Test
    public void testNewMessageToSendDoesnNotContainMessageTypeAnnotation() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        MessageAnnotations annotations = amqpStreamMessageFacade.getMessageAnnotations();

        assertNull(annotations, "MessageAnnotations section was not present");
        assertEquals(JMS_STREAM_MESSAGE, amqpStreamMessageFacade.getJmsMsgType());
    }

    @Test
    public void testNewMessageToSendContainsAmqpSequenceBody() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        Section body = amqpStreamMessageFacade.getBody();

        assertNotNull(body, "Body section was not present");
        assertTrue(body instanceof AmqpSequence, "Body section was not of expected type: " + body.getClass());
    }

    @Test
    public void testPeekWithNewMessageToSendThrowsMEOFE() throws Exception {
        assertThrows(MessageEOFException.class, () -> {
            AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();
            amqpStreamMessageFacade.peek();
        });
    }

    @Test
    public void testPopWithNewMessageToSendThrowsMEOFE() throws Exception {
        assertThrows(MessageEOFException.class, () -> {
            AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();
            amqpStreamMessageFacade.pop();
        });
    }

    @Test
    public void testPeekUsingReceivedMessageWithAmqpValueBodyReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpValue(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
    }

    @Test
    public void testPeekUsingReceivedMessageWithAmqpSequenceBodyReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
    }

    @Test
    public void testRepeatedPeekReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpStreamMessageFacade.hasBody(), "Message should report that it contains a body");
        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
    }

    @Test
    public void testRepeatedPeekAfterPopReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();
        assertEquals(Boolean.TRUE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
    }

    @Test
    public void testResetPositionAfterPop() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();

        amqpStreamMessageFacade.reset();

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();

        assertEquals(Boolean.TRUE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();
    }

    @Test
    public void testResetPositionAfterPeekThrowsMEOFE() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();
        assertEquals(Boolean.TRUE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();

        try {
            amqpStreamMessageFacade.peek();
            fail("expected exception to be thrown");
        } catch (MessageEOFException meofe) {
            // expected
        }

        amqpStreamMessageFacade.reset();

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();
        assertEquals(Boolean.TRUE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
    }

    @Test
    public void testHasNext() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        assertFalse(amqpStreamMessageFacade.hasNext(), "unexpected value");

        // add some things
        amqpStreamMessageFacade.put(Boolean.TRUE);
        amqpStreamMessageFacade.put(Boolean.FALSE);

        assertTrue(amqpStreamMessageFacade.hasNext(), "unexpected value");
        amqpStreamMessageFacade.pop();
        assertTrue(amqpStreamMessageFacade.hasNext(), "unexpected value");
        amqpStreamMessageFacade.pop();
        assertFalse(amqpStreamMessageFacade.hasNext(), "unexpected value");
    }

    @Test
    public void testClearBody() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        // add some stuff
        amqpStreamMessageFacade.put(Boolean.TRUE);
        amqpStreamMessageFacade.put(Boolean.FALSE);

        // retrieve only some of it, leaving some unread
        assertEquals(Boolean.TRUE, amqpStreamMessageFacade.peek(), "unexpected value");
        amqpStreamMessageFacade.pop();

        // clear
        amqpStreamMessageFacade.clearBody();

        // add something else
        amqpStreamMessageFacade.put(Character.valueOf('c'));

        // check we can get it alone before another IOOBE (i.e position was reset, other contents cleared)
        assertEquals(Character.valueOf('c'), amqpStreamMessageFacade.peek(), "unexpected value");
        amqpStreamMessageFacade.pop();

        try {
            amqpStreamMessageFacade.peek();
            fail("expected exception to be thrown");
        } catch (MessageEOFException meofe) {
            // expected
        }
    }

    @Test
    public void testPopFullyReadListThrowsMEOFE() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(Boolean.FALSE, amqpStreamMessageFacade.peek(), "Unexpected value retrieved");
        amqpStreamMessageFacade.pop();

        try {
            amqpStreamMessageFacade.pop();
            fail("expected exception to be thrown");
        } catch (MessageEOFException meofe) {
            // expected
        }
    }

    @Test
    public void testCreateWithUnexpectedBodySectionTypeThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(new byte[0])));

        try {
            createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);
            fail("expected exception to be thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testCreateWithAmqpValueBodySectionContainingUnexpectedValueThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("not-a-list"));

        try {
            createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);
            fail("expected exception to be thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testCreateWithEmptyAmqpValueBodySection() throws Exception
    {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(null));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        //Should be able to use the message, e.g clearing it and adding to it.
        amqpStreamMessageFacade.clearBody();
        amqpStreamMessageFacade.put("myString");
    }

    @Test
    public void testCreateWithEmptyAmqpSequenceBodySection() throws Exception
    {
        Message message = Message.Factory.create();
        message.setBody(new AmqpSequence(null));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        //Should be able to use the message, e.g clearing it and adding to it.
        amqpStreamMessageFacade.clearBody();
        amqpStreamMessageFacade.put("myString");
    }

    @Test
    public void testCreateWithNoBodySection() throws Exception
    {
        Message message = Message.Factory.create();
        message.setBody(null);

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        //Should be able to use the message, e.g clearing it and adding to it.
        amqpStreamMessageFacade.clearBody();
        amqpStreamMessageFacade.put("myString");
    }
}