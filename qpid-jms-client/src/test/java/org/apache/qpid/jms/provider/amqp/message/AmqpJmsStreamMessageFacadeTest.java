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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.MessageEOFException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsStreamMessageFacadeTest extends QpidJmsTestCase {

    private JmsDestination consumerDestination;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        consumerDestination = new JmsTopic("TestTopic");
    };

    @Test
    public void testNewMessageToSendContainsMessageTypeAnnotation() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        Message protonMessage = amqpStreamMessageFacade.getAmqpMessage();
        MessageAnnotations annotations = protonMessage.getMessageAnnotations();
        Map<Symbol, Object> annotationsMap = annotations.getValue();

        assertNotNull("MessageAnnotations section was not present", annotations);
        assertNotNull("MessageAnnotations section value was not present", annotationsMap);

        assertTrue("expected message type annotation to be present", annotationsMap.containsKey(AmqpMessageSupport.getSymbol(AmqpMessageSupport.JMS_MSG_TYPE)));
        assertEquals("unexpected value for message type annotation value", AmqpMessageSupport.JMS_STREAM_MESSAGE, annotationsMap.get(AmqpMessageSupport.getSymbol(AmqpMessageSupport.JMS_MSG_TYPE)));
    }

    @Test(expected = MessageEOFException.class)
    public void testPeekWithNewMessageToSendThrowsMEOFE() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();
        amqpStreamMessageFacade.peek();
    }

    @Test(expected = MessageEOFException.class)
    public void testPopWithNewMessageToSendThrowsMEOFE() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();
        amqpStreamMessageFacade.pop();
    }

    @Test
    public void testPeekUsingReceivedMessageWithAmqpValueBodyReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpValue(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
    }

    @Test
    public void testPeekUsingReceivedMessageWithAmqpSequenceBodyReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
    }

    @Test
    public void testRepeatedPeekReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
    }

    @Test
    public void testRepeatedPeekAfterPopReturnsExpectedValue() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();
        assertEquals("Unexpected value retrieved", Boolean.TRUE, amqpStreamMessageFacade.peek());
    }

    @Test
    public void testResetPositionAfterPop() throws Exception {
        Message message = Message.Factory.create();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        list.add(Boolean.TRUE);
        message.setBody(new AmqpSequence(list));

        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createReceivedStreamMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();

        amqpStreamMessageFacade.reset();

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();

        assertEquals("Unexpected value retrieved", Boolean.TRUE, amqpStreamMessageFacade.peek());
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

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();
        assertEquals("Unexpected value retrieved", Boolean.TRUE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();

        try {
            amqpStreamMessageFacade.peek();
            fail("expected exception to be thrown");
        } catch (MessageEOFException meofe) {
            // expected
        }

        amqpStreamMessageFacade.reset();

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();
        assertEquals("Unexpected value retrieved", Boolean.TRUE, amqpStreamMessageFacade.peek());
    }

    @Test
    public void testIsEmpty() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        assertTrue("unexpected value", amqpStreamMessageFacade.isEmpty());

        // add something
        amqpStreamMessageFacade.put(Boolean.TRUE);

        assertFalse("unexpected value", amqpStreamMessageFacade.isEmpty());
    }

    @Test
    public void testHasNext() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        assertFalse("unexpected value", amqpStreamMessageFacade.hasNext());

        // add some things
        amqpStreamMessageFacade.put(Boolean.TRUE);
        amqpStreamMessageFacade.put(Boolean.FALSE);

        assertTrue("unexpected value", amqpStreamMessageFacade.hasNext());
        amqpStreamMessageFacade.pop();
        assertTrue("unexpected value", amqpStreamMessageFacade.hasNext());
        amqpStreamMessageFacade.pop();
        assertFalse("unexpected value", amqpStreamMessageFacade.hasNext());
    }

    @Test
    public void testClearBody() throws Exception {
        AmqpJmsStreamMessageFacade amqpStreamMessageFacade = createNewStreamMessageFacade();

        // add some stuff
        amqpStreamMessageFacade.put(Boolean.TRUE);
        amqpStreamMessageFacade.put(Boolean.FALSE);

        // retrieve only some of it, leaving some unread
        assertEquals("unexpected value", Boolean.TRUE, amqpStreamMessageFacade.peek());
        amqpStreamMessageFacade.pop();

        // clear
        amqpStreamMessageFacade.clearBody();

        // add something else
        amqpStreamMessageFacade.put(Character.valueOf('c'));

        // check we can get it alone before another IOOBE (i.e position was reset, other contents cleared)
        assertEquals("unexpected value", Character.valueOf('c'), amqpStreamMessageFacade.peek());
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

        assertEquals("Unexpected value retrieved", Boolean.FALSE, amqpStreamMessageFacade.peek());
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

    // ====== Utility Methods =======
    // ==============================

    private AmqpJmsStreamMessageFacade createNewStreamMessageFacade() {
        return new AmqpJmsStreamMessageFacade(createMockAmqpConnection());
    }

    private AmqpJmsStreamMessageFacade createReceivedStreamMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsStreamMessageFacade(amqpConsumer, message);
    }

    private AmqpConsumer createMockAmqpConsumer() {
        AmqpConsumer consumer = Mockito.mock(AmqpConsumer.class);
        Mockito.when(consumer.getConnection()).thenReturn(createMockAmqpConnection());
        Mockito.when(consumer.getDestination()).thenReturn(consumerDestination);
        return consumer;
    }

    private AmqpConnection createMockAmqpConnection() {
        return Mockito.mock(AmqpConnection.class);
    }
}