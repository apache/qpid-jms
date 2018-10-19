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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

/**
 * Test for the AmqpJmsMapMessageFacade class
 */
public class AmqpJmsMapMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    //---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageToSendDoesnNotContainMessageTypeAnnotation() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();

        MessageAnnotations annotations = amqpMapMessageFacade.getMessageAnnotations();

        assertNull("MessageAnnotations section was present", annotations);

        assertEquals(JMS_MAP_MESSAGE, amqpMapMessageFacade.getJmsMsgType());
    }

    @Test
    public void testNewMessageToSendClearBodyDoesNotFail() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        amqpMapMessageFacade.clearBody();
    }

    @Test
    public void testNewMessageToSendReportsNoBody() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        amqpMapMessageFacade.hasBody();
    }

    @Test
    public void testNewMessageToSendReportsIsEmpty() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertFalse(amqpMapMessageFacade.getMapNames().hasMoreElements());
    }

    @Test
    public void testNewMessageToSendItemExists() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertFalse(amqpMapMessageFacade.itemExists("entry"));
    }

    @Test
    public void testNewMessageToSendGetReturnsNull() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertNull(amqpMapMessageFacade.get("entry"));
    }

    @Test
    public void testNewMessageToSendRemoveReturnsNull() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertNull(amqpMapMessageFacade.remove("entry"));
    }

    @Test
    public void testNewMessageToSendReturnsEmptyMapNamesEnumeration() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertNotNull(amqpMapMessageFacade.getMapNames());

        Enumeration<String> names = amqpMapMessageFacade.getMapNames();
        assertFalse(names.hasMoreElements());
    }

    // ---------- test for normal message operations -------------------------//

    @Test
    public void testMessageClearBodyWorks() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        assertFalse(amqpMapMessageFacade.getMapNames().hasMoreElements());
        amqpMapMessageFacade.put("entry", "value");
        assertTrue(amqpMapMessageFacade.getMapNames().hasMoreElements());
        amqpMapMessageFacade.clearBody();
        assertFalse(amqpMapMessageFacade.getMapNames().hasMoreElements());
    }

    @Test
    public void testMessageCopy() throws Exception {
        AmqpJmsMapMessageFacade amqpMapMessageFacade = createNewMapMessageFacade();
        amqpMapMessageFacade.put("entry1", "value1");
        amqpMapMessageFacade.put("entry2", "value2");
        amqpMapMessageFacade.put("entry3", "value3");

        AmqpJmsMapMessageFacade copy = amqpMapMessageFacade.copy();
        assertTrue(copy.getMapNames().hasMoreElements());

        assertTrue(copy.itemExists("entry1"));
        assertTrue(copy.itemExists("entry2"));
        assertTrue(copy.itemExists("entry3"));
    }

    // ---------- test handling of received messages -------------------------//

    @Test
    public void testCreateWithEmptyMap() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(new HashMap<String, Object>()));

        AmqpJmsMapMessageFacade amqpMapMessageFacade = createReceivedMapMessageFacade(createMockAmqpConsumer(), message);

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpMapMessageFacade.clearBody();
        amqpMapMessageFacade.put("entry", "value");
    }

    @Test
    public void testCreateWithPopulatedMap() throws Exception {
        Message message = Message.Factory.create();
        Map<String, Object> bodyMap = new HashMap<String, Object>();
        bodyMap.put("entry1", Boolean.TRUE);
        bodyMap.put("entry2", Boolean.FALSE);

        message.setBody(new AmqpValue(bodyMap));

        AmqpJmsMapMessageFacade amqpMapMessageFacade = createReceivedMapMessageFacade(createMockAmqpConsumer(), message);

        // Data should be preserved
        assertTrue(amqpMapMessageFacade.getMapNames().hasMoreElements());
        Object result = amqpMapMessageFacade.get("entry1");
        assertNotNull(result);
        assertTrue(result instanceof Boolean);
        assertTrue(amqpMapMessageFacade.hasBody());

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpMapMessageFacade.clearBody();
        assertFalse(amqpMapMessageFacade.hasBody());
        amqpMapMessageFacade.put("entry", "value");
    }

    @Test
    public void testCreateWithAmqpSequenceBodySectionThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpSequence(null)   );

        try {
            createReceivedMapMessageFacade(createMockAmqpConsumer(), message);
            fail("expected exception to be thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testCreateWithAmqpValueBodySectionContainingUnexpectedValueThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("not-a-map"));

        try {
            createReceivedMapMessageFacade(createMockAmqpConsumer(), message);
            fail("expected exception to be thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testCreateWithNullBodySection() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(null);

        AmqpJmsMapMessageFacade amqpMapMessageFacade = createReceivedMapMessageFacade(createMockAmqpConsumer(), message);

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpMapMessageFacade.clearBody();
        amqpMapMessageFacade.put("entry", "value");
        assertTrue(amqpMapMessageFacade.getMapNames().hasMoreElements());
    }

    @Test
    public void testCreateWithEmptyAmqpValueBodySection() throws Exception {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(null));

        AmqpJmsMapMessageFacade amqpMapMessageFacade = createReceivedMapMessageFacade(createMockAmqpConsumer(), message);

        // Should be able to use the message, e.g clearing it and adding to it.
        amqpMapMessageFacade.clearBody();
        amqpMapMessageFacade.put("entry", "value");
        assertTrue(amqpMapMessageFacade.getMapNames().hasMoreElements());
    }

    //----- Test Read / Write of special contents in Map ---------------------//

    /**
     * Verify that for a message received with an AmqpValue containing a Map with a Binary entry
     * value, we are able to read it back as a byte[].
     *
     * @throws Exception if an error occurs while running the test.
     */
    @Test
    public void testReceivedMapWithBinaryEntryReturnsByteArray() throws Exception {
        String myKey1 = "key1";
        String bytesSource = "myBytesAmqpValue";

        Map<String, Object> origMap = new HashMap<String, Object>();
        byte[] bytes = bytesSource.getBytes();
        origMap.put(myKey1, new Binary(bytes));

        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(origMap));

        AmqpJmsMapMessageFacade amqpMapMessageFacade = createReceivedMapMessageFacade(createMockAmqpConsumer(), message);

        // retrieve the bytes using getBytes, check they match expectation
        Object objectValue = amqpMapMessageFacade.get(myKey1);
        assertTrue(byte[].class.equals(objectValue.getClass()));
        byte[] bytesValue = (byte[]) objectValue;
        assertTrue(Arrays.equals(bytes, bytesValue));
    }
}
