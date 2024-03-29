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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Test;

/**
 * Tests for class AmqpJmsObjectMessageFacade
 */
public class AmqpJmsObjectMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    // ---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageToSendDoesNotContainMessageTypeAnnotation() throws Exception {
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(false);

        MessageAnnotations annotations = amqpObjectMessageFacade.getMessageAnnotations();

        assertNull(annotations, "MessageAnnotations section was present");
        assertEquals(JMS_OBJECT_MESSAGE, amqpObjectMessageFacade.getJmsMsgType());
    }

    @Test
    public void testNewMessageToSendReturnsNullObject() throws Exception {
        doNewMessageToSendReturnsNullObjectTestImpl(false);
    }

    @Test
    public void testNewAmqpTypedMessageToSendReturnsNullObject() throws Exception {
        doNewMessageToSendReturnsNullObjectTestImpl(true);
    }

    private void doNewMessageToSendReturnsNullObjectTestImpl(boolean amqpTyped) throws Exception {
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(amqpTyped);
        assertNull(amqpObjectMessageFacade.getObject());
    }

    // ---------- Test state of messages prepared to send -----------------//

    @Test
    public void testNewMessageToSendHasBodySectionRepresentingNull() throws Exception {
        doNewMessageToSendHasBodySectionRepresentingNull(false);
    }

    @Test
    public void testNewAmqpTypedMessageToSendHasBodySectionRepresentingNull() throws Exception {
        doNewMessageToSendHasBodySectionRepresentingNull(true);
    }

    private void doNewMessageToSendHasBodySectionRepresentingNull(boolean amqpTyped) throws Exception {
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(amqpTyped);
        amqpObjectMessageFacade.onSend(0);

        assertNotNull(amqpObjectMessageFacade.getBody(), "Message body should be presents");
        if(amqpTyped) {
            assertSame(AmqpTypedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.getBody(), "Expected existing body section to be replaced");
        } else {
            assertSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.getBody(), "Expected existing body section to be replaced");
        }
    }

    // ---------- test for normal message operations -------------------------//

    /**
     * Test that setting an object on a new message results in the expected
     * content in the body section of the underlying message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetObjectOnNewMessage() throws Exception {
        String content = "myStringContent";

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(false);
        amqpObjectMessageFacade.setObject(content);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(content);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        // retrieve the bytes from the underlying message, check they match expectation
        Section section = amqpObjectMessageFacade.getBody();
        assertNotNull(section);
        assertEquals(Data.class, section.getClass());
        assertArrayEquals(bytes, ((Data) section).getValue().getArray(), "Underlying message data section did not contain the expected bytes");
    }

    /**
     * Test that setting an object on a new message results in the expected
     * content in the body section of the underlying message.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetObjectOnNewAmqpTypedMessage() throws Exception {
        String content = "myStringContent";

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(true);
        amqpObjectMessageFacade.setObject(content);

        // retrieve the body from the underlying message, check it matches expectation
        Section section = amqpObjectMessageFacade.getBody();
        assertNotNull(section);
        assertEquals(AmqpValue.class, section.getClass());
        assertEquals(content, ((AmqpValue) section).getValue(), "Underlying message body did not contain the expected content");
    }

    /**
     * Test that setting a null object on a message results in the underlying body
     * section being set with the null object body, ensuring getObject returns null.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetObjectWithNullClearsExistingBodySection() throws Exception {
        Message protonMessage = Message.Factory.create();
        protonMessage.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
        protonMessage.setBody(new Data(new Binary(new byte[0])));

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), protonMessage);

        assertNotNull(amqpObjectMessageFacade.getBody(), "Expected existing body section to be found");
        amqpObjectMessageFacade.setObject(null);
        assertSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.getBody(), "Expected existing body section to be replaced");
        assertNull(amqpObjectMessageFacade.getObject(), "Expected null object");
    }

    /**
     * Test that clearing the body on a message results in the underlying body
     * section being set with the null object body, ensuring getObject returns null.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyWithExistingSerializedBodySection() throws Exception {
        Message protonMessage = Message.Factory.create();
        protonMessage.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
        protonMessage.setBody(new Data(new Binary(new byte[0])));

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), protonMessage);

        assertNotNull(amqpObjectMessageFacade.getBody(), "Expected existing body section to be found");
        amqpObjectMessageFacade.clearBody();
        assertSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.getBody(), "Expected existing body section to be replaced");
        assertNull(amqpObjectMessageFacade.getObject(), "Expected null object");
    }

    /**
     * Test that setting an object on a new message and later getting the value, returns an
     * equal but different object that does not pick up intermediate changes to the set object.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetThenGetObjectOnSerializedMessageReturnsSnapshot() throws Exception {
        HashMap<String, String> origMap = new HashMap<String, String>();
        origMap.put("key1", "value1");

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(false);
        amqpObjectMessageFacade.setObject(origMap);

        // verify we get a different-but-equal object back
        Serializable serialized = amqpObjectMessageFacade.getObject();
        assertTrue(serialized instanceof Map<?, ?>, "Unexpected object type returned");
        Map<?, ?> returnedObject1 = (Map<?, ?>) serialized;
        assertNotSame(origMap, returnedObject1, "Expected different objects, due to snapshot being taken");
        assertEquals(origMap, returnedObject1, "Expected equal objects, due to snapshot being taken");

        // mutate the original object
        origMap.put("key2", "value2");

        // verify we get a different-but-equal object back when compared to the previously retrieved object
        Serializable serialized2 = amqpObjectMessageFacade.getObject();
        assertTrue(serialized2 instanceof Map<?, ?>, "Unexpected object type returned");
        Map<?, ?> returnedObject2 = (Map<?, ?>) serialized2;
        assertNotSame(origMap, returnedObject2, "Expected different objects, due to snapshot being taken");
        assertEquals(returnedObject1, returnedObject2, "Expected equal objects, due to snapshot being taken");

        // verify the mutated map is a different and not equal object
        assertNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
        assertNotEquals(origMap, returnedObject2, "Expected objects to differ, due to snapshot being taken");
    }

    // ---------- test handling of received messages -------------------------//

    @Test
    public void testGetObjectUsingReceivedMessageWithNoBodySectionNoContentTypeReturnsNull() throws Exception {
        doGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(true);
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithNoBodySectionReturnsNull() throws Exception {
        doGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(false);
    }

    private void doGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(boolean amqpTyped) throws IOException, ClassNotFoundException {
        Message message = Message.Factory.create();
        if (!amqpTyped) {
            message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
        }
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), message);

        assertNull(amqpObjectMessageFacade.getObject(), "Expected null object");
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithDataSectionContainingNothingReturnsNull() throws Exception {
        Message message = Message.Factory.create();
        message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
        message.setBody(new Data(null));

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), message);

        assertNull(amqpObjectMessageFacade.getObject(), "Expected null object");
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithNonDataNonAmqvValueBinarySectionThrowsISE() throws Exception {
        Message message = Message.Factory.create();
        message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
        message.setBody(new AmqpValue("nonBinarySectionContent"));

        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), message);

        try {
            amqpObjectMessageFacade.getObject();
            fail("Expected exception to be thrown");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    /**
     * Test that setting an object on a received message and later getting the value, returns an
     * equal but different object that does not pick up intermediate changes to the set object.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetThenGetObjectOnSerializedReceivedMessageNoContentTypeReturnsSnapshot() throws Exception {
        doTestSetThenGetObjectOnSerializedReceivedMessageReturnsSnapshot(false);
    }

    @Test
    public void testSetThenGetObjectOnSerializedReceivedMessageReturnsSnapshot() throws Exception {
        doTestSetThenGetObjectOnSerializedReceivedMessageReturnsSnapshot(true);
    }

    @SuppressWarnings("unchecked")
    private void doTestSetThenGetObjectOnSerializedReceivedMessageReturnsSnapshot(boolean contentType) throws Exception {

        HashMap<String, String> origMap = new HashMap<String, String>();
        origMap.put("key1", "value1");

        Message message = Message.Factory.create();
        if (contentType) {
            message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());
            message.setBody(new Data(new Binary(getSerializedBytes(origMap))));
        } else {
            message.setBody(new AmqpValue(origMap));
        }
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), message);

        // verify we get a different-but-equal object back
        Serializable serialized = amqpObjectMessageFacade.getObject();
        assertTrue(serialized instanceof Map<?, ?>, "Unexpected object type returned");
        Map<String, String> returnedObject1 = (Map<String, String>) serialized;
        assertNotSame(origMap, returnedObject1, "Expected different objects, due to snapshot being taken");
        assertEquals(origMap, returnedObject1, "Expected equal objects, due to snapshot being taken");

        // verify we get a different-but-equal object back when compared to the previously retrieved object
        Serializable serialized2 = amqpObjectMessageFacade.getObject();
        assertTrue(serialized2 instanceof Map<?, ?>, "Unexpected object type returned");
        Map<String, String> returnedObject2 = (Map<String, String>) serialized2;
        assertNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
        assertEquals(returnedObject1, returnedObject2, "Expected equal objects, due to snapshot being taken");

        // mutate the first returned object
        returnedObject1.put("key2", "value2");

        // verify the mutated map is a different and not equal object
        assertNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
        assertNotEquals(returnedObject1, returnedObject2, "Expected objects to differ, due to snapshot being taken");
    }

    private static byte[] getSerializedBytes(Serializable value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(value);
            oos.flush();
            oos.close();

            return baos.toByteArray();
        }
    }
}
