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
package org.apache.qpid.jms.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.apache.qpid.jms.message.facade.test.JmsTestObjectMessageFacade;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 */
public class JmsObjectMessageTest {

    private final JmsMessageFactory factory = new JmsTestMessageFactory();

    /**
     * Test that attempting to write bytes to a received message (without calling {@link ObjectMessage#clearBody()} first)
     * causes a {@link MessageNotWriteableException} to be thrown due to being read-only.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testReceivedObjectMessageThrowsMessageNotWriteableExceptionOnSetObject() throws Exception {
        String content = "myStringContent";
        JmsObjectMessageFacade facade = new JmsTestObjectMessageFacade();
        facade.setObject(content);
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.onDispatch();

        try {
            objectMessage.setObject("newObject");
            fail("Expected exception to be thrown");
        } catch (MessageNotWriteableException mnwe) {
            // expected
        }
    }

    /**
     * Test that calling {@link ObjectMessage#toString()} returns a meaningful value
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testToString() throws Exception {
        String content = "myStringContent";
        JmsObjectMessageFacade facade = new JmsTestObjectMessageFacade();
        facade.setObject(content);
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.onDispatch();

        assertTrue(objectMessage.toString().startsWith("JmsObjectMessageFacade"));
    }

    /**
     * Test that calling {@link ObjectMessage#clearBody()} causes a received
     * message to become writable
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyOnReceivedObjectMessageMakesMessageWritable() throws Exception {
        String content = "myStringContent";
        JmsObjectMessageFacade facade = new JmsTestObjectMessageFacade();
        facade.setObject(content);
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.onDispatch();

        assertTrue("Message should not be writable", objectMessage.isReadOnlyBody());
        objectMessage.clearBody();
        assertFalse("Message should be writable", objectMessage.isReadOnlyBody());
    }

    /**
     * Test that calling {@link ObjectMessage#clearBody()} of a received message
     * causes the body of the underlying message facade to be emptied.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyOnReceivedObjectMessageClearsUnderlyingMessageBody() throws Exception {
        String content = "myStringContent";
        JmsTestObjectMessageFacade facade = new JmsTestObjectMessageFacade();
        facade.setObject(content);
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.onDispatch();

        assertNotNull("Expected body section but none was present", facade.getSerializedObject());
        objectMessage.clearBody();

        // check that the returned object is now null
        assertNull("Unexpected object value", objectMessage.getObject());

        // verify the underlying message facade has no body
        assertNull("Expected no body section", facade.getSerializedObject());
    }

    /**
     * Test that setting an object on a new message and later getting the value, returns an
     * equal but different object that does not pick up intermediate changes to the set object.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetThenGetObjectReturnsSnapshot() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        origMap.put("key1", "value1");

        JmsTestObjectMessageFacade facade = new JmsTestObjectMessageFacade();
        facade.setObject((Serializable) origMap);
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.onDispatch();

        // verify we get a different-but-equal object back
        Serializable serialized = objectMessage.getObject();
        assertTrue("Unexpected object type returned", serialized instanceof Map<?,?>);
        Map<?,?> returnedObject1 = (Map<?,?>) serialized;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject1);
        assertEquals("Expected equal objects, due to snapshot being taken", origMap, returnedObject1);

        // mutate the original object
        origMap.put("key2", "value2");

        // verify we get a different-but-equal object back when compared to the previously retrieved object
        Serializable serialized2 = objectMessage.getObject();
        assertTrue("Unexpected object type returned", serialized2 instanceof Map<?,?>);
        Map<?,?> returnedObject2 = (Map<?,?>) serialized2;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject2);
        assertEquals("Expected equal objects, due to snapshot being taken", returnedObject1, returnedObject2);

        // verify the mutated map is a different and not equal object
        assertNotSame("Expected different objects, due to snapshot being taken", returnedObject1, returnedObject2);
        assertNotEquals("Expected objects to differ, due to snapshot being taken", origMap, returnedObject2);
    }

    /**
     * Test that setting an object on a new message which contains non-serializable content results
     * in an {@link MessageFormatException} being thrown due to failure to encode the object.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetObjectWithNonSerializableThrowsJMSMFE() throws Exception {
        Map<String, Object> origMap = new HashMap<String, Object>();
        origMap.put("key1", "value1");
        origMap.put("notSerializable", new NotSerializable());

        JmsObjectMessage objectMessage = factory.createObjectMessage();

        try {
            objectMessage.setObject((Serializable) origMap);
            fail("Expected exception to be thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }
    }

    // Test class
    private static class NotSerializable {
        public NotSerializable() {}
    }

    /**
     * Test that failure during deserialization of an object in a message results
     * in an {@link MessageFormatException} being throw.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=MessageFormatException.class)
    public void testGetObjectWithFailedDeserialisationThrowsJMSMFE() throws Exception {
        JmsObjectMessageFacade facade = Mockito.mock(JmsTestObjectMessageFacade.class);
        Mockito.when(facade.getObject()).thenThrow(new ClassCastException("Failed to get object"));
        JmsObjectMessage objectMessage = new JmsObjectMessage(facade);
        objectMessage.getObject();
    }

    @Test
    public void testBytes() throws JMSException, IOException {
        JmsObjectMessage msg = factory.createObjectMessage();
        String str = "testText";
        msg.setObject(str);

        msg = msg.copy();
        assertEquals(msg.getObject(), str);
    }

    @Test
    public void testSetObject() throws JMSException {
        JmsObjectMessage msg = factory.createObjectMessage();
        String str = "testText";
        msg.setObject(str);
        assertEquals(str, msg.getObject());
    }

    @Test
    public void testClearBody() throws JMSException {
        JmsObjectMessage objectMessage = factory.createObjectMessage();
        try {
            objectMessage.setObject("String");
            objectMessage.clearBody();
            assertFalse(objectMessage.isReadOnlyBody());
            assertNull(objectMessage.getObject());
            objectMessage.setObject("String");
            objectMessage.getObject();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        JmsObjectMessage msg = factory.createObjectMessage();
        msg.setObject("test");
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        try {
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotWriteableException e) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException { // should always be readable
        JmsObjectMessage msg = factory.createObjectMessage();
        msg.setReadOnlyBody(false);
        try {
            msg.setObject("test");
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        } catch (MessageNotWriteableException mnwe) {
        }
    }
}
