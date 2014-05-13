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
package org.apache.qpid.jms.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpObjectMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ObjectMessageImplTest extends QpidJmsTestCase
{
    private Delivery _mockDelivery;
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private AmqpConnection _mockAmqpConnection;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
    }

    /**
     * Test that attempting to write bytes to a received message (without calling {@link ObjectMessage#clearBody()} first)
     * causes a {@link MessageNotWriteableException} to be thrown due to being read-only.
     */
    @Test
    public void testReceivedObjectMessageThrowsMessageNotWriteableExceptionOnSetObject() throws Exception
    {
        String content = "myStringContent";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(content);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(_mockDelivery, message, _mockAmqpConnection, false);
        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(amqpSerializedObjectMessage, _mockSessionImpl,_mockConnectionImpl, null);

        try
        {
            objectMessageImpl.setObject("newObject");
            fail("Expected exception to be thrown");
        }
        catch(MessageNotWriteableException mnwe)
        {
            //expected
        }
    }

    /**
     * Test that calling {@link ObjectMessage#clearBody()} causes a received
     * message to become writable
     */
    @Test
    public void testClearBodyOnReceivedObjectMessageMakesMessageWritable() throws Exception
    {
        String content = "myStringContent";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(content);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(_mockDelivery, message, _mockAmqpConnection, false);
        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(amqpSerializedObjectMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertFalse("Message should not be writable", objectMessageImpl.isBodyWritable());

        objectMessageImpl.clearBody();

        assertTrue("Message should be writable", objectMessageImpl.isBodyWritable());
    }

    /**
     * Test that calling {@link ObjectMessage#clearBody()} of a received message
     * causes the body of the underlying {@link AmqpObjectMessage} to be emptied.
     */
    @Test
    public void testClearBodyOnReceivedObjectMessageClearsUnderlyingMessageBody() throws Exception
    {
        String content = "myStringContent";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(content);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        Message message = Proton.message();
        message.setBody(new Data(new Binary(bytes)));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(_mockDelivery, message, _mockAmqpConnection, false);
        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(amqpSerializedObjectMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertNotNull("Expected body section but none was present", message.getBody());

        objectMessageImpl.clearBody();

        //check that the returned object is now null
        assertNull("Unexpected object value", objectMessageImpl.getObject());

        //verify the underlying message has no body section
        //TODO: this test assumes we can omit the body section. If we decide otherwise it
        //should instead check for e.g. a data section containing a serialized null object
        assertNull("Expected no body section", message.getBody());
    }

    /**
     * Test that setting an object on a new message and later getting the value, returns an
     * equal but different object that does not pick up intermediate changes to the set object.
     */
    @Test
    public void testSetThenGetObjectReturnsSnapshot() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        origMap.put("key1", "value1");

        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        objectMessageImpl.setObject((Serializable) origMap);

        //verify we get a different-but-equal object back
        Serializable serialized = objectMessageImpl.getObject();
        assertTrue("Unexpected object type returned", serialized instanceof Map<?,?>);
        Map<?,?> returnedObject1 = (Map<?,?>) serialized;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject1);
        assertEquals("Expected equal objects, due to snapshot being taken", origMap, returnedObject1);

        //mutate the original object
        origMap.put("key2", "value2");

        //verify we get a different-but-equal object back when compared to the previously retrieved object
        Serializable serialized2 = objectMessageImpl.getObject();
        assertTrue("Unexpected object type returned", serialized2 instanceof Map<?,?>);
        Map<?,?> returnedObject2 = (Map<?,?>) serialized2;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject2);
        assertEquals("Expected equal objects, due to snapshot being taken", returnedObject1, returnedObject2);

        //verify the mutated map is a different and not equal object
        assertNotSame("Expected different objects, due to snapshot being taken", returnedObject1, returnedObject2);
        assertNotEquals("Expected objects to differ, due to snapshot being taken", origMap, returnedObject2);
    }

    /**
     * Test that setting an object on a new message which contains non-serializable content results
     * in an {@link MessageFormatException} being thrown due to failure to encode the object.
     */
    @Test
    public void testSetObjectWithNonSerializableThrowsJMSMFE() throws Exception
    {
        Map<String,Object> origMap = new HashMap<String,Object>();
        origMap.put("key1", "value1");
        origMap.put("notSerializable", new NotSerializable());

        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            objectMessageImpl.setObject((Serializable) origMap);
            fail("Expected exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    //Test class
    private static class NotSerializable
    {
        public NotSerializable()
        {
        }
    }

    /**
     * Test that failure during deserialization of an object in a message results
     * in an {@link MessageFormatException} being throw.
     */
    @Test
    public void testGetObjectWithFailedDeserialisationThrowsJMSMFE() throws Exception
    {
        Map<String,Object> origMap = new HashMap<String,Object>();
        origMap.put("key1", "value1");
        origMap.put("notSerializable", new NotSerializable());

        AmqpObjectMessage amqpSerializedObjectMessage = Mockito.mock(AmqpObjectMessage.class);
        Mockito.when(amqpSerializedObjectMessage.getObject()).thenThrow(new ClassNotFoundException());

        ObjectMessageImpl objectMessageImpl = new ObjectMessageImpl(amqpSerializedObjectMessage, _mockSessionImpl,_mockConnectionImpl, null);

        try
        {
            objectMessageImpl.getObject();
            fail("Expected exception to be thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }
}
