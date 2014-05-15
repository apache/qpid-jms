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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpObjectMessageTest extends QpidJmsTestCase
{
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
        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage();
        assertTrue("expected message type annotation to be present", amqpSerializedObjectMessage.messageAnnotationExists(ClientProperties.X_OPT_JMS_MSG_TYPE));
        assertEquals("unexpected value for message type annotation value", ClientProperties.OBJECT_MESSSAGE_TYPE, amqpSerializedObjectMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE));
    }

    @Test
    public void testGetObjectWithNewMessageToSendReturnsNull() throws Exception
    {
        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage();

        assertNull("Expected null object initially", amqpSerializedObjectMessage.getObject());
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithNoBodySectionReturnsNull() throws Exception
    {
        Message message = Proton.message();
        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(message, _mockDelivery, _mockAmqpConnection, false);

        assertNull("Expected null object", amqpSerializedObjectMessage.getObject());
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithDataSectionContainingNothingReturnsNull() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new Data(null));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(message, _mockDelivery, _mockAmqpConnection, false);

        assertNull("Expected null object", amqpSerializedObjectMessage.getObject());
    }

    @Test
    public void testGetObjectUsingReceivedMessageWithNonDataSectionThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue("doesntMatter"));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(message, _mockDelivery, _mockAmqpConnection, false);

        try
        {
            amqpSerializedObjectMessage.getObject();
            fail("Expected exception to be thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    /**
     * Test that setting an object on a new message results in the expected
     * content in the body section of the underlying message.
     */
    @Test
    public void testSetObjectOnNewMessage() throws Exception
    {
        String content = "myStringContent";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(content);
        oos.flush();
        oos.close();
        byte[] bytes = baos.toByteArray();

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage();
        amqpSerializedObjectMessage.setObject(content);

        Message protonMessage = amqpSerializedObjectMessage.getMessage();

        //retrieve the bytes from the underlying message, check they match expectation
        Data body = (Data) protonMessage.getBody();
        assertTrue("Underlying message data section did not contain the expected bytes", Arrays.equals(bytes, body.getValue().getArray()));
    }

    /**
     * Test that setting a null object on a message results in the underlying
     * body section being cleared, ensuring getObject returns null.
     */
    @Test
    public void testSetObjectWithNullClearsExistingBodySection() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new Data(new Binary(new byte[0])));

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage(message, _mockDelivery, _mockAmqpConnection, false);

        assertNotNull("Expected existing body section to be found", message.getBody());
        amqpSerializedObjectMessage.setObject(null);
        assertNull("Expected existing body section to be cleared", message.getBody());
        assertNull("Expected null object", amqpSerializedObjectMessage.getObject());
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

        AmqpObjectMessage amqpSerializedObjectMessage = new AmqpObjectMessage();
        amqpSerializedObjectMessage.setObject((Serializable) origMap);

        //verify we get a different-but-equal object back
        Serializable serialized = amqpSerializedObjectMessage.getObject();
        assertTrue("Unexpected object type returned", serialized instanceof Map<?,?>);
        Map<?,?> returnedObject1 = (Map<?,?>) serialized;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject1);
        assertEquals("Expected equal objects, due to snapshot being taken", origMap, returnedObject1);

        //mutate the original object
        origMap.put("key2", "value2");

        //verify we get a different-but-equal object back when compared to the previously retrieved object
        Serializable serialized2 = amqpSerializedObjectMessage.getObject();
        assertTrue("Unexpected object type returned", serialized2 instanceof Map<?,?>);
        Map<?,?> returnedObject2 = (Map<?,?>) serialized2;
        assertNotSame("Expected different objects, due to snapshot being taken", origMap, returnedObject2);
        assertEquals("Expected equal objects, due to snapshot being taken", returnedObject1, returnedObject2);

        //verify the mutated map is a different and not equal object
        assertNotSame("Expected different objects, due to snapshot being taken", returnedObject1, returnedObject2);
        assertNotEquals("Expected objects to differ, due to snapshot being taken", origMap, returnedObject2);
    }
}
