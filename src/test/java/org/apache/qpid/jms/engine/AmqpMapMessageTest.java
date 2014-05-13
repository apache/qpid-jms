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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpMapMessageTest extends QpidJmsTestCase
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
    public void testNewMessageToSendContainsAmqpValueBodyWithMap() throws Exception
    {
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage();
        assertTrue(amqpMapMessage.getMessage().getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue)amqpMapMessage.getMessage().getBody()).getValue() instanceof Map<?,?>);
    }

    @Test
    public void testGetMapNamesWithNewMessageToSendReturnsEmptySet() throws Exception
    {
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage();

        assertEquals("expected empty set", 0, amqpMapMessage.getMapKeys().size());
    }

    @Test
    public void testGetMapNamesUsingReceivedMessageWithNoBodySectionReturnsEmptySet() throws Exception
    {
        Message message = Proton.message();
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected empty set", 0, amqpMapMessage.getMapKeys().size());
    }

    @Test
    public void testGetMapNamesUsingReceivedMessageWithEmptyAmqpValueBodyReturnsExpectedSet() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected empty set", 0, amqpMapMessage.getMapKeys().size());
    }

    @Test
    public void testGetMapNamesUsingReceivedMessageWithAmqpValueMapBodyReturnsExpectedSet() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        String myKey2 = "key2";
        origMap.put(myKey1, "value1");
        origMap.put(myKey2, "value2");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected 2 map keys in set", 2, amqpMapMessage.getMapKeys().size());
        assertTrue("expected key was not found: " + myKey1, amqpMapMessage.getMapKeys().contains(myKey1));
        assertTrue("expected key was not found: " + myKey2, amqpMapMessage.getMapKeys().contains(myKey2));
    }

    @Test
    public void testClearMapEntriesUsingReceivedMessageWithAmqpValueMapBody() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        String myKey2 = "key2";
        origMap.put(myKey1, "value1");
        origMap.put(myKey2, "value2");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected 2 map keys in set", 2, amqpMapMessage.getMapKeys().size());
        amqpMapMessage.clearMapEntries();
        assertEquals("expected empty set", 0, amqpMapMessage.getMapKeys().size());
    }

    @Test
    public void testGetMapEntryUsingReceivedMessageWithAmqpValueMapBody() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        String myKey2 = "key2";
        String myValue1 = "value1";
        String myValue2 = "value2";
        origMap.put(myKey1, myValue1);
        origMap.put(myKey2, myValue2);

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        Object val1 = amqpMapMessage.getMapEntry(myKey1);
        assertEquals("expected value was not found for key" + myKey1 + ": " + myKey1, myValue1, val1);
        Object val2 = amqpMapMessage.getMapEntry(myKey2);
        assertEquals("expected value was not found for key" + myKey2 + ": " + myKey2, myValue2, val2);
    }

    @Test
    public void testSetEntryOnNewMessageUpdatesUnderlyingMessageBody() throws Exception
    {
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage();
        String myKey1 = "key1";
        String myValue1 = "value1";

        Map<?,?> bodyMap = (Map<?,?>) ((AmqpValue)amqpMapMessage.getMessage().getBody()).getValue();
        assertTrue("expected empty map", bodyMap.isEmpty());

        amqpMapMessage.setMapEntry(myKey1, myValue1);

        assertEquals("expected map to be updated", 1, amqpMapMessage.getMapKeys().size());
        assertTrue("expected key was not found: " + myKey1, bodyMap.containsKey(myKey1));
        assertEquals("expected value was not found: " + myValue1, myValue1, bodyMap.get(myKey1));
    }

    @Test
    public void testCreateAmqpMapMessageWithUnexpectedBodySectionTypeThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));

        try
        {
            new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);
            fail("expected exception not thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    @Test
    public void testMapEntryExistsUsingReceivedMessage() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        String myKey2 = "key2";
        origMap.put(myKey1, "value1");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        assertTrue("expected key was not found: " + myKey1, amqpMapMessage.mapEntryExists(myKey1));
        assertFalse("key should not have been found: " + myKey2, amqpMapMessage.mapEntryExists(myKey2));
    }
}
