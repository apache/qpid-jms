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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpMapMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MapMessageImplTest extends QpidJmsTestCase
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

    @Test
    public void testGetMapNamesWithNewMessageToSendReturnsEmptyEnumeration() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        Enumeration<?> names = mapMessageImpl.getMapNames();

        assertFalse("Expected new message to have no map names", names.hasMoreElements());
    }

    /**
     * Test that we are able to retrieve the names and values of map entries on a received message
     */
    @Test
    public void testGetMapNamesUsingReceivedMessageReturnsExpectedEnumeration() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        String myKey2 = "key2";
        origMap.put(myKey1, "value1");
        origMap.put(myKey2, "value2");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        MapMessageImpl mapMessageImpl = new MapMessageImpl(amqpMapMessage, _mockSessionImpl,_mockConnectionImpl, null);
        Enumeration<?> names = mapMessageImpl.getMapNames();

        int count = 0;
        List<Object> elements = new ArrayList<Object>();
        while(names.hasMoreElements())
        {
            count++;
            elements.add(names.nextElement());
        }
        assertEquals("expected 2 map keys in enumeration", 2,count);
        assertTrue("expected key was not found: " + myKey1, elements.contains(myKey1));
        assertTrue("expected key was not found: " + myKey2, elements.contains(myKey2));
    }

    /**
     * Test that we enforce the requirement that map message key names not be null or the empty string.
     */
    @Test
    public void testSetObjectWithNullOrEmptyKeyNameThrowsIAE() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        try
        {
            mapMessageImpl.setObject(null, "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        try
        {
            mapMessageImpl.setObject("", "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    /**
     * Test that we are not able to write to a received message without calling {@link MapMessageImpl#clearBody()}
     */
    @Test
    public void testReceivedMessageIsReadOnlyAndThrowsMNWE() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        origMap.put(myKey1, "value1");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        MapMessageImpl mapMessageImpl = new MapMessageImpl(amqpMapMessage, _mockSessionImpl,_mockConnectionImpl, null);

        try
        {
            mapMessageImpl.setObject("name", "value");
            fail("expected exception to be thrown");
        }
        catch(MessageNotWriteableException mnwe)
        {
            //expected
        }
    }

    /**
     * Test that calling {@link MapMessageImpl#clearBody()} makes a received message writable
     */
    @Test
    public void testClearBodyMakesReceivedMessageWritable() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        origMap.put(myKey1, "value1");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        MapMessageImpl mapMessageImpl = new MapMessageImpl(amqpMapMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertFalse("expected message to be read-only", mapMessageImpl.isBodyWritable());
        mapMessageImpl.clearBody();
        assertTrue("expected message to be writable", mapMessageImpl.isBodyWritable());
        mapMessageImpl.setObject("name", "value");
    }

    /**
     * Test that calling {@link MapMessageImpl#clearBody()} clears the underlying message body map.
     */
    @Test
    public void testClearBodyClearsUnderlyingMessageMap() throws Exception
    {
        Map<String,String> origMap = new HashMap<String,String>();
        String myKey1 = "key1";
        origMap.put(myKey1, "value1");

        Message message = Proton.message();
        message.setBody(new AmqpValue(origMap));
        AmqpMapMessage amqpMapMessage = new AmqpMapMessage(message, _mockDelivery, _mockAmqpConnection);

        MapMessageImpl mapMessageImpl = new MapMessageImpl(amqpMapMessage, _mockSessionImpl,_mockConnectionImpl, null);

        assertTrue("key should exist: " + myKey1, mapMessageImpl.itemExists(myKey1));
        mapMessageImpl.clearBody();
        assertTrue("expected map to be emptied", origMap.isEmpty());
        assertFalse("key should not exist", mapMessageImpl.itemExists(myKey1));
    }

    /**
     * Test that the {@link MapMessageImpl#setObject(String, Object)} method rejects Objects of unexpected types
     */
    @Test
    public void testSetObjectWithIllegalTypeThrowsMFE() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        try
        {
            mapMessageImpl.setObject("myPKey", new Exception());
            fail("Expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testSetGetObject() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);
        String keyName = "myProperty";

        Object entryValue = null;
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Boolean.valueOf(false);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Byte.valueOf((byte)1);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Short.valueOf((short)2);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Integer.valueOf(3);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Long.valueOf(4);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Float.valueOf(5.01F);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Double.valueOf(6.01);
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = "string";
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = Character.valueOf('c');
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));

        entryValue = new byte[] { (byte)1, (byte) 0, (byte)1};
        mapMessageImpl.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessageImpl.getObject(keyName));
    }

    // ======= Strings =========

    @Test
    public void testSetGetString() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        //null value
        String name = "myNullString";
        String value = null;

        assertFalse(mapMessageImpl.itemExists(name));
        mapMessageImpl.setString(name, value);
        assertTrue(mapMessageImpl.itemExists(name));
        assertEquals(value, mapMessageImpl.getString(name));

        //non-null value
        name = "myName";
        value = "myValue";

        assertFalse(mapMessageImpl.itemExists(name));
        mapMessageImpl.setString(name, value);
        assertTrue(mapMessageImpl.itemExists(name));
        assertEquals(value, mapMessageImpl.getString(name));
    }

    /**
     * Set a String, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetStringGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myStringName";
        String value;

        //boolean
        value =  "true";
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Boolean.valueOf(value), Boolean.class);

        //byte
        value =  String.valueOf(Byte.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Byte.valueOf(value), Byte.class);

        //short
        value =  String.valueOf(Short.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Short.valueOf(value), Short.class);

        //int
        value =  String.valueOf(Integer.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Integer.valueOf(value), Integer.class);

        //long
        value =  String.valueOf(Long.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Long.valueOf(value), Long.class);

        //float
        value =  String.valueOf(Float.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Float.valueOf(value), Float.class);

        //double
        value =  String.valueOf(Double.MAX_VALUE);
        mapMessageImpl.setString(name, value);
        assertGetMapEntryEquals(mapMessageImpl, name, Double.valueOf(value), Double.class);
    }

    /**
     * Set a String, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetStringGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        String value = "myStringValue";

        mapMessageImpl.setString(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
    }

    // ======= boolean =========

    /**
     * Set a boolean, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetBooleanGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        boolean value = true;

        mapMessageImpl.setBoolean(name, value);
        assertEquals("value not as expected", value, mapMessageImpl.getBoolean(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
    }

    /**
     * Set a boolean, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetBooleanGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        boolean value = true;

        mapMessageImpl.setBoolean(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Double.class);
    }

    // ======= byte =========

    /**
     * Set a byte, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetByteGetLegalProperty() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        byte value = (byte)1;

        mapMessageImpl.setByte(name, value);
        assertEquals(value, mapMessageImpl.getByte(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Short.valueOf(value), Short.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Integer.valueOf(value), Integer.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set a byte, then retrieve it as all of the illegal type combinations to verify it is fails as expected
     */
    @Test
    public void testSetByteGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        byte value = (byte)1;

        mapMessageImpl.setByte(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Double.class);
    }


    // ======= short =========

    /**
     * Set a short, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetShortGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        short value = (short)1;

        mapMessageImpl.setShort(name, value);
        assertEquals(value, mapMessageImpl.getShort(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Integer.valueOf(value), Integer.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set a short, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetShortGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        short value = (short)1;

        mapMessageImpl.setShort(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Double.class);
    }


    // ======= int =========

    /**
     * Set an int, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetIntGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        int value = (int)1;

        mapMessageImpl.setInt(name, value);
        assertEquals(value, mapMessageImpl.getInt(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set an int, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetIntGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        int value = (int)1;

        mapMessageImpl.setInt(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Double.class);
    }

    // ======= long =========

    /**
     * Set a long, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetLongGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        long value = Long.MAX_VALUE;

        mapMessageImpl.setLong(name, value);
        assertEquals(value, mapMessageImpl.getLong(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
    }

    /**
     * Set an long, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetLongGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        long value = Long.MAX_VALUE;

        mapMessageImpl.setLong(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Double.class);
    }

    // ======= float =========

    /**
     * Set a float, then retrieve it as all of the legal type combinations to verify it is parsed correctly
     */
    @Test
    public void testSetFloatGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        float value = Float.MAX_VALUE;

        mapMessageImpl.setFloat(name, value);
        assertEquals(value, mapMessageImpl.getFloat(name), 0.0);

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessageImpl, name, Double.valueOf(value), Double.class);
    }

    /**
     * Set a float, then retrieve it as all of the illegal type combinations to verify it fails as expected
     */
    @Test
    public void testSetFloatGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        float value = Float.MAX_VALUE;

        mapMessageImpl.setFloat(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Long.class);
    }

    // ======= double  =========

    @Test
    public void testSetDoubleGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        double value = Double.MAX_VALUE;

        mapMessageImpl.setDouble(name, value);
        assertEquals(value, mapMessageImpl.getDouble(name), 0.0);

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
    }

    @Test
    public void testSetDoubleGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        double value = Double.MAX_VALUE;

        mapMessageImpl.setDouble(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
    }

    // ======= character =========

    @Test
    public void testSetCharGetLegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        char value = 'c';

        mapMessageImpl.setChar(name, value);
        assertEquals(value, mapMessageImpl.getChar(name));

        assertGetMapEntryEquals(mapMessageImpl, name, String.valueOf(value), String.class);
    }

    @Test
    public void testSetCharGetIllegal() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        String name = "myName";
        char value = 'c';

        mapMessageImpl.setChar(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessageImpl, name, Float.class);
    }

    /**
     * Verify behaviour when retrieving a character with null value (i.e missing).
     * Unlike all the other types, this should throw an explicit NPE.
     */
    @Test
    public void testGetCharWithMissingValueThrowsNPE() throws Exception
    {
        MapMessageImpl mapMessageImpl = new MapMessageImpl(_mockSessionImpl,_mockConnectionImpl);

        try
        {
            mapMessageImpl.getChar("does.not.exist");
            fail("expected exception to be thrown");
        }
        catch(NullPointerException npe)
        {
            //expected
        }
    }

    //========= utility methods ========

    private void assertGetMapEntryEquals(MapMessageImpl testMessage, String name,
                                         Object expectedValue,
                                         Class<?> clazz) throws JMSException
    {
        Object actualValue = getMapEntryUsingTypeMethod(testMessage, name, clazz);
        assertEquals(expectedValue, actualValue);
    }

    private void assertGetMapEntryThrowsMessageFormatException(MapMessageImpl testMessage,
                                                               String name,
                                                               Class<?> clazz) throws JMSException
    {
        try
        {
            getMapEntryUsingTypeMethod(testMessage, name, clazz);

            fail("expected exception to be thrown");
        }
        catch(MessageFormatException jmsMFE)
        {
            //expected
        }
    }

    private Object getMapEntryUsingTypeMethod(MapMessageImpl testMessage, String name, Class<?> clazz) throws JMSException
    {
        if(clazz == Boolean.class)
        {
            return testMessage.getBoolean(name);
        }
        else if(clazz == Byte.class)
        {
            return testMessage.getByte(name);
        }
        else if(clazz == Character.class)
        {
            return testMessage.getChar(name);
        }
        else if(clazz == Short.class)
        {
            return testMessage.getShort(name);
        }
        else if(clazz == Integer.class)
        {
            return testMessage.getInt(name);
        }
        else if(clazz == Long.class)
        {
            return testMessage.getLong(name);
        }
        else if(clazz == Float.class)
        {
            return testMessage.getFloat(name);
        }
        else if(clazz == Double.class)
        {
            return testMessage.getDouble(name);
        }
        else if(clazz == String.class)
        {
            return testMessage.getString(name);
        }
        else if(clazz == byte[].class)
        {
            return testMessage.getBytes(name);
        }
        else
        {
            throw new RuntimeException("Unexpected entry type class");
        }
    }
}
