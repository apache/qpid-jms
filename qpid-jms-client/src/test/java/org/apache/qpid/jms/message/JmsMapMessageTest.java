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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.message.facade.JmsMapMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMapMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.Test;

/**
 * Test that the JMS level JmsMapMessage using a simple default message facade follows
 * the JMS spec.
 */
public class JmsMapMessageTest {

    private final JmsMessageFactory factory = new JmsTestMessageFactory();

    // ======= general =========

    @Test
    public void testToString() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();
        assertTrue(mapMessage.toString().startsWith("JmsMapMessage"));
    }

    @Test
    public void testGetMapNamesWithNewMessageToSendReturnsEmptyEnumeration() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();
        Enumeration<?> names = mapMessage.getMapNames();

        assertFalse("Expected new message to have no map names", names.hasMoreElements());
    }

    /**
     * Test that we are able to retrieve the names and values of map entries on a received
     * message
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetMapNamesUsingReceivedMessageReturnsExpectedEnumeration() throws Exception {
        JmsMapMessageFacade facade = new JmsTestMapMessageFacade();
        String myKey1 = "key1";
        String myKey2 = "key2";
        facade.put(myKey1, "value1");
        facade.put(myKey2, "value2");

        JmsMapMessage mapMessage = new JmsMapMessage(facade);
        Enumeration<?> names = mapMessage.getMapNames();

        int count = 0;
        List<Object> elements = new ArrayList<Object>();
        while (names.hasMoreElements()) {
            count++;
            elements.add(names.nextElement());
        }
        assertEquals("expected 2 map keys in enumeration", 2, count);
        assertTrue("expected key was not found: " + myKey1, elements.contains(myKey1));
        assertTrue("expected key was not found: " + myKey2, elements.contains(myKey2));
    }

    /**
     * Test that we enforce the requirement that map message key names not be null or the empty
     * string.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetObjectWithNullOrEmptyKeyNameThrowsIAE() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();
        try {
            mapMessage.setObject(null, "value");
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            mapMessage.setObject("", "value");
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    /**
     * Test that we are not able to write to a received message without calling
     * {@link JmsMapMessage#clearBody()}
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testReceivedMessageIsReadOnlyAndThrowsMNWE() throws Exception {
        JmsMapMessageFacade facade = new JmsTestMapMessageFacade();
        String myKey1 = "key1";
        facade.put(myKey1, "value1");
        JmsMapMessage mapMessage = new JmsMapMessage(facade);
        mapMessage.onDispatch();

        try {
            mapMessage.setObject("name", "value");
            fail("expected exception to be thrown");
        } catch (MessageNotWriteableException mnwe) {
            // expected
        }
    }

    /**
     * Test that calling {@link JmsMapMessage#clearBody()} makes a received message writable
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyMakesReceivedMessageWritable() throws Exception {
        JmsMapMessageFacade facade = new JmsTestMapMessageFacade();
        String myKey1 = "key1";
        facade.put(myKey1, "value1");

        JmsMapMessage mapMessage = new JmsMapMessage(facade);
        mapMessage.onDispatch();

        assertTrue("expected message to be read-only", mapMessage.isReadOnlyBody());
        mapMessage.clearBody();
        assertFalse("expected message to be writable", mapMessage.isReadOnlyBody());
        mapMessage.setObject("name", "value");
    }

    /**
     * Test that calling {@link JmsMapMessage#clearBody()} clears the underlying message body
     * map.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testClearBodyClearsUnderlyingMessageMap() throws Exception {
        JmsMapMessageFacade facade = new JmsTestMapMessageFacade();
        String myKey1 = "key1";
        facade.put(myKey1, "value1");

        JmsMapMessage mapMessage = new JmsMapMessage(facade);

        assertTrue("key should exist: " + myKey1, mapMessage.itemExists(myKey1));
        mapMessage.clearBody();
        assertFalse("key should not exist", mapMessage.itemExists(myKey1));
    }

    /**
     * When a map entry is not set, the behaviour of JMS specifies that it is equivalent to a
     * null value, and the accessors should either return null, throw NPE, or behave in the same
     * fashion as <primitive>.valueOf(String).
     *
     * Test that this is the case.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetMissingMapEntryResultsInExpectedBehaviour() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "does_not_exist";

        // expect null
        assertNull(mapMessage.getBytes(name));
        assertNull(mapMessage.getString(name));

        // expect false from Boolean.valueOf(null).
        assertFalse(mapMessage.getBoolean(name));

        // expect an NFE from the primitive integral <type>.valueOf(null) conversions
        assertGetMapEntryThrowsNumberFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsNumberFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsNumberFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsNumberFormatException(mapMessage, name, Long.class);

        // expect an NPE from the primitive float, double, and char <type>.valuleOf(null)
        // conversions
        assertGetMapEntryThrowsNullPointerException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsNullPointerException(mapMessage, name, Double.class);
        assertGetMapEntryThrowsNullPointerException(mapMessage, name, Character.class);
    }

    // ======= object =========

    /**
     * Test that the {@link JmsMapMessage#setObject(String, Object)} method rejects Objects of
     * unexpected types
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(expected=MessageFormatException.class)
    public void testSetObjectWithIllegalTypeThrowsMFE() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();
        mapMessage.setObject("myPKey", new Exception());
    }

    @Test
    public void testSetGetObject() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();
        String keyName = "myProperty";

        Object entryValue = null;
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Boolean.valueOf(false);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Byte.valueOf((byte) 1);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Short.valueOf((short) 2);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Integer.valueOf(3);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Long.valueOf(4);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Float.valueOf(5.01F);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Double.valueOf(6.01);
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = "string";
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        entryValue = Character.valueOf('c');
        mapMessage.setObject(keyName, entryValue);
        assertEquals(entryValue, mapMessage.getObject(keyName));

        byte[] bytes = new byte[] { (byte) 1, (byte) 0, (byte) 1 };
        mapMessage.setObject(keyName, bytes);
        Object retrieved = mapMessage.getObject(keyName);
        assertTrue(retrieved instanceof byte[]);
        assertTrue(Arrays.equals(bytes, (byte[]) retrieved));
    }

    // ======= Strings =========

    @Test
    public void testSetGetString() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        // null value
        String name = "myNullString";
        String value = null;

        assertFalse(mapMessage.itemExists(name));
        mapMessage.setString(name, value);
        assertTrue(mapMessage.itemExists(name));
        assertEquals(value, mapMessage.getString(name));

        // non-null value
        name = "myName";
        value = "myValue";

        assertFalse(mapMessage.itemExists(name));
        mapMessage.setString(name, value);
        assertTrue(mapMessage.itemExists(name));
        assertEquals(value, mapMessage.getString(name));
    }

    /**
     * Set a String, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetStringGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myStringName";
        String value;

        // boolean
        value = "true";
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Boolean.valueOf(value), Boolean.class);

        // byte
        value = String.valueOf(Byte.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Byte.valueOf(value), Byte.class);

        // short
        value = String.valueOf(Short.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Short.valueOf(value), Short.class);

        // int
        value = String.valueOf(Integer.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Integer.valueOf(value), Integer.class);

        // long
        value = String.valueOf(Long.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Long.valueOf(value), Long.class);

        // float
        value = String.valueOf(Float.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Float.valueOf(value), Float.class);

        // double
        value = String.valueOf(Double.MAX_VALUE);
        mapMessage.setString(name, value);
        assertGetMapEntryEquals(mapMessage, name, Double.valueOf(value), Double.class);
    }

    /**
     * Set a String, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetStringGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        String value = "myStringValue";

        mapMessage.setString(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
    }

    // ======= boolean =========

    /**
     * Set a boolean, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBooleanGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        boolean value = true;

        mapMessage.setBoolean(name, value);
        assertEquals("value not as expected", value, mapMessage.getBoolean(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);

        mapMessage.setBoolean(name, !value);
        assertEquals("value not as expected", !value, mapMessage.getBoolean(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(!value), String.class);
    }

    /**
     * Set a boolean, then retrieve it as all of the illegal type combinations to verify it
     * fails as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBooleanGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        boolean value = true;

        mapMessage.setBoolean(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ======= byte =========

    /**
     * Set a byte, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetByteGetLegalProperty() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte value = (byte) 1;

        mapMessage.setByte(name, value);
        assertEquals(value, mapMessage.getByte(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessage, name, Short.valueOf(value), Short.class);
        assertGetMapEntryEquals(mapMessage, name, Integer.valueOf(value), Integer.class);
        assertGetMapEntryEquals(mapMessage, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set a byte, then retrieve it as all of the illegal type combinations to verify it is
     * fails as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetByteGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte value = (byte) 1;

        mapMessage.setByte(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ======= short =========

    /**
     * Set a short, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetShortGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        short value = (short) 1;

        mapMessage.setShort(name, value);
        assertEquals(value, mapMessage.getShort(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessage, name, Integer.valueOf(value), Integer.class);
        assertGetMapEntryEquals(mapMessage, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set a short, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetShortGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        short value = (short) 1;

        mapMessage.setShort(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ======= int =========

    /**
     * Set an int, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetIntGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        int value = 1;

        mapMessage.setInt(name, value);
        assertEquals(value, mapMessage.getInt(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessage, name, Long.valueOf(value), Long.class);
    }

    /**
     * Set an int, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetIntGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        int value = 1;

        mapMessage.setInt(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ======= long =========

    /**
     * Set a long, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetLongGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        long value = Long.MAX_VALUE;

        mapMessage.setLong(name, value);
        assertEquals(value, mapMessage.getLong(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
    }

    /**
     * Set an long, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetLongGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        long value = Long.MAX_VALUE;

        mapMessage.setLong(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ======= float =========

    /**
     * Set a float, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetFloatGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        float value = Float.MAX_VALUE;

        mapMessage.setFloat(name, value);
        assertEquals(value, mapMessage.getFloat(name), 0.0);

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
        assertGetMapEntryEquals(mapMessage, name, Double.valueOf(value), Double.class);
    }

    /**
     * Set a float, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetFloatGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        float value = Float.MAX_VALUE;

        mapMessage.setFloat(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Long.class);
    }

    // ======= double =========

    /**
     * Set a double, then retrieve it as all of the legal type combinations to verify it is
     * parsed correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetDoubleGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        double value = Double.MAX_VALUE;

        mapMessage.setDouble(name, value);
        assertEquals(value, mapMessage.getDouble(name), 0.0);

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
    }

    /**
     * Set a double, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetDoubleGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        double value = Double.MAX_VALUE;

        mapMessage.setDouble(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
    }

    // ======= character =========

    /**
     * Set a char, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetCharGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        char value = 'c';

        mapMessage.setChar(name, value);
        assertEquals(value, mapMessage.getChar(name));

        assertGetMapEntryEquals(mapMessage, name, String.valueOf(value), String.class);
    }

    /**
     * Set a char, then retrieve it as all of the illegal type combinations to verify it fails
     * as expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetCharGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        char value = 'c';

        mapMessage.setChar(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, byte[].class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    // ========= bytes ========

    /**
     * Set bytes, then retrieve it as all of the legal type combinations to verify it is parsed
     * correctly
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBytesGetLegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte[] value = "myBytes".getBytes();

        mapMessage.setBytes(name, value);
        assertTrue(Arrays.equals(value, mapMessage.getBytes(name)));
    }

    /**
     * Set bytes, then retrieve it as all of the illegal type combinations to verify it fails as
     * expected
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBytesGetIllegal() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte[] value = "myBytes".getBytes();

        mapMessage.setBytes(name, value);

        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Character.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, String.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Boolean.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Byte.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Short.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Integer.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Long.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Float.class);
        assertGetMapEntryThrowsMessageFormatException(mapMessage, name, Double.class);
    }

    /**
     * Verify that setting bytes takes a copy of the array. Set bytes, then modify them, then
     * retrieve the map entry and verify the two differ.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBytesTakesSnapshot() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte[] orig = "myBytes".getBytes();
        byte[] copy = Arrays.copyOf(orig, orig.length);

        // set the original bytes
        mapMessage.setBytes(name, orig);

        // corrupt the original bytes
        orig[0] = (byte) 0;

        // verify retrieving the bytes still matches the copy but not the original array
        byte[] retrieved = mapMessage.getBytes(name);
        assertFalse(Arrays.equals(orig, retrieved));
        assertTrue(Arrays.equals(copy, retrieved));
    }

    /**
     * Verify that getting bytes returns a copy of the array. Set bytes, then get them, modify
     * the retrieved value, then get them again and verify the two differ.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetBytesReturnsSnapshot() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte[] orig = "myBytes".getBytes();

        // set the original bytes
        mapMessage.setBytes(name, orig);

        // retrieve them
        byte[] retrieved1 = mapMessage.getBytes(name);
        ;

        // corrupt the retrieved bytes
        retrieved1[0] = (byte) 0;

        // verify retrieving the bytes again still matches the original array, but not the
        // previously retrieved (and now corrupted) bytes.
        byte[] retrieved2 = mapMessage.getBytes(name);
        assertTrue(Arrays.equals(orig, retrieved2));
        assertFalse(Arrays.equals(retrieved1, retrieved2));
    }

    /**
     * Verify that setting bytes takes a copy of the array. Set bytes, then modify them, then
     * retrieve the map entry and verify the two differ.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetBytesWithOffsetAndLength() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        byte[] orig = "myBytesAll".getBytes();

        // extract the segment containing 'Bytes'
        int offset = 2;
        int length = 5;
        byte[] segment = Arrays.copyOfRange(orig, offset, offset + length);

        // set the same section from the original bytes
        mapMessage.setBytes(name, orig, offset, length);

        // verify the retrieved bytes from the map match the segment but not the full original
        // array
        byte[] retrieved = mapMessage.getBytes(name);
        assertFalse(Arrays.equals(orig, retrieved));
        assertTrue(Arrays.equals(segment, retrieved));
    }

    @Test
    public void testSetBytesWithNull() throws Exception {
        JmsMapMessage mapMessage = factory.createMapMessage();

        String name = "myName";
        mapMessage.setBytes(name, null);
        assertNull(mapMessage.getBytes(name));
    }

    // ========= utility methods ========

    private void assertGetMapEntryEquals(JmsMapMessage testMessage, String name, Object expectedValue, Class<?> clazz) throws JMSException {
        Object actualValue = getMapEntryUsingTypeMethod(testMessage, name, clazz);
        assertEquals(expectedValue, actualValue);
    }

    private void assertGetMapEntryThrowsMessageFormatException(JmsMapMessage testMessage, String name, Class<?> clazz) throws JMSException {
        try {
            getMapEntryUsingTypeMethod(testMessage, name, clazz);
            fail("expected exception to be thrown");
        } catch (MessageFormatException jmsMFE) {
            // expected
        }
    }

    private void assertGetMapEntryThrowsNumberFormatException(JmsMapMessage testMessage, String name, Class<?> clazz) throws JMSException {
        try {
            getMapEntryUsingTypeMethod(testMessage, name, clazz);
            fail("expected exception to be thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    private void assertGetMapEntryThrowsNullPointerException(JmsMapMessage testMessage, String name, Class<?> clazz) throws JMSException {
        try {
            getMapEntryUsingTypeMethod(testMessage, name, clazz);
            fail("expected exception to be thrown");
        } catch (NullPointerException npe) {
            // expected
        }
    }

    private Object getMapEntryUsingTypeMethod(JmsMapMessage testMessage, String name, Class<?> clazz) throws JMSException {
        if (clazz == Boolean.class) {
            return testMessage.getBoolean(name);
        } else if (clazz == Byte.class) {
            return testMessage.getByte(name);
        } else if (clazz == Character.class) {
            return testMessage.getChar(name);
        } else if (clazz == Short.class) {
            return testMessage.getShort(name);
        } else if (clazz == Integer.class) {
            return testMessage.getInt(name);
        } else if (clazz == Long.class) {
            return testMessage.getLong(name);
        } else if (clazz == Float.class) {
            return testMessage.getFloat(name);
        } else if (clazz == Double.class) {
            return testMessage.getDouble(name);
        } else if (clazz == String.class) {
            return testMessage.getString(name);
        } else if (clazz == byte[].class) {
            return testMessage.getBytes(name);
        } else {
            throw new RuntimeException("Unexpected entry type class");
        }
    }
}
