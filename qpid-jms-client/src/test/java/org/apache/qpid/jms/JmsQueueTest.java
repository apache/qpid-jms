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
package org.apache.qpid.jms;

import static org.apache.qpid.jms.SerializationTestSupport.roundTripSerialize;
import static org.apache.qpid.jms.SerializationTestSupport.serialize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.jupiter.api.Test;

public class JmsQueueTest extends QpidJmsTestCase {

    private static final String NAME_PROP = "address";
    private static final String LEGACY_NAME_PROP = "name";

    @Test
    public void testIsQueue() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertTrue(queue.isQueue(), "should be a queue");
    }

    @Test
    public void testIsTopic() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse(queue.isTopic(), "should not be a topic");
    }

    @Test
    public void testIsTemporary() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse(queue.isTemporary(), "should not be temporary");
    }

    @Test
    public void testEqualsWithNull() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse(queue.equals(null), "should not be equal");
    }

    @Test
    public void testEqualsWithDifferentObjectType() {
        JmsQueue queue = new JmsQueue("name");
        JmsTopic otherObject = new JmsTopic("name");
        assertFalse(queue.equals(otherObject), "should not be equal");
    }

    @Test
    public void testEqualsWithSameObject() {
        JmsQueue queue = new JmsQueue("name");
        assertTrue(queue.equals(queue), "should be equal to itself");
    }

    @Test
    public void testEqualsWithDifferentObject() {
        JmsQueue queue1 = new JmsQueue("name");
        JmsQueue queue2 = new JmsQueue("name");
        assertTrue(queue1.equals(queue2), "should be equal");
        assertTrue(queue2.equals(queue1), "should still be equal");
    }

    @Test
    public void testEqualsWithTemporaryQueue() {
        JmsQueue queue1 = new JmsQueue("name");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("name");
        assertFalse(queue1.equals(queue2), "should be unequal");
        assertFalse(queue2.equals(queue1), "should still be unequal");
    }

    @Test
    public void testHashcodeWithEqualNamedObjects() {
        JmsQueue queue1 = new JmsQueue("name");
        JmsQueue queue2 = new JmsQueue("name");
        assertEquals(queue1.hashCode(), queue2.hashCode(), "should have same hashcode");
    }

    @Test
    public void testHashcodeWithDifferentNamedObjects() {
        JmsQueue queue1 = new JmsQueue("name1");
        JmsQueue queue2 = new JmsQueue("name2");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(queue1.hashCode(), queue2.hashCode(), "should not have same hashcode");
    }

    @Test
    public void testGetProperties() throws Exception {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue(name);

        Map<String, String> props = queue.getProperties();

        assertTrue(props.containsKey(NAME_PROP), "Property not found: " + NAME_PROP);
        assertEquals(name, props.get(NAME_PROP), "Unexpected value for property: " + NAME_PROP);
        assertEquals(1, props.size(), "Unexpected number of properties");
    }

    @Test
    public void testSetProperties() throws Exception {
        setPropertiesTestImpl(true, false);
    }

    @Test
    public void testSetPropertiesWithLegacyNameProp() throws Exception {
        setPropertiesTestImpl(false, true);
    }

    @Test
    public void testSetPropertiesWithBothNameProps() throws Exception {
        setPropertiesTestImpl(true, true);
    }

    private void setPropertiesTestImpl(boolean addNameProp, boolean addLegacyNameProp) {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue();

        assertNull(queue.getQueueName(), "Shouldnt have name yet");

        Map<String, String> props = new HashMap<String, String>();
        if(addNameProp) {
            props.put(NAME_PROP, name);
        }

        if(addLegacyNameProp) {
            props.put(LEGACY_NAME_PROP, name);
        }

        Map<String, String> unusedProps = queue.setProperties(props);

        assertEquals(name, queue.getQueueName(), "Unexpected value for name");

        // Verify the returned map was empty and unmodifiable
        assertTrue(unusedProps.isEmpty(), "Map should be empty: " + unusedProps);
        try {
            unusedProps.put("a", "b");
            fail("Map should be unmodifiable");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testSetPropertiesWithUnusedOptions() throws Exception {
        String name = "myQueue";
        String unusedKey = "unusedKey";
        String unusedValue = "unusedValue";
        JmsQueue queue = new JmsQueue();


        Map<String, String> props = new HashMap<String, String>();
        props.put(NAME_PROP, name);
        props.put(unusedKey, unusedValue);
        Map<String, String> unusedProps = queue.setProperties(props);

        // Verify the name property was applied.
        assertEquals(name, queue.getQueueName(), "Unexpected value for name");

        //Verify that the unused property was returned
        assertEquals(1, unusedProps.size(), "Unexpected size of return map");
        assertTrue(unusedProps.containsKey(unusedKey), "Expected property not found in map: " + unusedProps);
        assertEquals(unusedValue, unusedProps.get(unusedKey), "Unexpected property value");

        // Verify the returned map was unmodifiable
        try {
            unusedProps.put("a", "b");
            fail("Map should be unmodifiable");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testSerializeThenDeserialize() throws Exception {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue(name);

        Object roundTripped = roundTripSerialize(queue);

        assertNotNull(roundTripped, "Null destination returned");
        assertEquals(JmsQueue.class, roundTripped.getClass(), "Unexpected type");
        assertEquals(name, ((JmsQueue)roundTripped).getQueueName(), "Unexpected name");

        assertEquals(queue, roundTripped, "Objects were not equal");
        assertEquals(queue.hashCode(), roundTripped.hashCode(), "Object hashCodes were not equal");
    }

    @Test
    public void testSerializeTwoEqualDestinations() throws Exception {
        JmsQueue queue1 = new JmsQueue("myQueue");
        JmsQueue queue2 = new JmsQueue("myQueue");

        assertEquals(queue1, queue2, "Destinations were not equal");

        byte[] bytes1 = serialize(queue1);
        byte[] bytes2 = serialize(queue2);

        assertArrayEquals(bytes1, bytes2, "Serialized bytes were not equal");
    }

    @Test
    public void testSerializeTwoDifferentDestinations() throws Exception {
        JmsQueue queue1 = new JmsQueue("myQueue1");
        JmsQueue queue2 = new JmsQueue("myQueue2");

        assertNotEquals(queue1, queue2, "Destinations were not expected to be equal");

        byte[] bytes1 = serialize(queue1);
        byte[] bytes2 = serialize(queue2);

        try {
            assertArrayEquals(bytes1, bytes2);
            fail("Expected arrays to differ");
        } catch (AssertionError ae) {
            // Expected, pass
        }
    }
}
