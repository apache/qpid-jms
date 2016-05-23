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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsTemporaryQueueTest extends QpidJmsTestCase {

    private static final String NAME_PROP = "name";

    @Test
    public void testIsQueue() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("myQueue");
        assertTrue("should be a queue", queue.isQueue());
    }

    @Test
    public void testIsTopic() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("myQueue");
        assertFalse("should not be a topic", queue.isTopic());
    }

    @Test
    public void testIsTemporary() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("myQueue");
        assertTrue("should be temporary", queue.isTemporary());
    }

    @Test
    public void testIsDeleted() throws Exception {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("myQueue");
        assertFalse("should not be deleted", queue.isDeleted());
        queue.delete();
        assertTrue("should be deleted", queue.isDeleted());
    }

    @Test
    public void testEqualsWithNull() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("myQueue");
        assertFalse("should not be equal", queue.equals(null));
    }

    @Test
    public void testEqualsWithDifferentObjectType() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("name");
        JmsQueue otherObject = new JmsQueue("name");
        assertFalse("should not be equal", queue.equals(otherObject));
    }

    @Test
    public void testEqualsWithSameObject() {
        JmsTemporaryQueue queue = new JmsTemporaryQueue("name");
        assertTrue("should be equal to itself", queue.equals(queue));
    }

    @Test
    public void testEqualsWithDifferentObject() {
        JmsTemporaryQueue queue1 = new JmsTemporaryQueue("name");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("name");
        assertTrue("should be equal", queue1.equals(queue2));
        assertTrue("should still be equal", queue2.equals(queue1));
    }

    @Test
    public void testHashcodeWithEqualNamedObjects() {
        JmsTemporaryQueue queue1 = new JmsTemporaryQueue("name");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("name");
        assertEquals("should have same hashcode", queue1.hashCode(), queue2.hashCode());
    }

    @Test
    public void testHashcodeWithDifferentNamedObjects() {
        JmsTemporaryQueue queue1 = new JmsTemporaryQueue("name1");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("name2");

        // Not strictly a requirement, but expected in this case
        assertNotEquals("should not have same hashcode", queue1.hashCode(), queue2.hashCode());
    }

    @Test
    public void testGetProperties() throws Exception {
        String name = "myQueue";
        JmsTemporaryQueue queue = new JmsTemporaryQueue(name);

        Map<String, String> props = queue.getProperties();

        assertTrue("Property not found: " + NAME_PROP, props.containsKey(NAME_PROP));
        assertEquals("Unexpected value for property: " + NAME_PROP, name, props.get(NAME_PROP));
        assertEquals("Unexpected number of properties", 1, props.size());
    }

    @Test
    public void testSetProperties() throws Exception {
        String name = "myQueue";
        JmsTemporaryQueue queue = new JmsTemporaryQueue();

        Map<String, String> props = new HashMap<String, String>();
        props.put(NAME_PROP, name);
        Map<String, String> unusedProps = queue.setProperties(props);

        assertEquals("Unexpected value for name", name, queue.getQueueName());

        // Verify the returned map was empty and unmodifiable
        assertTrue("Map should be empty: " + unusedProps, unusedProps.isEmpty());
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
        assertEquals("Unexpected value for name", name, queue.getQueueName());

        //Verify that the unused property was returned
        assertEquals("Unexpected size of return map", 1, unusedProps.size());
        assertTrue("Expected property not found in map: " + unusedProps, unusedProps.containsKey(unusedKey));
        assertEquals("Unexpected property value", unusedValue, unusedProps.get(unusedKey));

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
        JmsTemporaryQueue queue = new JmsTemporaryQueue(name);

        Object roundTripped = roundTripSerialize(queue);

        assertNotNull("Null destination returned", roundTripped);
        assertEquals("Unexpected type", JmsTemporaryQueue.class, roundTripped.getClass());
        assertEquals("Unexpected name", name, ((JmsTemporaryQueue)roundTripped).getQueueName());

        assertEquals("Objects were not equal", queue, roundTripped);
        assertEquals("Object hashCodes were not equal", queue.hashCode(), roundTripped.hashCode());
    }

    @Test
    public void testSerializeTwoEqualDestinations() throws Exception {
        JmsTemporaryQueue queue1 = new JmsTemporaryQueue("myQueue");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("myQueue");

        assertEquals("Destinations were not equal", queue1, queue2);

        byte[] bytes1 = serialize(queue1);
        byte[] bytes2 = serialize(queue2);

        assertArrayEquals("Serialized bytes were not equal", bytes1, bytes2);
    }

    @Test
    public void testSerializeTwoDifferentDestinations() throws Exception {
        JmsTemporaryQueue queue1 = new JmsTemporaryQueue("myQueue1");
        JmsTemporaryQueue queue2 = new JmsTemporaryQueue("myQueue2");

        assertNotEquals("Destinations were not expected to be equal", queue1, queue2);

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
