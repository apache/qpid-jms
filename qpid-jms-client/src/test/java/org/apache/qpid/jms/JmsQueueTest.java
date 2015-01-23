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

import static org.apache.qpid.jms.SerializationTestSupport.roundTripSerializeDestination;
import static org.apache.qpid.jms.SerializationTestSupport.serializeDestination;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsQueueTest extends QpidJmsTestCase {

    private static final String NAME_PROP = "name";

    @Test
    public void testIsQueue() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertTrue("should be a queue", queue.isQueue());
    }

    @Test
    public void testIsTopic() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse("should not be a topic", queue.isTopic());
    }

    @Test
    public void testIsTemporary() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse("should not be temporary", queue.isTemporary());
    }

    @Test
    public void testEqualsWithNull() {
        JmsQueue queue = new JmsQueue("myQueue");
        assertFalse("should not be equal", queue.equals(null));
    }

    @Test
    public void testEqualsWithDifferentObjectType() {
        JmsQueue queue = new JmsQueue("name");
        JmsTopic otherObject = new JmsTopic("name");
        assertFalse("should not be equal", queue.equals(otherObject));
    }

    @Test
    public void testEqualsWithSameObject() {
        JmsQueue queue = new JmsQueue("name");
        assertTrue("should be equal to itself", queue.equals(queue));
    }

    @Test
    public void testEqualsWithDifferentObject() {
        JmsQueue queue1 = new JmsQueue("name");
        JmsQueue queue2 = new JmsQueue("name");
        assertTrue("should be equal", queue1.equals(queue2));
        assertTrue("should still be equal", queue2.equals(queue1));
    }

    @Test
    public void testHashcodeWithEqualNamedObjects() {
        JmsQueue queue1 = new JmsQueue("name");
        JmsQueue queue2 = new JmsQueue("name");
        assertEquals("should have same hashcode", queue1.hashCode(), queue2.hashCode());
    }

    @Test
    public void testHashcodeWithDifferentNamedObjects() {
        JmsQueue queue1 = new JmsQueue("name1");
        JmsQueue queue2 = new JmsQueue("name2");

        // Not strictly a requirement, but expected in this case
        assertNotEquals("should not have same hashcode", queue1.hashCode(), queue2.hashCode());
    }

    @Test
    public void testPopulateProperties() throws Exception {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue(name);

        Map<String, String> props = new HashMap<String, String>();
        queue.populateProperties(props);

        assertTrue("Property not found: " + NAME_PROP, props.containsKey(NAME_PROP));
        assertEquals("Unexpected value for property: " + NAME_PROP, name, props.get(NAME_PROP));
        assertEquals("Unexpected number of properties", 1, props.size());
    }

    @Test
    public void testBuildFromProperties() throws Exception {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue();

        Map<String, String> props = new HashMap<String, String>();
        props.put(NAME_PROP, name);
        queue.buildFromProperties(props);

        assertEquals("Unexpected value for name", name, queue.getQueueName());
    }

    @Test
    public void testSerializeThenDeserialize() throws Exception {
        String name = "myQueue";
        JmsQueue queue = new JmsQueue(name);

        Destination roundTripped = roundTripSerializeDestination(queue);

        assertNotNull("Null destination returned", roundTripped);
        assertEquals("Unexpected type", JmsQueue.class, roundTripped.getClass());
        assertEquals("Unexpected name", name, ((JmsQueue)roundTripped).getQueueName());
        assertEquals("Objects were not equal", queue, roundTripped);
    }

    @Test
    public void testSerializeTwoEqualDestinations() throws Exception {
        JmsQueue queue1 = new JmsQueue("myQueue");
        JmsQueue queue2 = new JmsQueue("myQueue");

        assertEquals("Destinations were not equal", queue1, queue2);

        byte[] bytes1 = serializeDestination(queue1);
        byte[] bytes2 = serializeDestination(queue2);

        assertArrayEquals("Serialized bytes were not equal", bytes1, bytes2);
    }

    @Test
    public void testSerializeTwoDifferentDestinations() throws Exception {
        JmsQueue queue1 = new JmsQueue("myQueue1");
        JmsQueue queue2 = new JmsQueue("myQueue2");

        assertNotEquals("Destinations were not expected to be equal", queue1, queue2);

        byte[] bytes1 = serializeDestination(queue1);
        byte[] bytes2 = serializeDestination(queue2);

        try {
            assertArrayEquals(bytes1, bytes2);
            fail("Expected arrays to differ");
        } catch (AssertionError ae) {
            // Expected, pass
        }
    }
}
