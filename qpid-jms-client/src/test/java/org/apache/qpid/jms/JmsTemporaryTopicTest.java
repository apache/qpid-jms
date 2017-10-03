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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsTemporaryTopicTest extends QpidJmsTestCase {

    private static final String NAME_PROP = "address";
    private static final String LEGACY_NAME_PROP = "name";

    @Test
    public void testIsQueue() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("myTopic");
        assertFalse("should not be a queue", topic.isQueue());
    }

    @Test
    public void testIsTopic() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("myTopic");
        assertTrue("should be a topic", topic.isTopic());
    }

    @Test
    public void testIsTemporary() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("myTopic");
        assertTrue("should be temporary", topic.isTemporary());
    }

    @Test
    public void testIsDeleted() throws Exception {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("myTopic");
        assertFalse("should not be deleted", topic.isDeleted());
        topic.delete();
        assertTrue("should be deleted", topic.isDeleted());
    }

    @Test
    public void testEqualsWithNull() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("myTopic");
        assertFalse("should not be equal", topic.equals(null));
    }

    @Test
    public void testEqualsWithDifferentObjectType() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("name");
        JmsQueue otherObject = new JmsQueue("name");
        assertFalse("should not be equal", topic.equals(otherObject));
    }

    @Test
    public void testEqualsWithSameObject() {
        JmsTemporaryTopic topic = new JmsTemporaryTopic("name");
        assertTrue("should be equal to itself", topic.equals(topic));
    }

    @Test
    public void testEqualsWithDifferentObject() {
        JmsTemporaryTopic topic1 = new JmsTemporaryTopic("name");
        JmsTemporaryTopic topic2 = new JmsTemporaryTopic("name");
        assertTrue("should be equal", topic1.equals(topic2));
        assertTrue("should still be equal", topic2.equals(topic1));
    }

    @Test
    public void testHashcodeWithEqualNamedObjects() {
        JmsTemporaryTopic topic1 = new JmsTemporaryTopic("name");
        JmsTemporaryTopic topic2 = new JmsTemporaryTopic("name");
        assertEquals("should have same hashcode", topic1.hashCode(), topic2.hashCode());
    }

    @Test
    public void testHashcodeWithDifferentNamedObjects() {
        JmsTemporaryTopic topic1 = new JmsTemporaryTopic("name1");
        JmsTemporaryTopic topic2 = new JmsTemporaryTopic("name2");

        // Not strictly a requirement, but expected in this case
        assertNotEquals("should not have same hashcode", topic1.hashCode(), topic2.hashCode());
    }

    @Test
    public void testGetProperties() throws Exception {
        String name = "myTopic";
        JmsTemporaryTopic topic = new JmsTemporaryTopic(name);

        Map<String, String> props = topic.getProperties();

        assertTrue("Property not found: " + NAME_PROP, props.containsKey(NAME_PROP));
        assertEquals("Unexpected value for property: " + NAME_PROP, name, props.get(NAME_PROP));
        assertEquals("Unexpected number of properties", 1, props.size());
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
        String name = "myTopic";
        JmsTemporaryTopic topic = new JmsTemporaryTopic();

        assertNull("Shouldnt have name yet", topic.getTopicName());

        Map<String, String> props = new HashMap<String, String>();
        if(addNameProp) {
            props.put(NAME_PROP, name);
        }

        if(addLegacyNameProp) {
            props.put(LEGACY_NAME_PROP, name);
        }

        Map<String, String> unusedProps = topic.setProperties(props);

        assertEquals("Unexpected value for name", name, topic.getTopicName());

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
        String name = "myTopic";
        String unusedKey = "unusedKey";
        String unusedValue = "unusedValue";
        JmsTemporaryTopic topic = new JmsTemporaryTopic();

        Map<String, String> props = new HashMap<String, String>();
        props.put(NAME_PROP, name);
        props.put(unusedKey, unusedValue);
        Map<String, String> unusedProps = topic.setProperties(props);

        // Verify the name property was applied.
        assertEquals("Unexpected value for name", name, topic.getTopicName());

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
        String name = "myTopic";
        JmsTemporaryTopic topic = new JmsTemporaryTopic(name);

        Object roundTripped = roundTripSerialize(topic);

        assertNotNull("Null destination returned", roundTripped);
        assertEquals("Unexpected type", JmsTemporaryTopic.class, roundTripped.getClass());
        assertEquals("Unexpected name", name, ((JmsTemporaryTopic)roundTripped).getTopicName());

        assertEquals("Objects were not equal", topic, roundTripped);
        assertEquals("Object hashCodes were not equal", topic.hashCode(), roundTripped.hashCode());
    }

    @Test
    public void testSerializeTwoEqualDestinations() throws Exception {
        JmsTemporaryTopic topic1 = new JmsTemporaryTopic("myTopic");
        JmsTemporaryTopic topic2 = new JmsTemporaryTopic("myTopic");

        assertEquals("Destinations were not equal", topic1, topic2);

        byte[] bytes1 = serialize(topic1);
        byte[] bytes2 = serialize(topic2);

        assertArrayEquals("Serialized bytes were not equal", bytes1, bytes2);
    }

    @Test
    public void testSerializeTwoDifferentDestinations() throws Exception {
        JmsTemporaryTopic topic1 = new JmsTemporaryTopic("myTopic1");
        JmsTemporaryTopic topic2 = new JmsTemporaryTopic("myTopic2");

        assertNotEquals("Destinations were not expected to be equal", topic1, topic2);

        byte[] bytes1 = serialize(topic1);
        byte[] bytes2 = serialize(topic2);

        try {
            assertArrayEquals(bytes1, bytes2);
            fail("Expected arrays to differ");
        } catch (AssertionError ae) {
            // Expected, pass
        }
    }
}
