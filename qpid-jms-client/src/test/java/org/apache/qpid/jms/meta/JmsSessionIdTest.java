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
package org.apache.qpid.jms.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsSessionIdTest {

    private JmsConnectionId firstId;
    private JmsConnectionId secondId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        String rootId = generator.generateId();

        firstId = new JmsConnectionId(rootId);
        secondId = new JmsConnectionId(rootId);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromConnectionStringThrowsWhenNull() {
        new JmsSessionId((String) null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateConnectionFromStringThrowsWhenEmpty() {
        new JmsSessionId("", 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromConnectionIdThrowsWhenNull() {
        new JmsSessionId((JmsConnectionId) null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromSessionIdThrowsWhenNull() {
        new JmsSessionId((JmsSessionId) null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromConsumerIdThrowsWhenNull() {
        new JmsSessionId((JmsConsumerId) null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromProducerIdThrowsWhenNull() {
        new JmsSessionId((JmsProducerId) null);
    }

    @Test
    public void testStringConstructor() {
        JmsSessionId id = new JmsSessionId(firstId, 1);
        assertNotNull(id.getValue());
        assertNull(id.getProviderHint());
    }

    @Test
    public void testJmsSessionIdFromJmsConnectionId() throws Exception {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsSessionId id2 = new JmsSessionId(secondId, 1);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsSessionId() throws Exception {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsSessionId id2 = new JmsSessionId(id1);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsConsumerId() throws Exception {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsConsumerId consumerId = new JmsConsumerId(id1, 1);
        JmsSessionId id2 = new JmsSessionId(consumerId);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsProducerId() throws Exception {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsProducerId producerId = new JmsProducerId(id1, 1);
        JmsSessionId id2 = new JmsSessionId(producerId);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testGetParentId() {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        assertSame(firstId, id1.getParentId());

        JmsSessionId id2 = new JmsSessionId(firstId.getValue(), 1);
        assertEquals(firstId, id2.getParentId());
    }

    @Test
    public void testCompareTo() {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsSessionId id2 = new JmsSessionId(secondId, 2);

        assertEquals(-1, id1.compareTo(id2));
        assertEquals(0, id1.compareTo(id1));
        assertEquals(1, id2.compareTo(id1));
    }

    @Test
    public void testEquals() {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsSessionId id2 = new JmsSessionId(secondId, 2);

        assertTrue(id1.equals(id1));
        assertTrue(id2.equals(id2));
        assertFalse(id1.equals(id2));
        assertFalse(id2.equals(id1));

        assertFalse(id1.equals(null));
        assertFalse(id1.equals(new String("TEST")));

        JmsSessionId id3 = new JmsSessionId(firstId, 1);
        JmsSessionId id4 = new JmsSessionId(firstId, 2);
        JmsSessionId id5 = new JmsSessionId(firstId, 1);

        // Connection ID
        IdGenerator generator = new IdGenerator();
        String connectionId = generator.generateId();

        JmsSessionId id6 = new JmsSessionId(connectionId, 1);
        JmsSessionId id7 = new JmsSessionId(connectionId, 3);

        assertFalse(id3.equals(id4));
        assertTrue(id3.equals(id5));
        assertFalse(id3.equals(id6));
        assertFalse(id3.equals(id7));
        assertFalse(id4.equals(id6));
        assertFalse(id4.equals(id7));
    }

    @Test
    public void testHashCode() {
        JmsSessionId id1 = new JmsSessionId(firstId, 1);
        JmsSessionId id2 = new JmsSessionId(secondId, 2);

        assertEquals(id1.hashCode(), id1.hashCode());
        assertEquals(id2.hashCode(), id2.hashCode());
        assertFalse(id1.hashCode() == id2.hashCode());
    }
}
