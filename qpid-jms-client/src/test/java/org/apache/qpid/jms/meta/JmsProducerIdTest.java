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
public class JmsProducerIdTest {

    private JmsSessionId firstId;
    private JmsSessionId secondId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        String rootId = generator.generateId();

        firstId = new JmsSessionId(rootId, 1);
        secondId = new JmsSessionId(rootId, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromStringThrowsWhenNull() {
        new JmsProducerId((String) null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromStringThrowsWhenEmpty() {
        new JmsProducerId("");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromConnectionStringThrowsWhenNull() {
        new JmsProducerId((String) null, 1, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateConnectionFromStringThrowsWhenEmpty() {
        new JmsProducerId("", 1, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromSessionIdThrowsWhenNull() {
        new JmsProducerId((JmsSessionId) null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateFromConsumerIdThrowsWhenNull() {
        new JmsProducerId((JmsProducerId) null);
    }

    @Test
    public void testStringConstructor() {
        JmsProducerId id = new JmsProducerId(firstId, 1);
        assertNotNull(id.getValue());
        assertNull(id.getProviderHint());
    }

    @Test
    public void testJmsProducerIdFromJmsConnectionId() throws Exception {
        JmsProducerId id1 = new JmsProducerId(firstId.getConnectionId(), 1, 1);
        JmsProducerId id2 = new JmsProducerId(secondId.getConnectionId(), 1, 1);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsProducerIdFromJmsSessionId() throws Exception {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        JmsProducerId id2 = new JmsProducerId(id1);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsProducerIdFromJmsConsumerIdString() throws Exception {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        JmsProducerId id2 = new JmsProducerId(id1.toString());
        assertSame(id1.getValue(), id2.getValue());

        JmsProducerId id3 = new JmsProducerId("SOMEIDVALUE");
        assertEquals("SOMEIDVALUE", id3.getConnectionId());
        assertEquals(0, id3.getSessionId());
        assertEquals(0, id3.getValue());
    }

    @Test
    public void testGetParentId() {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        assertSame(firstId, id1.getParentId());

        JmsProducerId id2 = new JmsProducerId(firstId.getConnectionId(), 1, 1);
        assertEquals(firstId, id2.getParentId());
    }

    @Test
    public void testCompareTo() {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        JmsProducerId id2 = new JmsProducerId(secondId, 1);

        assertEquals(-1, id1.compareTo(id2));
        assertEquals(0, id1.compareTo(id1));
        assertEquals(1, id2.compareTo(id1));
    }

    @Test
    public void testEquals() {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        JmsProducerId id2 = new JmsProducerId(secondId, 1);

        assertTrue(id1.equals(id1));
        assertTrue(id2.equals(id2));
        assertFalse(id1.equals(id2));
        assertFalse(id2.equals(id1));

        assertFalse(id1.equals(null));
        assertFalse(id1.equals(new String("TEST")));

        JmsProducerId id3 = new JmsProducerId(firstId, 1);
        JmsProducerId id4 = new JmsProducerId(firstId, 2);
        JmsProducerId id5 = new JmsProducerId(firstId, 1);
        JmsProducerId id6 = new JmsProducerId(secondId.getConnectionId(), 1, 3);

        // Connection ID
        IdGenerator generator = new IdGenerator();
        String connectionId = generator.generateId();

        JmsProducerId id7 = new JmsProducerId(connectionId, 1, 2);
        JmsProducerId id8 = new JmsProducerId(connectionId, 1, 3);

        assertFalse(id3.equals(id4));
        assertTrue(id3.equals(id5));
        assertFalse(id3.equals(id6));
        assertFalse(id3.equals(id7));
        assertFalse(id3.equals(id8));
        assertFalse(id4.equals(id7));
        assertFalse(id4.equals(id8));
    }

    @Test
    public void testHashCode() {
        JmsProducerId id1 = new JmsProducerId(firstId, 1);
        JmsProducerId id2 = new JmsProducerId(secondId, 1);

        assertEquals(id1.hashCode(), id1.hashCode());
        assertEquals(id2.hashCode(), id2.hashCode());
        assertFalse(id1.hashCode() == id2.hashCode());
    }
}
