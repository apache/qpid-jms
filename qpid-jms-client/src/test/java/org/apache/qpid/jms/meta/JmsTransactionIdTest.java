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
import static org.junit.Assert.assertTrue;

import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsTransactionIdTest {

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
    public void testNullIdThrowsException() {
        new JmsTransactionId(null, 0);
    }

    @Test
    public void testConstructor() {
        JmsTransactionId id = new JmsTransactionId(firstId, 1);
        assertEquals(firstId, id.getConnectionId());
        assertNotNull(id.getValue());
        assertNull(id.getProviderHint());
    }

    @Test
    public void testToString() {
        JmsTransactionId id = new JmsTransactionId(firstId, 1);
        String txKey = id.toString();
        assertNotNull(txKey);
        assertTrue(txKey.startsWith("TX:"));
    }

    @Test
    public void testCompareTo() {
        JmsTransactionId id1 = new JmsTransactionId(firstId, 1);
        JmsTransactionId id2 = new JmsTransactionId(secondId, 2);

        assertEquals(-1, id1.compareTo(id2));
        assertEquals(0, id1.compareTo(id1));
        assertEquals(1, id2.compareTo(id1));
    }

    @Test
    public void testEquals() {
        JmsTransactionId id1 = new JmsTransactionId(firstId, 1);
        JmsTransactionId id2 = new JmsTransactionId(secondId, 2);

        assertTrue(id1.equals(id1));
        assertTrue(id2.equals(id2));
        assertFalse(id1.equals(id2));
        assertFalse(id2.equals(id1));

        assertFalse(id1.equals(null));
        assertFalse(id1.equals(new String("TEST")));

        JmsTransactionId id3 = new JmsTransactionId(firstId, 1);
        JmsTransactionId id4 = new JmsTransactionId(firstId, 2);
        JmsTransactionId id5 = new JmsTransactionId(firstId, 1);

        assertFalse(id3.equals(id4));
        assertTrue(id3.equals(id5));
    }

    @Test
    public void testHashCode() {
        JmsTransactionId id1 = new JmsTransactionId(firstId, 1);
        JmsTransactionId id2 = new JmsTransactionId(secondId, 2);

        assertEquals(id1.hashCode(), id1.hashCode());
        assertEquals(id2.hashCode(), id2.hashCode());
        assertFalse(id1.hashCode() == id2.hashCode());
    }
}
