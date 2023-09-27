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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.jms.util.IdGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for JmsConnectionId
 */
public class JmsConnectionIdTest {

    private String firstId;
    private String secondId;

    @BeforeEach
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        firstId = generator.generateId();
        secondId = generator.generateId();
    }

    @Test
    public void testCreateFromStringThrowsWhenNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId((String) null);
        });
    }

    @Test
    public void testCreateFromStringThrowsWhenEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId("");
        });
    }

    @Test
    public void testCreateFromConnectionIdThrowsWhenNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId((JmsConnectionId) null);
        });
    }

    @Test
    public void testCreateFromSessionIdThrowsWhenNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId((JmsSessionId) null);
        });
    }

    @Test
    public void testCreateFromProducerIdThrowsWhenNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId((JmsProducerId) null);
        });
    }

    @Test
    public void testCreateFromConsumerIdThrowsWhenNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionId((JmsConsumerId) null);
        });
    }

    @Test
    public void testStringConstructor() {
        JmsConnectionId id = new JmsConnectionId(firstId);
        assertNotNull(id.getValue());
        assertNull(id.getProviderHint());
    }

    @Test
    public void testProviderHints() {
        JmsConnectionId id = new JmsConnectionId(firstId);
        assertNotNull(id.getValue());
        assertNull(id.getProviderHint());

        String hint = new String("hint");

        id.setProviderHint(hint);

        assertEquals(hint, id.getProviderHint());
    }

    @Test
    public void testJmsConnectionIdFromJmsConnectionId() throws Exception {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsConnectionId id2 = new JmsConnectionId(id1);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsSessionId() throws Exception {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsSessionId sessionId = new JmsSessionId(id1, 1);
        JmsConnectionId id2 = new JmsConnectionId(sessionId);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsConsumerId() throws Exception {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsSessionId sessionId = new JmsSessionId(id1, 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);
        JmsConnectionId id2 = new JmsConnectionId(consumerId);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testJmsConnectionIdFromJmsProducerId() throws Exception {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsSessionId sessionId = new JmsSessionId(id1, 1);
        JmsProducerId consumerId = new JmsProducerId(sessionId, 1);
        JmsConnectionId id2 = new JmsConnectionId(consumerId);
        assertSame(id1.getValue(), id2.getValue());
    }

    @Test
    public void testCompareTo() {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsConnectionId id2 = new JmsConnectionId(secondId);

        assertFalse(id1.compareTo(id2) == 0);
    }

    @Test
    public void testEquals() {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsConnectionId id2 = new JmsConnectionId(secondId);

        assertTrue(id1.equals(id1));
        assertTrue(id2.equals(id2));
        assertFalse(id1.equals(id2));
        assertFalse(id2.equals(id1));

        assertFalse(id1.equals(null));
        assertFalse(id1.equals(new String("TEST")));
    }

    @Test
    public void testHashCode() {
        JmsConnectionId id1 = new JmsConnectionId(firstId);
        JmsConnectionId id2 = new JmsConnectionId(secondId);

        assertEquals(id1.hashCode(), id1.hashCode());
        assertEquals(id2.hashCode(), id2.hashCode());
        assertFalse(id1.hashCode() == id2.hashCode());
    }
}
