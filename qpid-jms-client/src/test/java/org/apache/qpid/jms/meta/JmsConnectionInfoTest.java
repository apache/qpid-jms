/**
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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for class JmsConnectionInfo
 */
public class JmsConnectionInfoTest {

    private JmsConnectionId firstId;
    private JmsConnectionId secondId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        firstId = new JmsConnectionId(generator.generateId());
        secondId = new JmsConnectionId(generator.generateId());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullConnectionId() {
        new JmsConnectionInfo(null);
    }

    @Test
    public void testCreate() {
        JmsConnectionInfo info = new JmsConnectionInfo(firstId);
        assertSame(firstId, info.getId());
        assertNotNull(info.toString());
    }

    @Test
    public void testCopy() {
        JmsConnectionInfo info = new JmsConnectionInfo(firstId);

        info.setAlwaysSyncSend(true);
        info.setClientId("test");
        info.setCloseTimeout(100);
        info.setConnectTimeout(200);
        info.setForceAsyncSends(true);
        info.setPassword("pass");
        info.setQueuePrefix("queue");
        info.setRequestTimeout(50);
        info.setSendTimeout(150);
        info.setTopicPrefix("topic");
        info.setUsername("user");
        boolean validatePropertyNames = ! info.isValidatePropertyNames();
        info.setValidatePropertyNames(validatePropertyNames);

        JmsConnectionInfo copy = info.copy();

        assertEquals(true, copy.isAlwaysSyncSend());
        assertEquals("test", copy.getClientId());
        assertEquals(100, copy.getCloseTimeout());
        assertEquals(200, copy.getConnectTimeout());
        assertEquals(true, copy.isForceAsyncSend());
        assertEquals("pass", copy.getPassword());
        assertEquals("queue", copy.getQueuePrefix());
        assertEquals(50, copy.getRequestTimeout());
        assertEquals(150, copy.getSendTimeout());
        assertEquals("topic", copy.getTopicPrefix());
        assertEquals("user", copy.getUsername());
        assertEquals(validatePropertyNames, copy.isValidatePropertyNames());

        assertEquals(info, copy);
}

    @Test
    public void testCompareTo() {
        JmsConnectionInfo first = new JmsConnectionInfo(firstId);
        JmsConnectionInfo second = new JmsConnectionInfo(secondId);

        assertEquals(-1, first.compareTo(second));
        assertEquals(0, first.compareTo(first));
        assertEquals(1, second.compareTo(first));
    }

    @Test
    public void testHashCode() {
        JmsConnectionInfo first = new JmsConnectionInfo(firstId);
        JmsConnectionInfo second = new JmsConnectionInfo(secondId);

        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(second.hashCode(), second.hashCode());

        assertFalse(first.hashCode() == second.hashCode());
    }

    @Test
    public void testEqualsCode() {
        JmsConnectionInfo first = new JmsConnectionInfo(firstId);
        JmsConnectionInfo second = new JmsConnectionInfo(secondId);

        assertEquals(first, first);
        assertEquals(second, second);

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));

        assertFalse(first.equals(null));
        assertFalse(second.equals("test"));
    }

    @Test
    public void testVisit() throws Exception {
        final JmsConnectionInfo first = new JmsConnectionInfo(firstId);

        final AtomicBoolean visited = new AtomicBoolean();

        first.visit(new JmsDefaultResourceVisitor() {

            @Override
            public void processConnectionInfo(JmsConnectionInfo info) {
                assertEquals(first, info);
                visited.set(true);
            }
        });

        assertTrue(visited.get());
    }
}
