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

import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsProducerInfoTest {

    private JmsProducerId firstId;
    private JmsProducerId secondId;

    private JmsSessionId firstSessionId;
    private JmsSessionId secondSessionId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        firstSessionId = new JmsSessionId(generator.generateId(), 1);
        secondSessionId = new JmsSessionId(generator.generateId(), 2);

        firstId = new JmsProducerId(firstSessionId, 1);
        secondId = new JmsProducerId(secondSessionId, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullConnectionId() {
        new JmsProducerInfo(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullSessionInfo() {
        new JmsProducerInfo(null, 1);
    }

    @Test
    public void testCreateFromProducerId() {
        JmsProducerInfo info = new JmsProducerInfo(firstId);
        assertSame(firstId, info.getId());
        assertSame(firstId.getParentId(), info.getParentId());
        assertNotNull(info.toString());
    }

    @Test
    public void testCreateFromSessionId() {
        JmsProducerInfo info = new JmsProducerInfo(new JmsSessionInfo(firstSessionId), 1);
        assertNotNull(info.toString());
    }

    @Test
    public void testCopy() {
        JmsProducerInfo info = new JmsProducerInfo(firstId);
        info.setDestination(new JmsTopic("Test"));

        JmsProducerInfo copy = info.copy();
        assertEquals(new JmsTopic("Test"), copy.getDestination());

        assertEquals(info, copy);
    }

    @Test
    public void testCompareTo() {
        JmsProducerInfo first = new JmsProducerInfo(firstId);
        JmsProducerInfo second = new JmsProducerInfo(secondId);

        assertEquals(-1, first.compareTo(second));
        assertEquals(0, first.compareTo(first));
        assertEquals(1, second.compareTo(first));
    }

    @Test
    public void testHashCode() {
        JmsProducerInfo first = new JmsProducerInfo(firstId);
        JmsProducerInfo second = new JmsProducerInfo(secondId);

        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(second.hashCode(), second.hashCode());

        assertFalse(first.hashCode() == second.hashCode());
    }

    @Test
    public void testEqualsCode() {
        JmsProducerInfo first = new JmsProducerInfo(firstId);
        JmsProducerInfo second = new JmsProducerInfo(secondId);

        assertEquals(first, first);
        assertEquals(second, second);

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));

        assertFalse(first.equals(null));
        assertFalse(second.equals("test"));
    }

    @Test
    public void testVisit() throws Exception {
        final JmsProducerInfo first = new JmsProducerInfo(firstId);

        final AtomicBoolean visited = new AtomicBoolean();

        first.visit(new JmsDefaultResourceVisitor() {

            @Override
            public void processProducerInfo(JmsProducerInfo info) {
                assertEquals(first, info);
                visited.set(true);
            }
        });

        assertTrue(visited.get());
    }
}
