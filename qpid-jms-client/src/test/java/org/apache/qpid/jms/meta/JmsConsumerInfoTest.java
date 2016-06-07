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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsConsumerInfoTest {

    private JmsConsumerId firstId;
    private JmsConsumerId secondId;

    private JmsSessionId firstSessionId;
    private JmsSessionId secondSessionId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        String rootId = generator.generateId();

        firstSessionId = new JmsSessionId(rootId, 1);
        secondSessionId = new JmsSessionId(rootId, 2);

        firstId = new JmsConsumerId(firstSessionId, 1);
        secondId = new JmsConsumerId(secondSessionId, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullConnectionId() {
        new JmsConsumerInfo(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullSessionInfo() {
        new JmsConsumerInfo(null, 1);
    }

    @Test
    public void testCreateFromConsumerId() {
        JmsConsumerInfo info = new JmsConsumerInfo(firstId);
        assertSame(firstId, info.getId());
        assertSame(firstId.getParentId(), info.getParentId());
        assertNotNull(info.toString());
    }

    @Test
    public void testCreateFromSessionId() {
        JmsConsumerInfo info = new JmsConsumerInfo(new JmsSessionInfo(firstSessionId), 1);
        assertNotNull(info.toString());
    }

    @Test
    public void testCopy() {
        JmsConsumerInfo info = new JmsConsumerInfo(firstId);

        info.setAcknowledgementMode(1);
        info.setBrowser(true);
        info.setClientId("test");
        info.setDestination(new JmsTopic("Test"));
        info.setLastDeliveredSequenceId(42);
        info.setNoLocal(true);
        info.setPrefetchSize(123456);
        info.setSelector("select");
        info.setSubscriptionName("name");
        info.setRedeliveryPolicy(new JmsDefaultRedeliveryPolicy());

        JmsConsumerInfo copy = info.copy();

        assertEquals(1, copy.getAcknowledgementMode());
        assertEquals(true, copy.isBrowser());
        assertEquals("test", copy.getClientId());
        assertEquals(new JmsTopic("Test"), copy.getDestination());
        assertEquals(42, copy.getLastDeliveredSequenceId());
        assertEquals(true, copy.isNoLocal());
        assertEquals(123456, copy.getPrefetchSize());
        assertEquals("select", copy.getSelector());
        assertEquals("name", copy.getSubscriptionName());

        assertNotSame(info.getRedeliveryPolicy(), copy.getRedeliveryPolicy());

        assertEquals(info, copy);
    }

    @Test
    public void testIsDurable() {
        JmsConsumerInfo info = new JmsConsumerInfo(firstId);
        assertFalse(info.isDurable());
        info.setSubscriptionName("name");
        assertTrue(info.isDurable());
    }

    @Test
    public void testCompareTo() {
        JmsConsumerInfo first = new JmsConsumerInfo(firstId);
        JmsConsumerInfo second = new JmsConsumerInfo(secondId);

        assertEquals(-1, first.compareTo(second));
        assertEquals(0, first.compareTo(first));
        assertEquals(1, second.compareTo(first));
    }

    @Test
    public void testHashCode() {
        JmsConsumerInfo first = new JmsConsumerInfo(firstId);
        JmsConsumerInfo second = new JmsConsumerInfo(secondId);

        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(second.hashCode(), second.hashCode());

        assertFalse(first.hashCode() == second.hashCode());
    }

    @Test
    public void testEqualsCode() {
        JmsConsumerInfo first = new JmsConsumerInfo(firstId);
        JmsConsumerInfo second = new JmsConsumerInfo(secondId);

        assertEquals(first, first);
        assertEquals(second, second);

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));

        assertFalse(first.equals(null));
        assertFalse(second.equals("test"));
    }

    @Test
    public void testVisit() throws Exception {
        final JmsConsumerInfo first = new JmsConsumerInfo(firstId);

        final AtomicBoolean visited = new AtomicBoolean();

        first.visit(new JmsDefaultResourceVisitor() {

            @Override
            public void processConsumerInfo(JmsConsumerInfo info) {
                assertEquals(first, info);
                visited.set(true);
            }
        });

        assertTrue(visited.get());
    }
}
