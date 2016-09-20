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

import javax.jms.Session;

import org.apache.qpid.jms.policy.JmsDefaultMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the JmsSessionInfo object
 */
public class JmsSessionInfoTest {

    private JmsSessionId firstId;
    private JmsSessionId secondId;

    private JmsConnectionInfo connectionInfo;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        String rootId = generator.generateId();

        firstId = new JmsSessionId(rootId, 1);
        secondId = new JmsSessionId(rootId, 2);

        JmsConnectionId connectionId = new JmsConnectionId(generator.generateId());
        connectionInfo = new JmsConnectionInfo(connectionId);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullConnectionId() {
        new JmsSessionInfo(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testExceptionWhenCreatedWithNullSessionId() {
        new JmsSessionInfo(null);
    }

    @Test
    public void testCreateFromSessionId() {
        JmsSessionInfo info = new JmsSessionInfo(firstId);
        assertSame(firstId, info.getId());
        assertNotNull(info.toString());
    }

    @Test
    public void testCreateFromConnectionInfo() {
        JmsSessionInfo info = new JmsSessionInfo(connectionInfo, 1);
        assertEquals(connectionInfo.getId(), info.getId().getParentId());
    }

    @Test
    public void testIsTransacted() {
        JmsSessionInfo info = new JmsSessionInfo(firstId);
        info.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);
        assertFalse(info.isTransacted());
        info.setAcknowledgementMode(Session.SESSION_TRANSACTED);
        assertTrue(info.isTransacted());
    }

    @Test
    public void testCopy() {
        JmsSessionInfo info = new JmsSessionInfo(firstId);

        info.setAcknowledgementMode(2);
        info.setSendAcksAsync(true);
        info.setMessageIDPolicy(new JmsDefaultMessageIDPolicy());
        info.setPrefetchPolicy(new JmsDefaultPrefetchPolicy());
        info.setPresettlePolicy(new JmsDefaultPresettlePolicy());
        info.setRedeliveryPolicy(new JmsDefaultRedeliveryPolicy());

        JmsSessionInfo copy = info.copy();

        assertEquals(2, copy.getAcknowledgementMode());
        assertEquals(true, copy.isSendAcksAsync());

        assertNotSame(info.getPrefetchPolicy(), copy.getPrefetchPolicy());
        assertNotSame(info.getPresettlePolicy(), copy.getPresettlePolicy());
        assertNotSame(info.getRedeliveryPolicy(), copy.getRedeliveryPolicy());
        assertNotSame(info.getMessageIDPolicy(), copy.getMessageIDPolicy());

        assertEquals(info, copy);
    }

    @Test
    public void testCompareTo() {
        JmsSessionInfo first = new JmsSessionInfo(firstId);
        JmsSessionInfo second = new JmsSessionInfo(secondId);

        assertEquals(-1, first.compareTo(second));
        assertEquals(0, first.compareTo(first));
        assertEquals(1, second.compareTo(first));
    }

    @Test
    public void testHashCode() {
        JmsSessionInfo first = new JmsSessionInfo(firstId);
        JmsSessionInfo second = new JmsSessionInfo(secondId);

        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(second.hashCode(), second.hashCode());

        assertFalse(first.hashCode() == second.hashCode());
    }

    @Test
    public void testEqualsCode() {
        JmsSessionInfo first = new JmsSessionInfo(firstId);
        JmsSessionInfo second = new JmsSessionInfo(secondId);

        assertEquals(first, first);
        assertEquals(second, second);

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));

        assertFalse(first.equals(null));
        assertFalse(second.equals("test"));
    }

    @Test
    public void testVisit() throws Exception {
        final JmsSessionInfo first = new JmsSessionInfo(firstId);

        final AtomicBoolean visited = new AtomicBoolean();

        first.visit(new JmsDefaultResourceVisitor() {

            @Override
            public void processSessionInfo(JmsSessionInfo info) {
                assertEquals(first, info);
                visited.set(true);
            }
        });

        assertTrue(visited.get());
    }

    @Test
    public void testGetMessageIDPolicyDefaults() {
        final JmsSessionInfo info = new JmsSessionInfo(firstId);

        assertNotNull(info.getMessageIDPolicy());
        info.setMessageIDPolicy(null);
        assertNotNull(info.getMessageIDPolicy());
        assertTrue(info.getMessageIDPolicy() instanceof JmsDefaultMessageIDPolicy);
    }

    @Test
    public void testGetPrefetchPolicyDefaults() {
        final JmsSessionInfo info = new JmsSessionInfo(firstId);

        assertNotNull(info.getPrefetchPolicy());
        info.setPrefetchPolicy(null);
        assertNotNull(info.getPrefetchPolicy());
        assertTrue(info.getPrefetchPolicy() instanceof JmsDefaultPrefetchPolicy);
    }

    @Test
    public void testGetPresettlePolicyDefaults() {
        final JmsSessionInfo info = new JmsSessionInfo(firstId);

        assertNotNull(info.getPresettlePolicy());
        info.setPresettlePolicy(null);
        assertNotNull(info.getPresettlePolicy());
        assertTrue(info.getPresettlePolicy() instanceof JmsDefaultPresettlePolicy);
    }

    @Test
    public void testGetRedeliveryPolicyDefaults() {
        final JmsSessionInfo info = new JmsSessionInfo(firstId);

        assertNotNull(info.getRedeliveryPolicy());
        info.setRedeliveryPolicy(null);
        assertNotNull(info.getRedeliveryPolicy());
        assertTrue(info.getRedeliveryPolicy() instanceof JmsDefaultRedeliveryPolicy);
    }
}
