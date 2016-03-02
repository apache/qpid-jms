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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsTransactionInfoTest {

    private JmsConnectionId firstId;
    private JmsConnectionId secondId;

    private JmsSessionId firstSessionId;
    private JmsSessionId secondSessionId;

    private JmsTransactionId firstTxId;
    private JmsTransactionId secondTxId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        firstId = new JmsConnectionId(generator.generateId());
        secondId = new JmsConnectionId(generator.generateId());

        firstSessionId = new JmsSessionId(firstId, 1);
        secondSessionId = new JmsSessionId(secondId, 2);

        firstTxId = new JmsTransactionId(firstId, 1);
        secondTxId = new JmsTransactionId(secondId, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowsWhenSessionIdIsNull() {
        new JmsTransactionInfo(null, firstTxId);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowsWhenTransactionIdIsNull() {
        new JmsTransactionInfo(firstSessionId, null);
    }

    @Test
    public void testCreateTransactionInfo() {
        JmsTransactionInfo info = new JmsTransactionInfo(firstSessionId, firstTxId);
        assertSame(firstSessionId, info.getSessionId());
        assertSame(firstTxId, info.getId());
        assertSame(firstSessionId, info.getSessionId());
        assertNotNull(info.toString());
    }

    @Test
    public void testCopy() {
        JmsTransactionInfo info = new JmsTransactionInfo(firstSessionId, firstTxId);
        JmsTransactionInfo copy = info.copy();
        assertEquals(info, copy);
    }

    @Test
    public void testCompareTo() {
        JmsTransactionInfo first = new JmsTransactionInfo(firstSessionId, firstTxId);
        JmsTransactionInfo second = new JmsTransactionInfo(secondSessionId, secondTxId);

        assertEquals(0, first.compareTo(first));
        assertFalse(second.compareTo(first) == 0);
    }

    @Test
    public void testHashCode() {
        JmsTransactionInfo first = new JmsTransactionInfo(firstSessionId, firstTxId);
        JmsTransactionInfo second = new JmsTransactionInfo(secondSessionId, secondTxId);

        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(second.hashCode(), second.hashCode());

        assertFalse(first.hashCode() == second.hashCode());
    }

    @Test
    public void testEqualsCode() {
        JmsTransactionInfo first = new JmsTransactionInfo(firstSessionId, firstTxId);
        JmsTransactionInfo second = new JmsTransactionInfo(secondSessionId, secondTxId);

        assertEquals(first, first);
        assertEquals(second, second);

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));

        assertFalse(first.equals(null));
        assertFalse(second.equals("test"));
    }

    @Test
    public void testVisit() throws Exception {
        final JmsTransactionInfo first = new JmsTransactionInfo(firstSessionId, firstTxId);

        final AtomicBoolean visited = new AtomicBoolean();

        first.visit(new JmsDefaultResourceVisitor() {

            @Override
            public void processTransactionInfo(JmsTransactionInfo info) {
                assertEquals(first, info);
                visited.set(true);
            }
        });

        assertTrue(visited.get());
    }
}
