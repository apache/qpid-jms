/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.impl;

import static org.junit.Assert.*;

import javax.jms.Queue;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;

public class QueueImplTest extends QpidJmsTestCase
{
    private String _testQueueName;
    private QueueImpl _testQueue;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _testQueueName = "testQueueName";
        _testQueue = new QueueImpl(_testQueueName);
    }

    @Test
    public void testGetQueueName() throws Exception
    {
        assertEquals(_testQueueName, _testQueue.getQueueName());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals(_testQueueName, _testQueue.toString());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception
    {
        Queue anotherEqualQueue = new QueueImpl(_testQueueName);
        Queue unequalQueue = new QueueImpl("otherName");

        assertFalse(_testQueue == anotherEqualQueue);
        assertFalse(_testQueue.equals(unequalQueue));

        assertTrue(_testQueue.equals(anotherEqualQueue));
        assertEquals(_testQueue.hashCode(), anotherEqualQueue.hashCode());
    }
}
