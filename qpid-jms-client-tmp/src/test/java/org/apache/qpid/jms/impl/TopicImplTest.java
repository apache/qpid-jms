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

import javax.jms.Topic;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;

public class TopicImplTest extends QpidJmsTestCase
{
    private String _testTopicName;
    private TopicImpl _testTopic;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _testTopicName = "testTopicName";
        _testTopic = new TopicImpl(_testTopicName);
    }

    @Test
    public void testGetTopicName() throws Exception
    {
        assertEquals(_testTopicName, _testTopic.getTopicName());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals(_testTopicName, _testTopic.toString());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception
    {
        Topic anotherEqualTopic = new TopicImpl(_testTopicName);
        Topic unequalTopic = new TopicImpl("otherName");

        assertFalse(_testTopic == anotherEqualTopic);
        assertFalse(_testTopic.equals(unequalTopic));

        assertTrue(_testTopic.equals(anotherEqualTopic));
        assertEquals(_testTopic.hashCode(), anotherEqualTopic.hashCode());
    }
}
