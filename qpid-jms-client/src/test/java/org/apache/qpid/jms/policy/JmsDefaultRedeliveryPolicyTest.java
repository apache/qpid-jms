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
package org.apache.qpid.jms.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.junit.Test;

/**
 * Test for the Default Redelivery Policy object.
 */
public class JmsDefaultRedeliveryPolicyTest {

    @Test
    public void testDefaults() {
        JmsDestination destination = new JmsQueue("test");
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES, policy.getMaxRedeliveries(destination));
    }

    @Test
    public void testSetMaxRedeliveries() {
        JmsDestination destination = new JmsQueue("test");
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        policy.setMaxRedeliveries(JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES + 1);

        assertFalse(JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES == policy.getMaxRedeliveries(destination));

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES + 1, policy.getMaxRedeliveries());
        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES + 1, policy.getMaxRedeliveries(destination));
    }

    @Test
    public void testEquals() {
        JmsDefaultRedeliveryPolicy policy1 = new JmsDefaultRedeliveryPolicy();
        JmsDefaultRedeliveryPolicy policy2 = new JmsDefaultRedeliveryPolicy();

        assertTrue(policy1.equals(policy1));
        assertTrue(policy1.equals(policy2));
        assertTrue(policy2.equals(policy1));
        assertFalse(policy1.equals(null));
        assertFalse(policy1.equals("test"));

        policy1.setMaxRedeliveries(5);
        policy2.setMaxRedeliveries(6);

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));
    }

    @Test
    public void testHashCode() {
        JmsDefaultRedeliveryPolicy policy1 = new JmsDefaultRedeliveryPolicy();
        JmsDefaultRedeliveryPolicy policy2 = new JmsDefaultRedeliveryPolicy();

        assertEquals(policy1.hashCode(), policy2.hashCode());

        policy1.setMaxRedeliveries(5);
        policy2.setMaxRedeliveries(6);

        assertFalse(policy1.hashCode() == policy2.hashCode());
    }
}
