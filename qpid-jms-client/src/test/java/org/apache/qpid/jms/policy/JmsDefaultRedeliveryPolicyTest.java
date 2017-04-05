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
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.JmsMessageSupport;
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
    public void testCopyConstructor() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        policy.setOutcome(JmsMessageSupport.ACCEPTED);
        policy.setMaxRedeliveries(1000);

        JmsDefaultRedeliveryPolicy clone = new JmsDefaultRedeliveryPolicy(policy);

        assertEquals(JmsMessageSupport.ACCEPTED, clone.getOutcome());
        assertEquals(1000, clone.getMaxRedeliveries());
    }

    @Test
    public void testCopy() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        policy.setOutcome(JmsMessageSupport.ACCEPTED);
        policy.setMaxRedeliveries(1000);

        JmsDefaultRedeliveryPolicy clone = policy.copy();

        assertEquals(JmsMessageSupport.ACCEPTED, clone.getOutcome());
        assertEquals(1000, clone.getMaxRedeliveries());
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

        policy1.setMaxRedeliveries(5);
        policy2.setMaxRedeliveries(5);

        policy1.setOutcome(1);
        policy2.setOutcome(2);

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));
    }

    @Test
    public void testSetOutcomeFromInt() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_OUTCOME, policy.getOutcome());

        policy.setOutcome(JmsMessageSupport.ACCEPTED);
        assertEquals(JmsMessageSupport.ACCEPTED, policy.getOutcome());

        policy.setOutcome(JmsMessageSupport.REJECTED);
        assertEquals(JmsMessageSupport.REJECTED, policy.getOutcome());

        policy.setOutcome(JmsMessageSupport.RELEASED);
        assertEquals(JmsMessageSupport.RELEASED, policy.getOutcome());

        policy.setOutcome(JmsMessageSupport.MODIFIED_FAILED);
        assertEquals(JmsMessageSupport.MODIFIED_FAILED, policy.getOutcome());

        policy.setOutcome(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE);
        assertEquals(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE, policy.getOutcome());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetOutcomeWithInvalidIntValue() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();
        policy.setOutcome(100);
    }

    @Test
    public void testSetOutcomeFromString() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_OUTCOME, policy.getOutcome());

        policy.setOutcome("ACCEPTED");
        assertEquals(JmsMessageSupport.ACCEPTED, policy.getOutcome());

        policy.setOutcome("REJECTED");
        assertEquals(JmsMessageSupport.REJECTED, policy.getOutcome());

        policy.setOutcome("RELEASED");
        assertEquals(JmsMessageSupport.RELEASED, policy.getOutcome());

        policy.setOutcome("MODIFIED_FAILED");
        assertEquals(JmsMessageSupport.MODIFIED_FAILED, policy.getOutcome());

        policy.setOutcome("MODIFIED_FAILED_UNDELIVERABLE");
        assertEquals(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE, policy.getOutcome());
    }

    @Test
    public void testSetOutcomeFromStringUsingIntString() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_OUTCOME, policy.getOutcome());

        policy.setOutcome(Integer.toString(JmsMessageSupport.ACCEPTED));

        assertEquals(JmsMessageSupport.ACCEPTED, policy.getOutcome());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetOutcomeWithInvaliStringValue() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();
        policy.setOutcome("FOO");
    }

    @Test
    public void testGetOutcomeWithDestination() {
        JmsDefaultRedeliveryPolicy policy = new JmsDefaultRedeliveryPolicy();

        assertEquals(JmsDefaultRedeliveryPolicy.DEFAULT_OUTCOME, policy.getOutcome(new JmsTopic()));

        policy.setOutcome(JmsMessageSupport.ACCEPTED);

        assertEquals(JmsMessageSupport.ACCEPTED, policy.getOutcome(new JmsTopic()));
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
