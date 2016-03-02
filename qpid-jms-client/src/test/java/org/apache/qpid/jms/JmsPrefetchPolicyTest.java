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
package org.apache.qpid.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *
 */
public class JmsPrefetchPolicyTest {

    @Test
    public void testHashCode() {
        JmsPrefetchPolicy policy1 = new JmsPrefetchPolicy();
        JmsPrefetchPolicy policy2 = new JmsPrefetchPolicy();

        assertTrue(policy1.hashCode() != 0);
        assertEquals(policy1.hashCode(), policy1.hashCode());
        assertEquals(policy2.hashCode(), policy2.hashCode());
    }

    @Test
    public void testJmsPrefetchPolicy() {
        JmsPrefetchPolicy policy = new JmsPrefetchPolicy();

        assertEquals(JmsPrefetchPolicy.DEFAULT_TOPIC_PREFETCH, policy.getTopicPrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH, policy.getDurableTopicPrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_QUEUE_PREFETCH, policy.getQueuePrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH, policy.getQueueBrowserPrefetch());
        assertEquals(JmsPrefetchPolicy.MAX_PREFETCH_SIZE, policy.getMaxPrefetchSize());
    }

    @Test
    public void testJmsPrefetchPolicyJmsPrefetchPolicy() {
        JmsPrefetchPolicy policy1 = new JmsPrefetchPolicy();
        policy1.setTopicPrefetch(10);
        policy1.setDurableTopicPrefetch(20);
        policy1.setQueueBrowserPrefetch(30);
        policy1.setQueuePrefetch(40);
        policy1.setMaxPrefetchSize(100);

        JmsPrefetchPolicy policy2 = new JmsPrefetchPolicy(policy1);

        assertEquals(policy1.getTopicPrefetch(), policy2.getTopicPrefetch());
        assertEquals(policy1.getDurableTopicPrefetch(), policy2.getDurableTopicPrefetch());
        assertEquals(policy1.getQueuePrefetch(), policy2.getQueuePrefetch());
        assertEquals(policy1.getQueueBrowserPrefetch(), policy2.getQueueBrowserPrefetch());
        assertEquals(policy1.getMaxPrefetchSize(), policy2.getMaxPrefetchSize());
    }

    @Test
    public void testGetMaxPrefetchSize() {
        JmsPrefetchPolicy policy = new JmsPrefetchPolicy();
        assertEquals(JmsPrefetchPolicy.MAX_PREFETCH_SIZE, policy.getMaxPrefetchSize());
        policy.setMaxPrefetchSize(10);
        assertEquals(10, policy.getMaxPrefetchSize());
    }

    @Test
    public void testMaxPrefetchSizeIsHonored() {
        JmsPrefetchPolicy policy = new JmsPrefetchPolicy();
        assertEquals(JmsPrefetchPolicy.MAX_PREFETCH_SIZE, policy.getMaxPrefetchSize());
        policy.setMaxPrefetchSize(42);
        assertEquals(42, policy.getMaxPrefetchSize());

        policy.setTopicPrefetch(100);
        policy.setDurableTopicPrefetch(100);
        policy.setQueueBrowserPrefetch(100);
        policy.setQueuePrefetch(100);

        assertEquals(42, policy.getTopicPrefetch());
        assertEquals(42, policy.getDurableTopicPrefetch());
        assertEquals(42, policy.getQueuePrefetch());
        assertEquals(42, policy.getQueueBrowserPrefetch());
    }

    @Test
    public void testSetAll() {
        JmsPrefetchPolicy policy = new JmsPrefetchPolicy();

        assertEquals(JmsPrefetchPolicy.DEFAULT_TOPIC_PREFETCH, policy.getTopicPrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH, policy.getDurableTopicPrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_QUEUE_PREFETCH, policy.getQueuePrefetch());
        assertEquals(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH, policy.getQueueBrowserPrefetch());

        policy.setAll(42);

        assertEquals(42, policy.getTopicPrefetch());
        assertEquals(42, policy.getDurableTopicPrefetch());
        assertEquals(42, policy.getQueuePrefetch());
        assertEquals(42, policy.getQueueBrowserPrefetch());
    }

    @Test
    public void testEqualsObject() {
        JmsPrefetchPolicy policy1 = new JmsPrefetchPolicy();
        JmsPrefetchPolicy policy2 = new JmsPrefetchPolicy();

        assertEquals(policy1, policy1);
        assertEquals(policy1, policy2);

        JmsPrefetchPolicy policy3 = new JmsPrefetchPolicy();
        policy3.setTopicPrefetch(10);
        JmsPrefetchPolicy policy4 = new JmsPrefetchPolicy();
        policy4.setQueuePrefetch(10);
        JmsPrefetchPolicy policy5 = new JmsPrefetchPolicy();
        policy5.setDurableTopicPrefetch(10);
        JmsPrefetchPolicy policy6 = new JmsPrefetchPolicy();
        policy6.setQueueBrowserPrefetch(10);

        assertFalse(policy1.equals(policy3));
        assertFalse(policy1.equals(policy4));
        assertFalse(policy1.equals(policy5));
        assertFalse(policy1.equals(policy6));

        assertFalse(policy1.equals(null));
        assertFalse(policy1.equals(""));
    }
}
