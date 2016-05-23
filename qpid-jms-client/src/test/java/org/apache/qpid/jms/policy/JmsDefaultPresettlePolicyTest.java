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
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test basic behavior of the JmsDefaultPresettlePolicy
 */
public class JmsDefaultPresettlePolicyTest {

    @Test
    public void testIsConsumerPresettledPresettleAll() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isConsumerPresttled(session, destination));
        assertFalse(policy.isProducerPresttled(session, null));

        policy.setPresettleAll(true);
        assertTrue(policy.isConsumerPresttled(session, destination));
        assertFalse(policy.isConsumerPresttled(session, null));
    }

    @Test
    public void testIsProducerPresettledPresettleAll() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isProducerPresttled(session, destination));
        assertFalse(policy.isProducerPresttled(session, null));

        policy.setPresettleAll(true);
        assertTrue(policy.isProducerPresttled(session, destination));
        assertTrue(policy.isProducerPresttled(session, null));
    }

    @Test
    public void testIsConsumerPresettledPresettleConsumer() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isConsumerPresttled(session, destination));
        assertFalse(policy.isConsumerPresttled(session, null));

        policy.setPresettleConsumers(true);
        assertTrue(policy.isConsumerPresttled(session, destination));
        assertFalse(policy.isConsumerPresttled(session, null));
    }

    @Test
    public void testIsProducerPresettledPresettleProducers() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isProducerPresttled(session, destination));
        assertFalse(policy.isProducerPresttled(session, null));

        policy.setPresettleProducers(true);
        assertTrue(policy.isProducerPresttled(session, destination));
        assertTrue(policy.isProducerPresttled(session, null));
    }

    @Test
    public void testIsConsumerPresettledPresettleTopicConsumer() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");

        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isConsumerPresttled(session, queue));
        assertFalse(policy.isConsumerPresttled(session, topic));
        assertFalse(policy.isConsumerPresttled(session, null));

        policy.setPresettleTopicConsumers(true);
        assertFalse(policy.isConsumerPresttled(session, queue));
        assertTrue(policy.isConsumerPresttled(session, topic));
        assertFalse(policy.isConsumerPresttled(session, null));
    }

    @Test
    public void testIsConsumerPresettledPresettleQueueConsumer() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");

        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isConsumerPresttled(session, queue));
        assertFalse(policy.isConsumerPresttled(session, topic));
        assertFalse(policy.isConsumerPresttled(session, null));

        policy.setPresettleQueueConsumers(true);
        assertTrue(policy.isConsumerPresttled(session, queue));
        assertFalse(policy.isConsumerPresttled(session, topic));
        assertFalse(policy.isConsumerPresttled(session, null));
    }

    @Test
    public void testIsProducerPresettledPresettleTopicProducers() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");

        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isProducerPresttled(session, queue));
        assertFalse(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));

        policy.setPresettleTopicProducers(true);
        assertFalse(policy.isProducerPresttled(session, queue));
        assertTrue(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));
    }

    @Test
    public void testIsProducerPresettledPresettleQueueProducers() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");

        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isProducerPresttled(session, queue));
        assertFalse(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));

        policy.setPresettleQueueProducers(true);
        assertTrue(policy.isProducerPresttled(session, queue));
        assertFalse(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));
    }

    @Test
    public void testIsProducerPresettledPresettleTransactedProducers() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");

        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isProducerPresttled(session, queue));
        assertFalse(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));

        Mockito.when(session.isTransacted()).thenReturn(true);

        policy.setPresettleTransactedProducers(true);
        assertTrue(policy.isProducerPresttled(session, queue));
        assertTrue(policy.isProducerPresttled(session, topic));
        assertFalse(policy.isProducerPresttled(session, null));
    }

    @Test
    public void testPresettleAll() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();

        assertFalse(policy.isPresettleAll());
        policy.setPresettleAll(true);
        assertTrue(policy.isPresettleAll());

        assertTrue(policy.isPresettleConsumers());
        assertTrue(policy.isPresettleQueueConsumers());
        assertTrue(policy.isPresettleTopicConsumers());
        assertTrue(policy.isPresettleProducers());
        assertTrue(policy.isPresettleQueueProducers());
        assertTrue(policy.isPresettleTopicProducers());
        assertTrue(policy.isPresettleTransactedProducers());

        policy.setPresettleAll(false);

        assertFalse(policy.isPresettleConsumers());
        assertFalse(policy.isPresettleProducers());
        assertFalse(policy.isPresettleQueueConsumers());
        assertFalse(policy.isPresettleQueueProducers());
        assertFalse(policy.isPresettleTopicConsumers());
        assertFalse(policy.isPresettleTopicProducers());
        assertFalse(policy.isPresettleTransactedProducers());
    }

    @Test
    public void testPresettleConsumers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();

        assertFalse(policy.isPresettleConsumers());

        policy.setPresettleConsumers(true);

        assertTrue(policy.isPresettleConsumers());
        assertTrue(policy.isPresettleQueueConsumers());
        assertTrue(policy.isPresettleTopicConsumers());

        policy.setPresettleConsumers(false);

        assertFalse(policy.isPresettleConsumers());
        assertFalse(policy.isPresettleQueueConsumers());
        assertFalse(policy.isPresettleTopicConsumers());
    }

    @Test
    public void testPresettleProducers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();

        assertFalse(policy.isPresettleProducers());

        policy.setPresettleProducers(true);

        assertTrue(policy.isPresettleProducers());
        assertTrue(policy.isPresettleQueueProducers());
        assertTrue(policy.isPresettleTopicProducers());

        policy.setPresettleProducers(false);

        assertFalse(policy.isPresettleProducers());
        assertFalse(policy.isPresettleQueueProducers());
        assertFalse(policy.isPresettleTopicProducers());
    }

    @Test
    public void testPresettleQueueProducers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isPresettleQueueProducers());

        policy.setPresettleQueueProducers(true);
        assertTrue(policy.isPresettleQueueProducers());

        policy.setPresettleQueueProducers(false);
        assertFalse(policy.isPresettleQueueProducers());
    }

    @Test
    public void testPresettleTopicProducers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isPresettleTopicProducers());

        policy.setPresettleTopicProducers(true);
        assertTrue(policy.isPresettleTopicProducers());

        policy.setPresettleTopicProducers(false);
        assertFalse(policy.isPresettleTopicProducers());
    }

    @Test
    public void testPresettleTransactedProducers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isPresettleTransactedProducers());

        policy.setPresettleTransactedProducers(true);
        assertTrue(policy.isPresettleTransactedProducers());

        policy.setPresettleTransactedProducers(false);
        assertFalse(policy.isPresettleTransactedProducers());
    }

    @Test
    public void testPresettleTopicConsumers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isPresettleTopicConsumers());

        policy.setPresettleTopicConsumers(true);
        assertTrue(policy.isPresettleTopicConsumers());

        policy.setPresettleTopicConsumers(false);
        assertFalse(policy.isPresettleTopicConsumers());
    }

    @Test
    public void testPresettleQueueConsumers() {
        JmsDefaultPresettlePolicy policy = new JmsDefaultPresettlePolicy();
        assertFalse(policy.isPresettleQueueConsumers());

        policy.setPresettleQueueConsumers(true);
        assertTrue(policy.isPresettleQueueConsumers());

        policy.setPresettleQueueConsumers(false);
        assertFalse(policy.isPresettleQueueConsumers());
    }

    @Test
    public void testEquals() {
        JmsDefaultPresettlePolicy policy1 = new JmsDefaultPresettlePolicy();
        JmsDefaultPresettlePolicy policy2 = new JmsDefaultPresettlePolicy();

        assertTrue(policy1.equals(policy1));
        assertTrue(policy1.equals(policy2));
        assertTrue(policy2.equals(policy1));
        assertFalse(policy1.equals(null));
        assertFalse(policy1.equals("test"));

        policy1.setPresettleAll(false);
        policy1.setPresettleAll(true);

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(false);
        policy1.setPresettleQueueProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(false);
        policy1.setPresettleQueueProducers(false);
        policy1.setPresettleTransactedProducers(true);

        assertFalse(policy2.equals(policy1));
    }

    @Test
    public void testHashCode() {
        JmsDefaultPresettlePolicy policy1 = new JmsDefaultPresettlePolicy();
        JmsDefaultPresettlePolicy policy2 = new JmsDefaultPresettlePolicy();

        assertEquals(policy1.hashCode(), policy2.hashCode());

        policy1.setPresettleAll(false);
        policy2.setPresettleAll(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(false);
        policy1.setPresettleQueueProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setPresettleAll(false);
        policy1.setPresettleProducers(false);
        policy2.setPresettleConsumers(false);
        policy2.setPresettleTopicConsumers(false);
        policy2.setPresettleQueueConsumers(false);
        policy1.setPresettleTopicProducers(false);
        policy1.setPresettleQueueProducers(false);
        policy1.setPresettleTransactedProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());
    }
}
