/*
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
 */
package org.apache.qpid.jms.provider.amqp;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SUB_NAME_DELIMITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSRuntimeException;

import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.junit.Test;

public class AmqpSubscriptionTrackerTest {

    private AtomicInteger consumerIdCounter = new AtomicInteger();

    private JmsConsumerInfo createConsumerInfo(String subscriptionName, String topicName, boolean shared, boolean durable, boolean hasClientID) {
        return createConsumerInfo(subscriptionName, topicName, shared, durable, null, hasClientID);
    }

    private JmsConsumerInfo createConsumerInfo(String subscriptionName, String topicName, boolean shared, boolean durable, String selector, boolean isExplicitClientID) {
        JmsConsumerId consumerId = new JmsConsumerId("ID:MOCK:1", 1, consumerIdCounter.incrementAndGet());
        JmsTopic topic = new JmsTopic(topicName);

        JmsConsumerInfo consumerInfo = new JmsConsumerInfo(consumerId, null);

        consumerInfo.setSubscriptionName(subscriptionName);
        consumerInfo.setDestination(topic);
        consumerInfo.setShared(shared);
        consumerInfo.setDurable(durable);
        consumerInfo.setSelector(selector);
        consumerInfo.setExplicitClientID(isExplicitClientID);

        return consumerInfo;
    }

    @Test
    public void testReserveNextSubscriptionLinkNameExceptions() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        JmsConsumerInfo consumerInfo = createConsumerInfo(subscriptionName1, topicName, true, true, true);

        try {
            tracker.reserveNextSubscriptionLinkName(null, consumerInfo);
            fail("Should throw IAE on null subscription name");
        } catch (IllegalArgumentException iae) {}

        try {
            tracker.reserveNextSubscriptionLinkName("", consumerInfo);
            fail("Should throw IAE on empty subscription name");
        } catch (IllegalArgumentException iae) {}

        try {
            tracker.reserveNextSubscriptionLinkName("test" + SUB_NAME_DELIMITER, consumerInfo);
            fail("Should throw IAE on subscription name with delimiter");
        } catch (IllegalArgumentException iae) {}

        try {
            tracker.reserveNextSubscriptionLinkName("test", null);
            fail("Should throw IAE on null Consumer Info");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedDurable() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        assertEquals("Unexpected second sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "2", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2));

        // For the second shared sub name
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName2, tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer1));
        JmsConsumerInfo sub2consumer2 = createConsumerInfo(subscriptionName2, topicName, true, true, true);
        assertEquals("Unexpected second sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "2", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer2));

        // Register a third subscriber for a subscription, after removing the first subscriber for the subscription.
        // Validate the new link name isn't the same as the second subscribers (which is still using its name...)
        tracker.consumerRemoved(sub2consumer1);
        JmsConsumerInfo sub2consumer3 = createConsumerInfo(subscriptionName2, topicName, true, true, true);
        assertEquals("Unexpected third subscriber link name", subscriptionName2 + SUB_NAME_DELIMITER + "3", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer3));
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedDurableWithNonMatchingSelector() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name with selector 'color = red'
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, true, "color = red", true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // For the next shared sub name with selector 'color = blue'
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, true, "color = blue", true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }

        // For the next shared sub name with selector 'color = blue'
        JmsConsumerInfo sub1consumer3 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer3);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }

        // Remove the consumer and add a new one with no selector
        tracker.consumerRemoved(sub1consumer1);

        JmsConsumerInfo sub1consumer4 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer4));

        // Try adding the second consumer again with selector "color = blue"
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedDurableWithNonMatchingTopic() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name on Topic
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // For the next shared sub name on different Topic
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName + "-Alt", true, true, true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when Topic doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedDurableWithoutClientID() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, true, false);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "global", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, true, false);
        assertEquals("Unexpected second sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "global2", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2));

        // For the second shared sub name
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, true, false);
        assertEquals("Unexpected first sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "global", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer1));
        JmsConsumerInfo sub2consumer2 = createConsumerInfo(subscriptionName2, topicName, true, true, false);
        assertEquals("Unexpected second sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "global2", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer2));

        // Register a third subscriber for a subscription, after removing the first subscriber for the subscription.
        // Validate the new link name isn't the same as the second subscribers (which is still using its name...)
        tracker.consumerRemoved(sub2consumer1);
        JmsConsumerInfo sub2consumer3 = createConsumerInfo(subscriptionName2, topicName, true, true, false);
        assertEquals("Unexpected third subscriber link name", subscriptionName2 + SUB_NAME_DELIMITER + "global3", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer3));
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedVolatile() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName1));
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, false, true);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName1));
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, false, true);
        assertEquals("Unexpected second sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile2", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName1));

        // For the second shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, false, true);
        assertEquals("Unexpected first sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer1));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));
        JmsConsumerInfo sub2consumer2 = createConsumerInfo(subscriptionName2, topicName, true, false, true);
        assertEquals("Unexpected second sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "volatile2", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer2));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));

        // Register a third subscriber for a subscription, after removing the first subscriber for the subscription.
        // Validate the new link name isn't the same as the second subscribers (which is still using its name...)
        tracker.consumerRemoved(sub2consumer1);
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));
        JmsConsumerInfo sub2consumer3 = createConsumerInfo(subscriptionName2, topicName, true, false, true);
        assertEquals("Unexpected third subscriber link name", subscriptionName2 + SUB_NAME_DELIMITER + "volatile3", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer3));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedVolatileWithNonMatchingSelector() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name with selector 'color = red'
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, false, "color = red", true);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // For the next shared sub name with selector 'color = blue'
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, false, "color = blue", true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }

        // For the first shared sub name with no selector
        JmsConsumerInfo sub1consumer3 = createConsumerInfo(subscriptionName1, topicName, true, false, true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer3);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }

        // Remove the consumer and add the third one which has no selector
        tracker.consumerRemoved(sub1consumer1);

        assertEquals("Unexpected second sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer3));

        // Try adding the second consumer again with selector "color = blue"
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when selector doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedVolatileWithNonMatchingTopic() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name with Topic
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, false, true);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // For the next shared sub name with different Topic
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName + "-alt", true, false, true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2);
            fail("Expected JMSRuntimeException when Topic doesn't match previous subscription");
        } catch (JMSRuntimeException jmsre) {
        }
    }

    @Test
    public void testReserveNextSubscriptionLinkNameSharedVolatileWithoutClientID() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName1));
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, false, false);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "global-volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, true, false, false);
        assertEquals("Unexpected second sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "global-volatile2", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName1));

        // For the second shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, false, false);
        assertEquals("Unexpected first sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "global-volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer1));
        JmsConsumerInfo sub2consumer2 = createConsumerInfo(subscriptionName2, topicName, true, false, false);
        assertEquals("Unexpected second sub link name", subscriptionName2 + SUB_NAME_DELIMITER + "global-volatile2", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer2));
        assertTrue("Should be active shard volatile sub", tracker.isActiveSharedVolatileSub(subscriptionName2));

        // Register a third subscriber for a subscription, after removing the first subscriber for the subscription.
        // Validate the new link name isn't the same as the second subscribers (which is still using its name...)
        tracker.consumerRemoved(sub2consumer1);
        JmsConsumerInfo sub2consumer3 = createConsumerInfo(subscriptionName2, topicName, true, false, false);
        assertEquals("Unexpected third subscriber link name", subscriptionName2 + SUB_NAME_DELIMITER + "global-volatile3", tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer3));
    }

    @Test
    public void testReserveNextSubscriptionLinkNameExclusiveDurable() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveExclusiveDurableSub(subscriptionName1));
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, false, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));
        assertTrue("Should be active shard volatile sub", tracker.isActiveExclusiveDurableSub(subscriptionName1));
        // This shouldn't happen, checks elsewhere should stop requests for an exclusive durable sub link
        // name if its already in use, but check we get the same name anyway even with an existing registration.
        JmsConsumerInfo sub1consumer2 = createConsumerInfo(subscriptionName1, topicName, false, true, true);
        assertEquals("Unexpected second sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer2));

        // For the second shared sub name
        assertFalse("Should be active shard volatile sub", tracker.isActiveExclusiveDurableSub(subscriptionName2));
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, false, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName2, tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer1));
        assertTrue("Should be active shard volatile sub", tracker.isActiveExclusiveDurableSub(subscriptionName2));
        // This shouldn't happen, checks elsewhere should stop requests for an exclusive durable sub link
        // name if its already in use, but check we get the same name anyway even with an existing registration.
        JmsConsumerInfo sub2consumer2 = createConsumerInfo(subscriptionName2, topicName, false, true, true);
        assertEquals("Unexpected second sub link name", subscriptionName2, tracker.reserveNextSubscriptionLinkName(subscriptionName2, sub2consumer2));
    }

    @Test
    public void testReserveNextSubscriptionLinkNameExclusiveNonDurable() {
        String topicName = "myTopic";
        String subscriptionName = "mySubscription";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        JmsConsumerInfo subInfo = createConsumerInfo(subscriptionName, topicName, false, false, true);
        try {
            tracker.reserveNextSubscriptionLinkName(subscriptionName, subInfo);
            fail("Should have thrown exception, tracker doesn't name these subs");
        } catch (IllegalStateException ise) {
            // Expected
        }

        // Verify it no-ops with an exclusive non-durable sub info
        tracker.consumerRemoved(subInfo);
    }

    @Test
    public void testIsActiveExclusiveDurableSub() {
        String subscriptionName1 = "mySubscription";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        assertFalse(tracker.isActiveExclusiveDurableSub(subscriptionName1));

        JmsConsumerInfo subInfo = createConsumerInfo(subscriptionName1, "myTopic", false, true, true);
        tracker.reserveNextSubscriptionLinkName(subscriptionName1, subInfo);

        assertTrue(tracker.isActiveExclusiveDurableSub(subscriptionName1));

        tracker.consumerRemoved(subInfo);

        assertFalse(tracker.isActiveExclusiveDurableSub(subscriptionName1));
    }

    @Test
    public void testIsActiveSharedDurableSub() {
        String subscriptionName1 = "mySubscription";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        assertFalse(tracker.isActiveSharedDurableSub(subscriptionName1));

        JmsConsumerInfo subInfo = createConsumerInfo(subscriptionName1, "myTopic", true, true, true);
        tracker.reserveNextSubscriptionLinkName(subscriptionName1, subInfo);

        assertTrue(tracker.isActiveSharedDurableSub(subscriptionName1));

        tracker.consumerRemoved(subInfo);

        assertFalse(tracker.isActiveSharedDurableSub(subscriptionName1));
    }

    @Test
    public void testIsActiveDurableSub() {
        String subscriptionName = "mySubscription";

        // Test when an exclusive durable sub is active
        AmqpSubscriptionTracker tracker1 = new AmqpSubscriptionTracker();

        assertFalse(tracker1.isActiveDurableSub(subscriptionName));

        JmsConsumerInfo subInfo1 = createConsumerInfo(subscriptionName, "myTopic", false, true, true);
        tracker1.reserveNextSubscriptionLinkName(subscriptionName, subInfo1);

        assertTrue(tracker1.isActiveDurableSub(subscriptionName));
        assertTrue(tracker1.isActiveExclusiveDurableSub(subscriptionName));
        assertFalse(tracker1.isActiveSharedDurableSub(subscriptionName));

        tracker1.consumerRemoved(subInfo1);

        assertFalse(tracker1.isActiveDurableSub(subscriptionName));
        assertFalse(tracker1.isActiveExclusiveDurableSub(subscriptionName));
        assertFalse(tracker1.isActiveSharedDurableSub(subscriptionName));

        // Test when an shared durable sub is active
        AmqpSubscriptionTracker tracker2 = new AmqpSubscriptionTracker();

        assertFalse(tracker2.isActiveDurableSub(subscriptionName));

        JmsConsumerInfo subInfo2 = createConsumerInfo(subscriptionName, "myTopic", true, true, true);
        tracker2.reserveNextSubscriptionLinkName(subscriptionName, subInfo2);

        assertTrue(tracker2.isActiveDurableSub(subscriptionName));
        assertFalse(tracker2.isActiveExclusiveDurableSub(subscriptionName));
        assertTrue(tracker2.isActiveSharedDurableSub(subscriptionName));

        tracker2.consumerRemoved(subInfo2);

        assertFalse(tracker2.isActiveDurableSub(subscriptionName));
        assertFalse(tracker2.isActiveExclusiveDurableSub(subscriptionName));
        assertFalse(tracker2.isActiveSharedDurableSub(subscriptionName));
    }

    @Test
    public void testConsumerRemovedIgnoresConsumersWithoutSubscriptions() {
        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // Should not throw for no subscription
        JmsConsumerInfo subInfo1 = createConsumerInfo(null, "myTopic", true, true, true);
        tracker.consumerRemoved(subInfo1);

        // Should not throw for empty subscription
        JmsConsumerInfo subInfo2 = createConsumerInfo("", "myTopic", true, true, true);
        tracker.consumerRemoved(subInfo2);
    }

    @Test
    public void testConsumerRemovedIgnoresUntrackedSharedDurable() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, true, true);
        assertEquals("Unexpected first sub link name", subscriptionName1, tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // Create another shared sub that is not registered and remove it
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, true, true);

        tracker.consumerRemoved(sub2consumer1);

        // First sub should still be active
        assertTrue(tracker.isActiveSharedDurableSub(subscriptionName1));

        // remove the first one and it should now go inactive.
        tracker.consumerRemoved(sub1consumer1);

        // First sub should still be active
        assertFalse(tracker.isActiveSharedDurableSub(subscriptionName1));
    }

    @Test
    public void testConsumerRemovedIgnoresUntrackedSharedVolatile() {
        String topicName = "myTopic";
        String subscriptionName1 = "mySubscription1";
        String subscriptionName2 = "mySubscription2";

        AmqpSubscriptionTracker tracker = new AmqpSubscriptionTracker();

        // For the first shared sub name
        JmsConsumerInfo sub1consumer1 = createConsumerInfo(subscriptionName1, topicName, true, false, true);
        assertEquals("Unexpected first sub link name", subscriptionName1 + SUB_NAME_DELIMITER + "volatile1", tracker.reserveNextSubscriptionLinkName(subscriptionName1, sub1consumer1));

        // Create another shared sub that is not registered and remove it
        JmsConsumerInfo sub2consumer1 = createConsumerInfo(subscriptionName2, topicName, true, false, true);

        tracker.consumerRemoved(sub2consumer1);

        // First sub should still be active
        assertTrue(tracker.isActiveSharedVolatileSub(subscriptionName1));

        // remove the first one and it should now go inactive.
        tracker.consumerRemoved(sub1consumer1);

        // First sub should still be active
        assertFalse(tracker.isActiveSharedVolatileSub(subscriptionName1));
    }
}