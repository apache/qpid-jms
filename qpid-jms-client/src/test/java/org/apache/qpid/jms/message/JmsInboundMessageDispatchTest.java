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
package org.apache.qpid.jms.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.jupiter.api.Test;

public class JmsInboundMessageDispatchTest {

    @Test
    public void testEqualsWithNullAndOtherTypes() {
        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(1);
        assertFalse(envelope.equals(null));
        assertFalse(envelope.equals(""));
    }

    @Test
    public void testEqualAndHashCodeWithNotInitializedAndInitializedEnvelopes() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        JmsConsumerId consumerId2 = new JmsConsumerId(sessionId, 2);
        envelope2.setConsumerId(consumerId2);
        envelope2.setMessageId("myMessageId");

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");

        envelope2.setMessageId(null);
        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");

        envelope2.setConsumerId(null);
        assertTrue(envelope1.equals(envelope2), "objects should be equal");
        assertTrue(envelope2.equals(envelope1), "objects should still be equal");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceOnly() {
        int sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);

        assertTrue(envelope1.equals(envelope2), "objects should be equal");
        assertTrue(envelope2.equals(envelope1), "objects should still be equal");

        assertEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should be the same");
    }

    @Test
    public void testEqualAndHashCodeWithDifferentSequenceOnly() {
        int sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence + 1);

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceDifferentConsumerId() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        JmsConsumerId consumerId1 = new JmsConsumerId(sessionId, 1);
        envelope1.setConsumerId(consumerId1);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        JmsConsumerId consumerId2 = new JmsConsumerId(sessionId, 2);
        envelope2.setConsumerId(consumerId2);

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceSameConsumerId() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        envelope1.setConsumerId(consumerId);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        envelope2.setConsumerId(consumerId);

        assertTrue(envelope1.equals(envelope2), "objects should be equal");
        assertTrue(envelope2.equals(envelope1), "objects should still be equal");

        assertEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should be the same");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceSameConsumerIdSameMessageId() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);
        Object messageId = "myMessageId";

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        envelope1.setConsumerId(consumerId);
        envelope1.setMessageId(messageId);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        envelope2.setConsumerId(consumerId);
        envelope2.setMessageId(messageId);

        assertTrue(envelope1.equals(envelope2), "objects should be equal");
        assertTrue(envelope2.equals(envelope1), "objects should still be equal");

        assertEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should be the same");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceSameConsumerIdDifferentMessageIdTypes() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);
        Object messageId1 = new Binary(new byte[] { (byte) 1, (byte) 0 });
        Object messageId2 = UnsignedLong.valueOf(2);

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        envelope1.setConsumerId(consumerId);
        envelope1.setMessageId(messageId1);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        envelope2.setConsumerId(consumerId);
        envelope2.setMessageId(messageId2);

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceSameConsumerIdDifferentMessageIdTypes2() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);
        Object messageId1 = UUID.randomUUID();
        Object messageId2 = messageId1.toString();

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        envelope1.setConsumerId(consumerId);
        envelope1.setMessageId(messageId1);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);
        envelope2.setConsumerId(consumerId);
        envelope2.setMessageId(messageId2);

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");
    }

    @Test
    public void testEqualAndHashCodeWithDifferentSequenceSameConsumerIdSameMessageId() {
        JmsSessionId sessionId = new JmsSessionId("con", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);
        Object messageId = "myMessageId";

        long sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        envelope1.setConsumerId(consumerId);
        envelope1.setMessageId(messageId);

        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence + 1);
        envelope2.setConsumerId(consumerId);
        envelope2.setMessageId(messageId);

        assertFalse(envelope1.equals(envelope2), "objects should not be equal");
        assertFalse(envelope2.equals(envelope1), "objects should still not be equal");

        // Not strictly a requirement, but expected in this case
        assertNotEquals(envelope1.hashCode(), envelope2.hashCode(), "hashCodes should not be the same");
    }
}
