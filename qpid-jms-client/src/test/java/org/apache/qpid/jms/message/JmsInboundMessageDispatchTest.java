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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.Test;

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

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());

        envelope2.setMessageId(null);
        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());

        envelope2.setConsumerId(null);
        assertTrue("objects should be equal", envelope1.equals(envelope2));
        assertTrue("objects should still be equal", envelope2.equals(envelope1));
    }

    @Test
    public void testEqualAndHashCodeWithSameSequenceOnly() {
        int sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence);

        assertTrue("objects should be equal", envelope1.equals(envelope2));
        assertTrue("objects should still be equal", envelope2.equals(envelope1));

        assertEquals("hashCodes should be the same", envelope1.hashCode(), envelope2.hashCode());
    }

    @Test
    public void testEqualAndHashCodeWithDifferentSequenceOnly() {
        int sequence = 1;
        JmsInboundMessageDispatch envelope1 = new JmsInboundMessageDispatch(sequence);
        JmsInboundMessageDispatch envelope2 = new JmsInboundMessageDispatch(sequence + 1);

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertTrue("objects should be equal", envelope1.equals(envelope2));
        assertTrue("objects should still be equal", envelope2.equals(envelope1));

        assertEquals("hashCodes should be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertTrue("objects should be equal", envelope1.equals(envelope2));
        assertTrue("objects should still be equal", envelope2.equals(envelope1));

        assertEquals("hashCodes should be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());
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

        assertFalse("objects should not be equal", envelope1.equals(envelope2));
        assertFalse("objects should still not be equal", envelope2.equals(envelope1));

        // Not strictly a requirement, but expected in this case
        assertNotEquals("hashCodes should not be the same", envelope1.hashCode(), envelope2.hashCode());
    }
}
