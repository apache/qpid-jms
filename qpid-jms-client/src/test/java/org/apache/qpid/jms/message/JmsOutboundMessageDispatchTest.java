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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JmsOutboundMessageDispatchTest {

    private JmsOutboundMessageDispatch envelope;

    @Before
    public void setUp() {
        envelope = new JmsOutboundMessageDispatch();
    }

    @Test
    public void testCreateState() {
        assertFalse(envelope.isSendAsync());
        assertNull(envelope.getDestination());
        assertNull(envelope.getMessage());
        assertNull(envelope.getProducerId());
        assertEquals(0, envelope.getDispatchId());

        envelope.setDestination(new JmsTopic("test"));
        envelope.setProducerId(new JmsProducerId("ID:test:1:0:1"));
        envelope.setDispatchId(1);
        envelope.setMessage(Mockito.mock(JmsMessage.class));
        envelope.setSendAsync(true);

        assertTrue(envelope.isSendAsync());
        assertNotNull(envelope.getDestination());
        assertNotNull(envelope.getMessage());
        assertNotNull(envelope.getProducerId());
        assertNotNull(envelope.getDispatchId());
    }

    @Test
    public void testToString() {
        envelope.setDispatchId(42);
        assertTrue(envelope.toString().startsWith("JmsOutboundMessageDispatch"));
        assertTrue(envelope.toString().contains("42"));
    }

    @Test
    public void testToStringNullDispatchId() {
        assertTrue(envelope.toString().startsWith("JmsOutboundMessageDispatch"));
    }
}
