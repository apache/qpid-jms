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
package org.apache.qpid.jms.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.DeliveryMode;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsMessageProducerTest extends JmsConnectionTestSupport {

    private JmsSession session;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockProvider();
        session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 10000)
    public void testMultipleCloseCallsNoErrors() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.close();
        producer.close();
    }

    @Test(timeout = 10000)
    public void testCreateProducerWithNullDestination() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertNull(producer.getDestination());
    }

    @Test(timeout = 10000)
    public void testGetDisableMessageID() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertFalse(producer.getDisableMessageID());
        producer.setDisableMessageID(true);
        assertTrue(producer.getDisableMessageID());
    }

    @Test(timeout = 10000)
    public void testGetDisableTimeStamp() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertFalse(producer.getDisableMessageTimestamp());
        producer.setDisableMessageTimestamp(true);
        assertTrue(producer.getDisableMessageTimestamp());
    }

    @Test(timeout = 10000)
    public void testPriorityConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        producer.setPriority(9);
        assertEquals(9, producer.getPriority());
    }

    @Test(timeout = 10000)
    public void testTimeToLiveConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_TIME_TO_LIVE, producer.getTimeToLive());
        producer.setTimeToLive(1000);
        assertEquals(1000, producer.getTimeToLive());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testAnonymousProducerThrowsUOEWhenExplictDestinationNotProvided() throws Exception {
        MessageProducer producer = session.createProducer(null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testExplicitProducerThrowsUOEWhenExplictDestinationIsProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        MessageProducer producer = session.createProducer(new JmsQueue());

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(dest, message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testAnonymousDestinationProducerThrowsIDEWhenNullDestinationIsProvided() throws Exception {
        MessageProducer producer = session.createProducer(null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(null, message);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }
}
