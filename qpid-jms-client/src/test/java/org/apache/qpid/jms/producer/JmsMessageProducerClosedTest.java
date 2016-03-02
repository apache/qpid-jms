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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the contract of MessageProducer that has been closed.
 */
public class JmsMessageProducerClosedTest extends JmsConnectionTestSupport {

    protected MessageProducer producer;
    protected Message message;
    protected Destination destination;

    protected MessageProducer createProducer() throws Exception {
        connection = createConnectionToMockProvider();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        message = session.createMessage();
        destination = session.createTopic("test");
        MessageProducer producer = session.createProducer(destination);
        producer.close();
        return producer;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        producer = createProducer();
    }

    @Test(timeout=30000)
    public void testClose() throws Exception {
        producer.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDisableMessageIDFails() throws Exception {
        producer.setDisableMessageID(true);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDisableMessageIDFails() throws Exception {
        producer.getDisableMessageID();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDisableMessageTimestampFails() throws Exception {
        producer.setDisableMessageTimestamp(false);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDisableMessageTimestampFails() throws Exception {
        producer.getDisableMessageTimestamp();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDeliveryModeFails() throws Exception {
        producer.setDeliveryMode(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDeliveryModeFails() throws Exception {
        producer.getDeliveryMode();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetPriorityFails() throws Exception {
        producer.setPriority(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetPriorityFails() throws Exception {
        producer.getPriority();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetTimeToLiveFails() throws Exception {
        producer.setTimeToLive(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetTimeToLiveFails() throws Exception {
        producer.getTimeToLive();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDestinationFails() throws Exception {
        producer.getDestination();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendFails() throws Exception {
        producer.send(message);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithDestinationFails() throws Exception {
        producer.send(destination, message);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithModePriorityTTLFails() throws Exception {
        producer.send(message, 1, 3, 111);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithDestinationModePriorityTTLFails() throws Exception {
        producer.send(destination, message, 1, 3, 111);
    }
}

