/**
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

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test the contract of MessageProducer that has been closed.
 */
public class JmsMessageProducerClosedTest extends AmqpTestSupport {

    protected MessageProducer producer;
    protected Message message;
    protected Destination destination;

    protected MessageProducer createProducer() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        message = session.createMessage();
        destination = session.createTopic("test");
        MessageProducer producer = session.createProducer(destination);
        producer.close();
        return producer;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        producer = createProducer();
    }

    @Test(timeout=30000)
    public void testClose() throws JMSException {
        producer.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDisableMessageIDFails() throws JMSException {
        producer.setDisableMessageID(true);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDisableMessageIDFails() throws JMSException {
        producer.getDisableMessageID();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDisableMessageTimestampFails() throws JMSException {
        producer.setDisableMessageTimestamp(false);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDisableMessageTimestampFails() throws JMSException {
        producer.getDisableMessageTimestamp();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetDeliveryModeFails() throws JMSException {
        producer.setDeliveryMode(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDeliveryModeFails() throws JMSException {
        producer.getDeliveryMode();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetPriorityFails() throws JMSException {
        producer.setPriority(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetPriorityFails() throws JMSException {
        producer.getPriority();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetTimeToLiveFails() throws JMSException {
        producer.setTimeToLive(1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetTimeToLiveFails() throws JMSException {
        producer.getTimeToLive();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetDestinationFails() throws JMSException {
        producer.getDestination();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendFails() throws JMSException {
        producer.send(message);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithDestinationFails() throws JMSException {
        producer.send(destination, message);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithModePriorityTTLFails() throws JMSException {
        producer.send(message, 1, 3, 111);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSendWithDestinationModePriorityTTLFails() throws JMSException {
        producer.send(destination, message, 1, 3, 111);
    }
}

