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

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

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
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        producer = createProducer();
    }

    @Test
    @Timeout(30)
    public void testClose() throws Exception {
        producer.close();
    }

    @Test
    @Timeout(30)
    public void testSetDisableMessageIDFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.setDisableMessageID(true);
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageIDFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getDisableMessageID();
        });
    }

    @Test
    @Timeout(30)
    public void testSetDisableMessageTimestampFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.setDisableMessageTimestamp(false);
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageTimestampFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getDisableMessageTimestamp();
        });
    }

    @Test
    @Timeout(30)
    public void testSetDeliveryModeFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.setDeliveryMode(1);
        });
    }

    @Test
    @Timeout(30)
    public void testGetDeliveryModeFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getDeliveryMode();
        });
    }

    @Test
    @Timeout(30)
    public void testSetPriorityFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.setPriority(1);
        });
    }

    @Test
    @Timeout(30)
    public void testGetPriorityFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getPriority();
        });
    }

    @Test
    @Timeout(30)
    public void testSetTimeToLiveFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.setTimeToLive(1);
        });
    }

    @Test
    @Timeout(30)
    public void testGetTimeToLiveFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getTimeToLive();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDestinationFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.getDestination();
        });
    }

    @Test
    @Timeout(30)
    public void testSendFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.send(message);
        });
    }

    @Test
    @Timeout(30)
    public void testSendWithDestinationFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.send(destination, message);
        });
    }

    @Test
    @Timeout(30)
    public void testSendWithModePriorityTTLFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.send(message, 1, 3, 111);
        });
    }

    @Test
    @Timeout(30)
    public void testSendWithDestinationModePriorityTTLFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            producer.send(destination, message, 1, 3, 111);
        });
    }
}

