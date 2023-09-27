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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test basic MessageProducer functionality.
 */
public class JmsProduceMessageTypesTest extends AmqpTestSupport {

    @Test
    @Timeout(60)
    public void testSendJMSMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        Message message = session.createMessage();
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());
    }

    @Test
    @Timeout(60)
    public void testSendJMSBytesMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        String payload = "TEST";

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();
        message.writeUTF(payload);
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof BytesMessage);
        BytesMessage bytes = (BytesMessage) received;
        assertEquals(payload, bytes.readUTF());
    }

    @Test
    @Timeout(60)
    public void testSendJMSMapMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        MapMessage message = session.createMapMessage();
        message.setBoolean("Boolean", false);
        message.setString("STRING", "TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof MapMessage);
        MapMessage map = (MapMessage) received;
        assertEquals("TEST", map.getString("STRING"));
        assertEquals(false, map.getBooleanProperty("Boolean"));
    }

    @Test
    @Timeout(60)
    public void testSendJMSStreamMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        String payload = "TEST";

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        StreamMessage message = session.createStreamMessage();
        message.writeString(payload);
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof StreamMessage);
        StreamMessage stream = (StreamMessage) received;
        assertEquals(payload, stream.readString());
    }

    @Test
    @Timeout(60)
    public void testSendJMSTextMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        String payload = "TEST";

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof TextMessage);
        TextMessage text = (TextMessage) received;
        assertEquals(payload, text.getText());
    }

    @Test
    @Timeout(60)
    public void testSendJMSObjectMessage() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(testMethodName);
        MessageProducer producer = session.createProducer(queue);
        ObjectMessage message = session.createObjectMessage("TEST");
        producer.send(message);
        QueueViewMBean proxy = getProxyToQueue(testMethodName);
        assertEquals(1, proxy.getQueueSize());
    }
}
