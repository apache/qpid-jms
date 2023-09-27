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
package org.apache.qpid.jms.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test basic Session functionality.
 */
public class JmsSessionTest extends AmqpTestSupport {

    @Test
    @Timeout(60)
    public void testCreateSession() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        session.close();
    }

    @Test
    @Timeout(30)
    public void testSessionCreateProducer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        producer.close();
        session.close();
    }

    @Test
    @Timeout(30)
    public void testSessionCreateConsumer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.close();
        session.close();
    }

    @Test
    @Timeout(30)
    public void testCreateTemporaryQueue() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof TemporaryQueue);

        final BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getTemporaryQueues().length);
    }

    @Test
    @Timeout(30)
    public void testDeleteTemporaryQueue() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof TemporaryQueue);

        final BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getTemporaryQueues().length);

        TemporaryQueue tempQueue = (TemporaryQueue) queue;
        tempQueue.delete();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return broker.getTemporaryQueues().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Temp Queue should be deleted.");
    }

    @Test
    @Timeout(30)
    public void testCreateTemporaryTopic() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        assertNotNull(topic);
        assertTrue(topic instanceof TemporaryTopic);

        final BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getTemporaryTopics().length);
    }

    @Test
    @Timeout(30)
    public void testDeleteTemporaryTopic() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();
        assertNotNull(topic);
        assertTrue(topic instanceof TemporaryTopic);

        final BrokerViewMBean broker = getProxyToBroker();
        assertEquals(1, broker.getTemporaryTopics().length);

        TemporaryTopic tempTopic = (TemporaryTopic) topic;
        tempTopic.delete();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return broker.getTemporaryTopics().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)), "Temp Topic should be deleted.");
    }
}
