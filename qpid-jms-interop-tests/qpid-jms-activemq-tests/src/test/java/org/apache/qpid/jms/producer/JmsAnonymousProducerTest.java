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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test JMS Anonymous Producer functionality.
 */
public class JmsAnonymousProducerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateProducer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        session.createProducer(null);

        assertTrue(brokerService.getAdminView().getTotalProducerCount() == 0);
    }

    @Test(timeout = 60000)
    public void testAnonymousSend() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        assertNotNull(session);
        MessageProducer producer = session.createProducer(null);

        Message message = session.createMessage();
        producer.send(queue, message);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testAnonymousSendToMultipleDestinations() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue1 = session.createQueue(name.getMethodName() + 1);
        Queue queue2 = session.createQueue(name.getMethodName() + 2);
        Queue queue3 = session.createQueue(name.getMethodName() + 3);
        assertNotNull(session);
        MessageProducer producer = session.createProducer(null);

        Message message = session.createMessage();
        producer.send(queue1, message);
        producer.send(queue2, message);
        producer.send(queue3, message);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName() + 1);
        assertEquals(1, proxy.getQueueSize());
        proxy = getProxyToQueue(name.getMethodName() + 2);
        assertEquals(1, proxy.getQueueSize());
        proxy = getProxyToQueue(name.getMethodName() + 3);
        assertEquals(1, proxy.getQueueSize());
    }
}
