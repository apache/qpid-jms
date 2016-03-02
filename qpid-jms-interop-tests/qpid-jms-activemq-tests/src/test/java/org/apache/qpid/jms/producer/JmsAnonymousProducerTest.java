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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.util.StopWatch;
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
    public void testAnonymousSendToTopic() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(name.getMethodName());
        assertNotNull(session);
        MessageProducer producer = session.createProducer(null);
        assertNotNull(producer);
        MessageConsumer consumer = session.createConsumer(topic);
        assertNotNull(consumer);

        Message message = session.createMessage();
        producer.send(topic, message);

        TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(1, proxy.getEnqueueCount());
    }

    @Test(timeout = 60000)
    public void testAnonymousSendToThreeDestinations() throws Exception {
        StopWatch timer = new StopWatch();
        doTestAnonymousProducerSendToMultipleDests(3, 1);
        LOG.info("Time to send to three destinations: {} ms", timer.taken());
    }

    @Test(timeout = 60000)
    public void testAnonymousSendToTenDestinations() throws Exception {
        StopWatch timer = new StopWatch();
        doTestAnonymousProducerSendToMultipleDests(10, 1);
        LOG.info("Time to send to ten destinations: {} ms", timer.taken());
    }

    @Test(timeout = 60000)
    public void testAnonymousSendToOneHundredDestinations() throws Exception {
        StopWatch timer = new StopWatch();
        doTestAnonymousProducerSendToMultipleDests(100, 1);
        LOG.info("Time to send to one hundred destinations: {} ms", timer.taken());
    }

    @Test(timeout = 60000)
    public void testAnonymousSendToTenDestinationsTenTimes() throws Exception {
        StopWatch timer = new StopWatch();
        doTestAnonymousProducerSendToMultipleDests(10, 10);
        LOG.info("Time to send to ten destinations ten times: {} ms", timer.taken());
    }

    public void doTestAnonymousProducerSendToMultipleDests(int numDestinations, int numIterations) throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        List<Queue> queues = new ArrayList<Queue>(numDestinations);
        for (int i = 0; i < numDestinations; ++i) {
            queues.add(session.createQueue(name.getMethodName() + i));
        }

        assertNotNull(session);
        MessageProducer producer = session.createProducer(null);

        for (int iteration = 1; iteration <= numIterations; ++iteration) {
            Message message = session.createMessage();
            for (int i = 0; i < numDestinations; ++i) {
                producer.send(queues.get(i), message);
            }

            for (int i = 0; i < numDestinations; ++i) {
                QueueViewMBean proxy = getProxyToQueue(queues.get(i).getQueueName());
                assertEquals(iteration, proxy.getQueueSize());
            }
        }
    }

    // TODO - Should only get JMSSecurityException on ActiveMQ 5.12.2+
    @Test(timeout = 30000)
    public void testAnonymousProducerNotAuthorized() throws Exception {
        connection = createAmqpConnection("guest", "password");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("USERS.txQueue");
        MessageProducer producer = session.createProducer(null);

        try {
            producer.send(queue, session.createTextMessage());
            fail("Should not be able to produce here.");
        } catch (JMSSecurityException jmsSE) {
            LOG.info("Caught expected exception");
        } catch (JMSException jms) {
            LOG.info("Caught expected exception");
        }
    }

}
