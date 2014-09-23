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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Durable Topic Subscriber functionality.
 */
public class JmsDurableSubscriberTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsMessageConsumerTest.class);

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Test(timeout = 60000)
    public void testCreateDuableSubscriber() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(name.getMethodName());
        session.createDurableSubscriber(topic, name.getMethodName() + "-subscriber");

        TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
    }

    @Test(timeout = 60000)
    public void testDurableGoesOfflineAndReturns() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(name.getMethodName());
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, name.getMethodName() + "-subscriber");

        TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber = session.createDurableSubscriber(topic, name.getMethodName() + "-subscriber");

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);
    }

    @Test(timeout = 60000)
    public void testOfflineSubscriberGetsItsMessages() throws Exception {
        connection = createAmqpConnection();
        connection.setClientID("DURABLE-AMQP");
        connection.start();

        final int MSG_COUNT = 5;

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(name.getMethodName());
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, name.getMethodName() + "-subscriber");

        TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        subscriber.close();

        assertEquals(0, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i < MSG_COUNT; i++) {
            producer.send(session.createTextMessage("Message: " + i));
        }
        producer.close();

        LOG.info("Bringing offline subscription back online.");
        subscriber = session.createDurableSubscriber(topic, name.getMethodName() + "-subscriber");

        assertEquals(1, brokerService.getAdminView().getDurableTopicSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getInactiveDurableTopicSubscribers().length);

        final CountDownLatch messages = new CountDownLatch(MSG_COUNT);
        subscriber.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.info("Consumer got a message: {}", message);
                messages.countDown();
            }
        });

        assertTrue("Only recieved messages: " + messages.getCount(), messages.await(30, TimeUnit.SECONDS));
    }
}
