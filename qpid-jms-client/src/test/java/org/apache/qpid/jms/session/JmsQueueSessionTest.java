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

import static org.junit.Assert.assertNotNull;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsQueueSession;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the contract of JmsQueueSession against JMS Spec requirements.
 */
public class JmsQueueSessionTest extends JmsConnectionTestSupport {

    private QueueSession queueSession;
    private final JmsTopic topic = new JmsTopic();
    private final JmsQueue queue = new JmsQueue();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        queueConnection = createQueueConnectionToMockProvider();
        queueConnection.start();

        queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerToTopic() throws JMSException {
        queueSession.createConsumer(topic);
    }

    @Test(timeout = 30000)
    public void testCreateReceiver() throws JMSException {
        assertNotNull(queueSession.createReceiver(queue));
    }

    @Test(timeout = 30000)
    public void testCreateReceiverWithSelector() throws JMSException {
        assertNotNull(queueSession.createReceiver(queue, "color = red"));
    }

    @Test(timeout = 30000)
    public void testCreateConsumerToQueue() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelector() throws JMSException {
        queueSession.createConsumer(topic, "color = red");
    }

    @Test(timeout = 30000)
    public void testCreateConsumerToQueueWithSelector() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue, "color = red"));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelectorNoLocal() throws JMSException {
        queueSession.createConsumer(topic, "color = red", false);
    }

    @Test(timeout = 30000)
    public void testCreateConsumerToQueueWithSelectorNoLocal() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue, "color = red", false));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSubscriber() throws JMSException {
        ((JmsQueueSession) queueSession).createSubscriber(topic);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSubscriberSelector() throws JMSException {
        ((JmsQueueSession) queueSession).createSubscriber(topic, "color = red", false);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateProducerToTopic() throws JMSException {
        queueSession.createProducer(topic);
    }

    @Test(timeout = 30000)
    public void testCreateProducerToQueue() throws JMSException {
        assertNotNull(queueSession.createProducer(queue));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateTopicPublisher() throws JMSException {
        ((JmsQueueSession) queueSession).createPublisher(topic);
    }

    /**
     * Test that a call to <code>createDurableSubscriber()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableSubscriberOnQueueSession() throws JMSException {
        queueSession.createDurableSubscriber(topic, "subscriptionName");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableSubscriberWithSelectorOnQueueSession() throws JMSException {
        queueSession.createDurableSubscriber(topic, "subscriptionName", "color = red", false);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableConsumerOnQueueSession() throws JMSException {
        queueSession.createDurableConsumer(topic, "subscriptionName");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableConsumerWithSelectorOnQueueSession() throws JMSException {
        queueSession.createDurableConsumer(topic, "subscriptionName", "color = red", false);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSharedConsumerOnQueueSession() throws JMSException {
        queueSession.createSharedConsumer(topic, "subscriptionName");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSharedConsumerWithSelectorOnQueueSession() throws JMSException {
        queueSession.createSharedConsumer(topic, "subscriptionName", "color = red");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSharedDurableConsumerOnQueueSession() throws JMSException {
        queueSession.createSharedDurableConsumer(topic, "subscriptionName");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateSharedDurableConsumerWithSelectorOnQueueSession() throws JMSException {
        queueSession.createSharedConsumer(topic, "subscriptionName", "color = red");
    }

    /**
     * Test that a call to <code>createTemporaryTopic()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateTemporaryTopicOnQueueSession() throws JMSException {
        queueSession.createTemporaryTopic();
    }

    /**
     * Test that a call to <code>createTopic()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateTopicOnQueueSession() throws JMSException {
        queueSession.createTopic("test-topic");
    }

    /**
     * Test that a call to <code>unsubscribe()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testUnsubscribeOnQueueSession() throws JMSException  {
        queueSession.unsubscribe("subscriptionName");
    }
}
