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
import javax.jms.Session;
import javax.jms.TopicSession;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.JmsTopicSession;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the contract of JmsTopicSession against JMS Spec requirements.
 */
public class JmsTopicSessionTest extends JmsConnectionTestSupport {

    private TopicSession topicSession;
    private final JmsQueue queue = new JmsQueue();
    private final JmsTopic topic = new JmsTopic();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        topicConnection = createTopicConnectionToMockProvider();
        topicConnection.start();

        topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Test that a call to <code>createBrowser()</code> method
     * on a <code>TopicSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateBrowserOnTopicSession() throws JMSException {
        topicSession.createBrowser(queue);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateBrowserWithSelectorOnTopicSession() throws JMSException {
        topicSession.createBrowser(queue, "color = red");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerToQueue() throws JMSException {
        topicSession.createConsumer(queue);
    }

    @Test(timeout = 30000)
    public void testCreateConsumerToTopic() throws JMSException {
       assertNotNull(topicSession.createConsumer(topic));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelectorToQueue() throws JMSException {
        topicSession.createConsumer(queue, "color = red");
    }

    @Test(timeout = 30000)
    public void testCreateConsumerWithSelectorToTopic() throws JMSException {
        assertNotNull(topicSession.createConsumer(topic, "color = red"));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelectorNoLocalToQueue() throws JMSException {
        topicSession.createConsumer(queue, "color = red", false);
    }

    @Test(timeout = 30000)
    public void testCreateConsumerWithSelectorNoLocalToTopic() throws JMSException {
        assertNotNull(topicSession.createConsumer(topic, "color = red", false));
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateProducerToQueue() throws JMSException {
        topicSession.createProducer(queue);
    }

    @Test(timeout = 30000)
    public void testCreateProducerToTopic() throws JMSException {
        assertNotNull(topicSession.createProducer(topic));
    }

    /**
     * Test that a call to <code>createQueue()</code> method
     * on a <code>TopicSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateQueueOnTopicSession() throws JMSException {
        topicSession.createQueue("test-queue");
    }

    /**
     * Test that a call to <code>createTemporaryQueue()</code> method
     * on a <code>TopicSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateTemporaryQueueOnTopicSession() throws JMSException {
        topicSession.createTemporaryQueue();
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateQueueReceiver() throws JMSException {
        ((JmsTopicSession) topicSession).createReceiver(queue);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateQueueReceiverWithSelector() throws JMSException {
        ((JmsTopicSession) topicSession).createReceiver(queue, "color = read");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateQueueSender() throws JMSException {
        ((JmsTopicSession) topicSession).createSender(queue);
    }
}
