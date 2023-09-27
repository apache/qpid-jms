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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TopicSession;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.JmsTopicSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test the contract of JmsTopicSession against JMS Spec requirements.
 */
public class JmsTopicSessionTest extends JmsConnectionTestSupport {

    private TopicSession topicSession;
    private final JmsQueue queue = new JmsQueue();
    private final JmsTopic topic = new JmsTopic();

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

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
    @Test
    @Timeout(30)
    public void testCreateBrowserOnTopicSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createBrowser(queue);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateBrowserWithSelectorOnTopicSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createBrowser(queue, "color = red");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToQueue() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createConsumer(queue);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToTopic() throws JMSException {
       assertNotNull(topicSession.createConsumer(topic));
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelectorToQueue() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createConsumer(queue, "color = red");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelectorToTopic() throws JMSException {
        assertNotNull(topicSession.createConsumer(topic, "color = red"));
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelectorNoLocalToQueue() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createConsumer(queue, "color = red", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelectorNoLocalToTopic() throws JMSException {
        assertNotNull(topicSession.createConsumer(topic, "color = red", false));
    }

    @Test
    @Timeout(30)
    public void testCreateProducerToQueue() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createProducer(queue);
        });
    }

    @Test
    @Timeout(30)
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
    @Test
    @Timeout(30)
    public void testCreateQueueOnTopicSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createQueue("test-queue");
        });
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
    @Test
    @Timeout(30)
    public void testCreateTemporaryQueueOnTopicSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            topicSession.createTemporaryQueue();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueReceiver() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsTopicSession) topicSession).createReceiver(queue);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueReceiverWithSelector() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsTopicSession) topicSession).createReceiver(queue, "color = read");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateQueueSender() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsTopicSession) topicSession).createSender(queue);
        });
    }
}
