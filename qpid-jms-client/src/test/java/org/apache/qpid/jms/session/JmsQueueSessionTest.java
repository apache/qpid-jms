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

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsQueueSession;
import org.apache.qpid.jms.JmsTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test the contract of JmsQueueSession against JMS Spec requirements.
 */
public class JmsQueueSessionTest extends JmsConnectionTestSupport {

    private QueueSession queueSession;
    private final JmsTopic topic = new JmsTopic();
    private final JmsQueue queue = new JmsQueue();

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        queueConnection = createQueueConnectionToMockProvider();
        queueConnection.start();

        queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToTopic() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createConsumer(topic);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateReceiver() throws JMSException {
        assertNotNull(queueSession.createReceiver(queue));
    }

    @Test
    @Timeout(30)
    public void testCreateReceiverWithSelector() throws JMSException {
        assertNotNull(queueSession.createReceiver(queue, "color = red"));
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToQueue() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue));
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelector() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createConsumer(topic, "color = red");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToQueueWithSelector() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue, "color = red"));
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerWithSelectorNoLocal() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createConsumer(topic, "color = red", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateConsumerToQueueWithSelectorNoLocal() throws JMSException {
        assertNotNull(queueSession.createConsumer(queue, "color = red", false));
    }

    @Test
    @Timeout(30)
    public void testCreateSubscriber() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsQueueSession) queueSession).createSubscriber(topic);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSubscriberSelector() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsQueueSession) queueSession).createSubscriber(topic, "color = red", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateProducerToTopic() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createProducer(topic);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateProducerToQueue() throws JMSException {
        assertNotNull(queueSession.createProducer(queue));
    }

    @Test
    @Timeout(30)
    public void testCreateTopicPublisher() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            ((JmsQueueSession) queueSession).createPublisher(topic);
        });
    }

    /**
     * Test that a call to <code>createDurableSubscriber()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>jakarta.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test
    @Timeout(30)
    public void testCreateDurableSubscriberOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createDurableSubscriber(topic, "subscriptionName");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableSubscriberWithSelectorOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createDurableSubscriber(topic, "subscriptionName", "color = red", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableConsumerOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createDurableConsumer(topic, "subscriptionName");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableConsumerWithSelectorOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createDurableConsumer(topic, "subscriptionName", "color = red", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSharedConsumerOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createSharedConsumer(topic, "subscriptionName");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSharedConsumerWithSelectorOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createSharedConsumer(topic, "subscriptionName", "color = red");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSharedDurableConsumerOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createSharedDurableConsumer(topic, "subscriptionName");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSharedDurableConsumerWithSelectorOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createSharedConsumer(topic, "subscriptionName", "color = red");
        });
    }

    /**
     * Test that a call to <code>createTemporaryTopic()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>jakarta.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test
    @Timeout(30)
    public void testCreateTemporaryTopicOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createTemporaryTopic();
        });
    }

    /**
     * Test that a call to <code>createTopic()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>jakarta.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test
    @Timeout(30)
    public void testCreateTopicOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.createTopic("test-topic");
        });
    }

    /**
     * Test that a call to <code>unsubscribe()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>jakarta.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     *
     * @throws JMSException if an error occurs during the test.
     */
    @Test
    @Timeout(30)
    public void testUnsubscribeOnQueueSession() throws JMSException {
        assertThrows(IllegalStateException.class, () -> {
            queueSession.unsubscribe("subscriptionName");
        });
    }
}
