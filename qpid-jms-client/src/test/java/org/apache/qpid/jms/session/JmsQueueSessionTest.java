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
package org.apache.qpid.jms.session;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the contract of JmsQueueSession against JMS Spec requirements.
 */
public class JmsQueueSessionTest extends JmsConnectionTestSupport {

    private QueueSession queueSession;
    private final JmsTopic topic = new JmsTopic();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        queueConnection = createQueueConnectionToMockProvider();
        queueConnection.start();

        queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerTopicSession() throws JMSException {
        queueSession.createConsumer(topic);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelectorTopicSession() throws JMSException {
        queueSession.createConsumer(topic, "color = red");
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConsumerWithSelectorNoLocalTopicSession() throws JMSException {
        queueSession.createConsumer(topic, "color = red", false);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateProducerTopicSession() throws JMSException {
        queueSession.createProducer(topic);
    }

    /**
     * Test that a call to <code>createDurableSubscriber()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableSubscriberOnQueueSession() throws JMSException {
        queueSession.createDurableSubscriber(topic, "subscriptionName");
    }

    /**
     * Test that a call to <code>createTemporaryTopic()</code> method
     * on a <code>QueueSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
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
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testUnsubscribeOnQueueSession() throws JMSException  {
        queueSession.unsubscribe("subscriptionName");
    }
}
