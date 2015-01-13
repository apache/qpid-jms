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
package org.apache.qpid.jms;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test various contract aspects of the QueueConnection implementation
 */
public class JmsQueueConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsQueueConnectionTest.class);

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    private JmsQueueConnection queueConnection;
    private QueueSession queueSession;
    private final JmsTopic topic = new JmsTopic();

    @Before
    public void setUp() throws Exception {

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.debug("Handling provider create call for resource: {}", args[0]);
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).create(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.debug("Handling provider destroy call");
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).destroy(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        queueConnection = new JmsQueueConnection("ID:TEST:1", provider, clientIdGenerator);
        queueConnection.start();

        queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        queueConnection.close();
    }

    /**
     * Test that a call to <code>createDurableConnectionConsumer()</code> method
     * on a <code>QueueConnection</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateDurableConnectionConsumerOnQueueConnection() throws JMSException{
        queueConnection.createDurableConnectionConsumer(topic, "subscriptionName", "", (ServerSessionPool)null, 1);
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