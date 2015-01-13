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
import javax.jms.Session;
import javax.jms.TopicSession;

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
 * Test various contract aspects of the TopicConnection implementation
 */
public class JmsTopicConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicConnectionTest.class);

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    private JmsTopicConnection topicConnection;
    private TopicSession topicSession;
    private final JmsQueue queue = new JmsQueue();

    @Before
    public void setUp() throws Exception {

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.trace("Handling provider create call for resource: {}", args[0]);
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).create(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.trace("Handling provider destroy call");
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).destroy(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        topicConnection = new JmsTopicConnection("ID:TEST:1", provider, clientIdGenerator);
        topicConnection.start();

        topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        topicConnection.close();
    }

    /**
     * Test that a call to <code>createBrowser()</code> method
     * on a <code>TopicSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateBrowserOnTopicSession() throws JMSException {
        topicSession.createBrowser(queue);
    }

    /**
     * Test that a call to <code>createQueue()</code> method
     * on a <code>TopicSession</code> throws a
     * <code>javax.jms.IllegalStateException</code>.
     * (see JMS 1.1 specs, table 4-1).
     *
     * @since JMS 1.1
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
     */
    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateTemporaryQueueOnTopicSession() throws JMSException {
        topicSession.createTemporaryQueue();
    }
}