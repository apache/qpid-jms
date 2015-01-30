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

import java.net.URI;

import org.apache.qpid.jms.message.facade.defaults.JmsDefaultMessageFactory;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for tests that require a JmsConnection that is created using a
 * Mocked Provider object to simulate a connection.
 */
public class JmsConnectionTestSupport extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionTestSupport.class);

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    protected JmsConnection connection;
    protected ProviderListener providerListener;

    private void createMockProvider() throws Exception {
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof JmsConnectionInfo) {
                    providerListener.onConnectionEstablished(new URI("vm://localhost"));
                }
                LOG.trace("Handling provider create resource: {}", args[0]);
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).create(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof JmsResource) {
                    LOG.trace("Handling provider start resource: {}", args[0]);
                    ProviderFuture request = (ProviderFuture) args[1];
                    request.onSuccess();
                }
                return null;
            }
        }).when(provider).start(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.trace("Handling provider recover: {}", args[0]);
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).recover(Mockito.any(JmsSessionId.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.trace("Handling provider session acknowledge: {}", args[0]);
                ProviderFuture request = (ProviderFuture) args[1];
                request.onSuccess();
                return null;
            }
        }).when(provider).acknowledge(Mockito.any(JmsSessionId.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof JmsResource) {
                    LOG.trace("Handling provider destroy resource: {}", args[0]);
                    ProviderFuture request = (ProviderFuture) args[1];
                    request.onSuccess();
                }
                return null;
            }
        }).when(provider).destroy(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof ProviderListener) {
                    providerListener = (ProviderListener) args[0];
                }
                return null;
            }
        }).when(provider).setProviderListener(Mockito.any(ProviderListener.class));

        Mockito.when(provider.getProviderListener()).thenReturn(providerListener);
        Mockito.when(provider.getMessageFactory()).thenReturn(new JmsDefaultMessageFactory());
    }

    protected JmsConnection createConnectionToMockProvider() throws Exception {
        return new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
    }

    protected JmsQueueConnection createQueueConnectionToMockProvider() throws Exception {
        return new JmsQueueConnection("ID:TEST:1", provider, clientIdGenerator);
    }

    protected JmsTopicConnection createTopicConnectionToMockProvider() throws Exception {
        return new JmsTopicConnection("ID:TEST:1", provider, clientIdGenerator);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createMockProvider();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (connection != null) {
            connection.close();
        }
    }
}
