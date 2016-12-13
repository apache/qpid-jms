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
package org.apache.qpid.jms;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSContext;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.provider.mock.MockProviderListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;
import org.junit.Before;

/**
 * Base for tests that require a JmsConnection that is created using a
 * Mocked Provider object to simulate a connection.
 */
public class JmsConnectionTestSupport extends QpidJmsTestCase {

    private static final AtomicLong CONN_ID_SUFFIX = new AtomicLong();
    private final IdGenerator clientIdGenerator = new IdGenerator();

    protected final MockProviderFactory mockFactory = new MockProviderFactory();

    protected JmsConnection connection;
    protected JmsTopicConnection topicConnection;
    protected JmsQueueConnection queueConnection;
    protected ProviderListener providerListener;
    protected JmsConnectionInfo connectionInfo;

    private Provider createMockProvider() throws Exception {
        return mockFactory.createProvider(new URI("mock://localhost")).setEventListener(new MockProviderListener() {

            @Override
            public void whenProviderListenerSet(Provider provider, ProviderListener listener) {
                providerListener = listener;
            }
        });
    }

    protected JmsContext createJMSContextToMockProvider() throws Exception {
        JmsConnection connection = new JmsConnection(connectionInfo, createMockProvider());
        JmsContext context = new JmsContext(connection, JMSContext.AUTO_ACKNOWLEDGE);

        return context;
    }

    protected JmsConnection createConnectionToMockProvider() throws Exception {
        return new JmsConnection(connectionInfo, createMockProvider());
    }

    protected JmsQueueConnection createQueueConnectionToMockProvider() throws Exception {
        return new JmsQueueConnection(connectionInfo, createMockProvider());
    }

    protected JmsTopicConnection createTopicConnectionToMockProvider() throws Exception {
        return new JmsTopicConnection(connectionInfo, createMockProvider());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connectionInfo = new JmsConnectionInfo(new JmsConnectionId("ID:JCTS(" + CONN_ID_SUFFIX.incrementAndGet() + "):" + getClass().getName()));
        connectionInfo.setClientId(clientIdGenerator.generateId(), false);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (connection != null) {
            connection.close();
        }
        if (topicConnection != null) {
            topicConnection.close();
        }
        if (queueConnection != null) {
            queueConnection.close();
        }
    }
}
