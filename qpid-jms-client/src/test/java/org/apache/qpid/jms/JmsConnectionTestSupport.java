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

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.provider.mock.MockProviderListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;

/**
 * Base for tests that require a JmsConnection that is created using a
 * Mocked Provider object to simulate a connection.
 */
public class JmsConnectionTestSupport extends QpidJmsTestCase {

    private final IdGenerator clientIdGenerator = new IdGenerator();

    protected final MockProviderFactory mockFactory = new MockProviderFactory();

    protected JmsConnection connection;
    protected JmsTopicConnection topicConnection;
    protected JmsQueueConnection queueConnection;
    protected ProviderListener providerListener;

    private Provider createMockProvider() throws Exception {
        return mockFactory.createProvider(new URI("mock://localhost")).setEventListener(new MockProviderListener() {

            @Override
            public void whenProviderListenerSet(Provider provider, ProviderListener listener) {
                providerListener = listener;
            }
        });
    }

    protected JmsConnection createConnectionToMockProvider() throws Exception {
        return new JmsConnection("ID:TEST:1", createMockProvider(), clientIdGenerator);
    }

    protected JmsQueueConnection createQueueConnectionToMockProvider() throws Exception {
        return new JmsQueueConnection("ID:TEST:1", createMockProvider(), clientIdGenerator);
    }

    protected JmsTopicConnection createTopicConnectionToMockProvider() throws Exception {
        return new JmsTopicConnection("ID:TEST:1", createMockProvider(), clientIdGenerator);
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
