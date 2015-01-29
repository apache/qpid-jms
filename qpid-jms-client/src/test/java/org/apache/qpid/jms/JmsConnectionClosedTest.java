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

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
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
 * Test Connection methods contracts when state is closed.
 */
public class JmsConnectionClosedTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionClosedTest.class);

    protected Destination destination;

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    protected JmsConnection connection;
    protected ProviderListener providerListener;

    protected JmsConnection createConnectionToMockProvider() throws Exception {

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof JmsResource) {
                    LOG.debug("Handling provider create resource: {}", args[0]);
                    ProviderFuture request = (ProviderFuture) args[1];
                    request.onSuccess();
                }
                return null;
            }
        }).when(provider).create(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                if (args[0] instanceof JmsResource) {
                    LOG.debug("Handling provider destroy resource: {}", args[0]);
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

        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        return connection;
    }

    protected JmsConnection createConnection() throws Exception {
        connection = createConnectionToMockProvider();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createTopic("test");
        connection.close();
        return connection;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (connection != null) {
            connection.close();
        }
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetClientIdFails() throws Exception {
        connection.getClientID();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetClientIdFails() throws Exception {
        connection.setClientID("test");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMetaData() throws Exception {
        connection.getMetaData();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetExceptionListener() throws Exception {
        connection.getExceptionListener();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetExceptionListener() throws Exception {
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
            }
        });
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testStartFails() throws Exception {
        connection.start();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testStopFails() throws Exception {
        connection.stop();
    }

    @Test(timeout=30000)
    public void testClose() throws Exception {
        connection.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConnectionConsumerFails() throws Exception {
        connection.createConnectionConsumer(destination, "", null, 1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableConnectionConsumerFails() throws Exception {
        connection.createDurableConnectionConsumer((Topic) destination, "id", "", null, 1);
    }
}
