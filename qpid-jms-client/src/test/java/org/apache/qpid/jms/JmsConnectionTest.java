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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionTest.class);

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    @Test(expected=JMSException.class)
    public void testJmsConnectionThrowsJMSExceptionProviderStartFails() throws JMSException, IllegalStateException, IOException {
        Mockito.doThrow(IOException.class).when(provider).start();
        new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
    }

    @Test
    public void testStateAfterCreate() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);

        assertFalse(connection.isStarted());
        assertFalse(connection.isClosed());
        assertFalse(connection.isConnected());
    }

    @Test
    public void testGetProvider() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        assertSame(provider, connection.getProvider());
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testSetClientIdWithNull() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.setClientID(null);
    }

    @Test
    public void testGetConnectionId() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        assertEquals("ID:TEST:1", connection.getConnectionId().toString());
    }

    @Test
    public void testAddConnectionListener() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        JmsConnectionListener listener = new JmsConnectionListener() {

            @Override
            public void onInboundMessage(JmsInboundMessageDispatch envelope) {
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }
        };

        assertFalse(connection.removeConnectionListener(listener));
        connection.addConnectionListener(listener);
        assertTrue(connection.removeConnectionListener(listener));
    }

    @Test
    public void testConnectionStart() throws JMSException, IOException {

        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.debug("Handling provider create call");
                if (args[0] instanceof JmsConnectionInfo) {
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
                LOG.debug("Handling provider destroy call");
                if (args[0] instanceof JmsConnectionInfo) {
                    ProviderFuture request = (ProviderFuture) args[1];
                    request.onSuccess();
                }
                return null;
            }
        }).when(provider).destroy(Mockito.any(JmsResource.class), Mockito.any(ProviderFuture.class));

        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.close();
    }

    //---------- Test methods fail after connection closed -------------------//

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testSetClientIdAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.setClientID("test-Id");
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testStartCalledAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.start();
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testStopCalledAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.stop();
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testSetExceptionListenerAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.setExceptionListener(null);
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testFetExceptionListenerAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.getExceptionListener();
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testCreateConnectionConsumerForTopicAfterClose() throws JMSException {
        JmsDestination destination = new JmsTopic("test");
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.createConnectionConsumer(destination, null, null, 0);
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testCreateConnectionConsumerForQueueAfterClose() throws JMSException {
        JmsDestination destination = new JmsQueue("test");
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.createConnectionConsumer(destination, null, null, 0);
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testCreateTopicSessionAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testCreateQueueSessionAfterClose() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.close();
        connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}
