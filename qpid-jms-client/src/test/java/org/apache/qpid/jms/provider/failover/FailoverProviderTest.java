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
package org.apache.qpid.jms.provider.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.test.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behavior of the FailoverProvider
 */
public class FailoverProviderTest extends FailoverProviderTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverProviderTest.class);

    private List<URI> uris;
    private FailoverProvider provider;
    private JmsConnectionInfo connection;

    @Override
    @Before
    public void setUp() throws Exception {
        uris = new ArrayList<URI>();

        uris.add(new URI("mock://192.168.2.1:5672"));
        uris.add(new URI("mock://192.168.2.2:5672"));
        uris.add(new URI("mock://192.168.2.3:5672"));
        uris.add(new URI("mock://192.168.2.4:5672"));

        connection = createConnectionInfo();

        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
        super.tearDown();
    }

    @Test(timeout = 30000)
    public void testCreateProviderOnlyUris() {
        provider = new FailoverProvider(uris);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertTrue(provider.getNestedOptions().isEmpty());
    }

    @Test(timeout = 30000)
    public void testCreateProviderOnlyNestedOptions() {
        Map<String, String> options = new HashMap<String, String>();
        options.put("transport.tcpNoDelay", "true");

        provider = new FailoverProvider(options);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertFalse(provider.getNestedOptions().isEmpty());
        assertTrue(provider.getNestedOptions().containsKey("transport.tcpNoDelay"));
    }

    @Test(timeout = 30000)
    public void testCreateProviderWithNestedOptions() {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        assertNotNull(provider.getNestedOptions());
        assertTrue(provider.getNestedOptions().isEmpty());
    }

    @Test(timeout = 30000)
    public void testProviderListener() {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertNull(provider.getProviderListener());
        provider.setProviderListener(new DefaultProviderListener());
        assertNotNull(provider.getProviderListener());
    }

    @Test(timeout = 30000)
    public void testGetRemoteURI() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());

        assertNull(provider.getRemoteURI());
        provider.connect(connection);
        assertTrue("Should have a remote URI after connect", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return provider.getRemoteURI() != null;
            }
        }, TimeUnit.SECONDS.toMillis(20), 10));
    }

    @Test(timeout = 30000)
    public void testToString() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());

        assertNotNull(provider.toString());
        provider.connect(connection);
        assertTrue("Should have a mock scheme after connect", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("FailoverProvider: toString = {}", provider.toString());
                return provider.toString().contains("mock://");
            }
        }, TimeUnit.SECONDS.toMillis(20), 10));
    }

    @Test(timeout = 30000)
    public void testConnectToMock() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());

        final CountDownLatch connected = new CountDownLatch(1);

        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
                connected.countDown();
            }
        });

        provider.connect(connection);

        ProviderFuture request = new ProviderFuture();
        provider.create(createConnectionInfo(), request);

        request.sync(10, TimeUnit.SECONDS);

        assertTrue(request.isComplete());

        provider.close();

        assertEquals(1, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(1, mockPeer.getContextStats().getConnectionAttempts());
    }

    @Test(timeout = 30000)
    public void testCannotStartWithoutListener() throws Exception {
        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
        provider.connect(connection);

        try {
            provider.start();
            fail("Should not allow a start if no listener added yet.");
        } catch (IllegalStateException ex) {
        }

        provider.close();
    }

    @Test(timeout = 30000)
    public void testStartupMaxReconnectAttempts() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnConnect=true)" +
            "?failover.startupMaxReconnectAttempts=5" +
            "&failover.useReconnectBackOff=false");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test(timeout = 30000)
    public void testMaxReconnectAttempts() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnConnect=true)" +
            "?failover.maxReconnectAttempts=5" +
            "&failover.useReconnectBackOff=false");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test(timeout = 30000)
    public void testMaxReconnectAttemptsWithBackOff() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnConnect=true)" +
            "?failover.maxReconnectAttempts=5" +
            "&failover.maxReconnectDelay=60" +
            "&failover.reconnectDelay=10" +
            "&failover.useReconnectBackOff=true");

        Connection connection = null;
        try {
            connection = factory.createConnection();
            connection.start();
            fail("Should have stopped after five retries.");
        } catch (JMSException ex) {
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        assertEquals(5, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(5, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(5, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test(timeout = 30000)
    public void testFailureOnCloseIsSwallowed() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost?mock.failOnClose=true)");

        Connection connection = factory.createConnection();
        connection.start();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getProvidersCreated());
        assertEquals(1, mockPeer.getContextStats().getConnectionAttempts());
        assertEquals(1, mockPeer.getContextStats().getCloseAttempts());
    }

    @Test(timeout = 30000)
    public void testSessionLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsSessionInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsSessionInfo.class));
    }

    @Test(timeout = 30000)
    public void testConsumerLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(_testName.getMethodName());
        session.createConsumer(destination).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsConsumerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getStartResourceCalls(JmsConsumerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsConsumerInfo.class));
    }

    @Test(timeout = 30000)
    public void testProducerLifeCyclePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(_testName.getMethodName());
        session.createProducer(destination).close();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getCreateResourceCalls(JmsProducerInfo.class));
        assertEquals(1, mockPeer.getContextStats().getDestroyResourceCalls(JmsProducerInfo.class));
    }

    @Test(timeout = 30000)
    public void testSessionRecoverPassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        session.recover();
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getRecoverCalls());
    }

    @Test(timeout = 30000)
    public void testSessionUnsubscribePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        session.unsubscribe("some-subscription");
        connection.close();

        assertEquals(1, mockPeer.getContextStats().getUnsubscribeCalls());
    }

    @Test(timeout = 30000)
    public void testSendMessagePassthrough() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "failover:(mock://localhost)");

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createMessage());

        connection.close();

        assertEquals(1, mockPeer.getContextStats().getSendCalls());
    }

    @Test(timeout=10000)
    public void testTimeoutsSetFromConnectionInfo() throws IOException, JMSException {
        final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(4);
        final long CLOSE_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
        final long SEND_TIMEOUT = TimeUnit.SECONDS.toMillis(6);
        final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(7);

        provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }
        });

        provider.connect(connection);
        provider.start();

        JmsConnectionInfo connectionInfo = createConnectionInfo();

        connectionInfo.setConnectTimeout(CONNECT_TIMEOUT);
        connectionInfo.setCloseTimeout(CLOSE_TIMEOUT);
        connectionInfo.setSendTimeout(SEND_TIMEOUT);
        connectionInfo.setRequestTimeout(REQUEST_TIMEOUT);

        ProviderFuture request = new ProviderFuture();
        provider.create(connectionInfo, request);
        request.sync();

        assertEquals(CLOSE_TIMEOUT, provider.getCloseTimeout());
        assertEquals(SEND_TIMEOUT, provider.getSendTimeout());
        assertEquals(REQUEST_TIMEOUT, provider.getRequestTimeout());
    }

    @Test(timeout = 30000)
    public void testAmqpOpenServerListActionDefault() {
        provider = new FailoverProvider(uris);
        assertEquals("REPLACE", provider.getAmqpOpenServerListAction());
    }

    @Test(timeout = 30000)
    public void testSetGetAmqpOpenServerListAction() {
        provider = new FailoverProvider(uris);
        String action = "ADD";
        assertFalse(action.equals(provider.getAmqpOpenServerListAction()));

        provider.setAmqpOpenServerListAction(action);
        assertEquals(action, provider.getAmqpOpenServerListAction());
    }

    @Test(timeout = 30000)
    public void testSetInvalidAmqpOpenServerListActionThrowsIAE() {
        provider = new FailoverProvider(uris);
        try {
            provider.setAmqpOpenServerListAction("invalid");
            fail("no exception was thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }
}
