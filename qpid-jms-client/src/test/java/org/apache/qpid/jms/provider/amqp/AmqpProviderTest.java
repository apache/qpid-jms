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
package org.apache.qpid.jms.provider.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.meta.JmsAbstractResource;
import org.apache.qpid.jms.meta.JmsAbstractResourceId;
import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResourceId;
import org.apache.qpid.jms.meta.JmsResourceVistor;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test some basic functionality of the AmqpProvider
 */
public class AmqpProviderTest extends QpidJmsTestCase {

    private final IdGenerator connectionIdGenerator = new IdGenerator();
    private final String TEST_USERNAME = "user";
    private final String TEST_PASSWORD = "password";

    private AmqpProvider provider;
    private JmsConnectionInfo connectionInfo;

    @Override
    @Before
    public void setUp() throws Exception {
        connectionInfo = new JmsConnectionInfo(new JmsConnectionId("ID:TEST-Connection:1"));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    @Test(timeout=20000)
    public void testCreate() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
    }

    @Test(timeout=20000, expected=RuntimeException.class)
    public void testGetMessageFactoryTrowsWhenNotConnected() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        provider.getMessageFactory();
    }

    @Test(timeout=20000)
    public void testUnInitializedProviderReturnsDefaultConnectTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT, provider.getConnectTimeout());
    }

    @Test(timeout=20000)
    public void testUnInitializedProviderReturnsDefaultCloseTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT, provider.getCloseTimeout());
    }

    @Test(timeout=20000)
    public void testUnInitializedProviderReturnsDefaultSendTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(JmsConnectionInfo.DEFAULT_SEND_TIMEOUT, provider.getSendTimeout());
    }

    @Test(timeout=20000)
    public void testUnInitializedProviderReturnsDefaultRequestTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT, provider.getRequestTimeout());
    }

    @Test(timeout=20000)
    public void testGetDefaultDrainTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(TimeUnit.MINUTES.toMillis(1), provider.getDrainTimeout());
    }

    @Test(timeout=20000)
    public void testGetDefaultIdleTimeout() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        assertEquals(TimeUnit.MINUTES.toMillis(1), provider.getIdleTimeout());
    }

    @Test(timeout=20000)
    public void testEnableTraceFrames() throws Exception {
        provider = new AmqpProviderFactory().createProvider(getDefaultURI());
        TransportImpl transport = (TransportImpl) provider.getProtonTransport();
        assertNotNull(transport);
        assertNull(transport.getProtocolTracer());
        provider.setTraceFrames(true);
        assertNotNull(transport.getProtocolTracer());
    }

    @Test(timeout=20000)
    public void testCreateFailsWithUnknownProtocol() throws Exception {
        try {
            AmqpProviderFactory factory = new AmqpProviderFactory();
            factory.setTransportScheme("ftp");
            factory.createProvider(new URI("ftp://localhost:5672"));
            fail("Should have failed to connect.");
        } catch (Exception ex) {
        }
    }

    @Test(timeout=20000)
    public void testConnectThrowsWhenNoPeer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            URI peerURI = getPeerURI(testPeer);
            testPeer.close();

            provider = new AmqpProviderFactory().createProvider(peerURI);
            try {
                provider.connect(connectionInfo);
                fail("Should have failed to connect.");
            } catch (Exception ex) {
            }
        }
    }

    @Test(timeout=20000)
    public void testDisableSaslLayer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            provider.setSaslLayer(false);

            testPeer.expectSaslLayerDisabledConnect(null);

            provider.connect(connectionInfo);

            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetIdleTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));

            TransportImpl transport = (TransportImpl) provider.getProtonTransport();

            final int NEW_TIMEOUT = provider.getIdleTimeout() + 500;

            provider.setIdleTimeout(NEW_TIMEOUT);
            provider.connect(connectionInfo);

            assertEquals(NEW_TIMEOUT, transport.getIdleTimeout());

            testPeer.expectOpen();
            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetMaxFrameSize() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));

            TransportImpl transport = (TransportImpl) provider.getProtonTransport();

            final int NEW_MAX_FRAME_SIZE = provider.getMaxFrameSize() + 500;

            provider.setMaxFrameSize(NEW_MAX_FRAME_SIZE);
            provider.connect(connectionInfo);

            assertEquals(NEW_MAX_FRAME_SIZE, transport.getMaxFrameSize());

            testPeer.expectOpen();
            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSkipSetMaxFrameSize() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));

            TransportImpl transport = (TransportImpl) provider.getProtonTransport();

            final int RECORDED_MAX = transport.getMaxFrameSize();

            provider.setMaxFrameSize(-1);
            provider.connect(connectionInfo);

            assertEquals(RECORDED_MAX, transport.getMaxFrameSize());

            testPeer.expectOpen();
            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
    @Test(timeout=20000)
    public void testStartThrowsIfNoListenerSet() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            provider.connect(connectionInfo);

            assertNull(provider.getProviderListener());

            try {
                provider.start();
                fail("Should have thrown an error, no listener registered.");
            } catch (Exception ex) {
            }

            provider.setProviderListener(new DefaultProviderListener());

            assertNotNull(provider.getProviderListener());

            try {
                provider.start();
            } catch (Exception ex) {
                fail("Should not have thrown an error, listener registered.");
            }

            testPeer.expectOpen();
            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testToString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            provider.connect(connectionInfo);
            assertTrue(provider.toString().contains("localhost"));
            assertTrue(provider.toString().contains(String.valueOf(testPeer.getServerPort())));

            testPeer.expectOpen();
            testPeer.expectClose();

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testClosedProviderThrowsIOException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectClose();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            provider.connect(connectionInfo);
            provider.close();

            try {
                provider.start();
                fail("Should have thrown an IOException when closed.");
            } catch (Exception ex) {}

            try {
                provider.connect(connectionInfo);
                fail("Should have thrown an IOException when closed.");
            } catch (Exception ex) {}

            ProviderFuture request = provider.newProviderFuture();
            try {
                provider.unsubscribe("subscription-name", request);
                fail("Should have thrown an IOException when closed.");
            } catch (Exception ex) {}

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTimeoutsSetFromConnectionInfo() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(4);
            final long CLOSE_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
            final long SEND_TIMEOUT = TimeUnit.SECONDS.toMillis(6);
            final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(7);

            connectionInfo.setUsername(TEST_USERNAME);
            connectionInfo.setPassword(TEST_PASSWORD);

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            testPeer.expectSaslPlain(TEST_USERNAME, TEST_PASSWORD);
            testPeer.expectOpen();
            testPeer.expectBegin();
            provider.connect(connectionInfo);
            testPeer.expectClose();

            JmsConnectionInfo connectionInfo = createConnectionInfo();

            connectionInfo.setConnectTimeout(CONNECT_TIMEOUT);
            connectionInfo.setCloseTimeout(CLOSE_TIMEOUT);
            connectionInfo.setSendTimeout(SEND_TIMEOUT);
            connectionInfo.setRequestTimeout(REQUEST_TIMEOUT);

            ProviderFuture request = provider.newProviderFuture();
            provider.create(connectionInfo, request);
            request.sync();

            assertEquals(CONNECT_TIMEOUT, provider.getConnectTimeout());
            assertEquals(CLOSE_TIMEOUT, provider.getCloseTimeout());
            assertEquals(SEND_TIMEOUT, provider.getSendTimeout());
            assertEquals(REQUEST_TIMEOUT, provider.getRequestTimeout());

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testErrorDuringCreateResourceFailsRequest() throws Exception {
        doErrorDuringOperationFailsRequestTestImpl(Op.CREATE);
    }

    @Test(timeout = 20000)
    public void testErrorDuringStartResourceFailsRequest() throws Exception {
        doErrorDuringOperationFailsRequestTestImpl(Op.START);
    }

    @Test(timeout = 20000)
    public void testErrorDuringStopResourceFailsRequest() throws Exception {
        doErrorDuringOperationFailsRequestTestImpl(Op.STOP);
    }

    @Test(timeout = 20000)
    public void testErrorDuringDestroyResourceFailsRequest() throws Exception {
        doErrorDuringOperationFailsRequestTestImpl(Op.DESTROY);
    }

    private void doErrorDuringOperationFailsRequestTestImpl(Op operation) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectClose();

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));
            provider.connect(connectionInfo);

            final AtomicBoolean errorThrown = new AtomicBoolean();
            JmsResource resourceInfo = new JmsAbstractResource() {
                @Override
                public void visit(JmsResourceVistor visitor) {
                    errorThrown.set(true);
                    throw new Error("Deliberate error for testing");
                }

                @Override
                public JmsResourceId getId() {
                    return new JmsAbstractResourceId() {
                    };
                }
            };

            assertFalse("Error should not yet be thrown", errorThrown.get());
            ProviderFuture request = provider.newProviderFuture();

            switch(operation) {
            case CREATE:
                provider.create(resourceInfo, request);
                break;
            case START:
                provider.start(resourceInfo, request);
                break;
            case STOP:
                provider.stop(resourceInfo, request);
                break;
            case DESTROY:
                provider.destroy(resourceInfo, request);
                break;
            default:
                throw new IllegalArgumentException("Unexpected operation given");
            }

            try {
                request.sync();
                fail("Request should have failed");
            } catch (Exception e) {
                // Expected
            }

            assertTrue("Error should have been thrown", errorThrown.get());

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateResourceFailsWhenNoConnectCalled() throws Exception {
        doErrorDuringOperationFailsWhenNoConnectCalledTestImpl(Op.CREATE);
    }

    @Test(timeout = 20000)
    public void testStartResourceFailsWhenNoConnectCalled() throws Exception {
        doErrorDuringOperationFailsWhenNoConnectCalledTestImpl(Op.START);
    }

    @Test(timeout = 20000)
    public void testStopResourceFailsWhenNoConnectCalled() throws Exception {
        doErrorDuringOperationFailsWhenNoConnectCalledTestImpl(Op.STOP);
    }

    @Test(timeout = 20000)
    public void testDestroyResourceFailsWhenNoConnectCalled() throws Exception {
        doErrorDuringOperationFailsWhenNoConnectCalledTestImpl(Op.DESTROY);
    }

    private void doErrorDuringOperationFailsWhenNoConnectCalledTestImpl(Op operation) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer()) {

            provider = new AmqpProviderFactory().createProvider(getPeerURI(testPeer));

            final AtomicBoolean errorThrown = new AtomicBoolean();
            JmsResource resourceInfo = new JmsAbstractResource() {
                @Override
                public void visit(JmsResourceVistor visitor) {
                    errorThrown.set(true);
                    throw new Error("Deliberate error for testing");
                }

                @Override
                public JmsResourceId getId() {
                    return new JmsAbstractResourceId() {
                    };
                }
            };

            assertFalse("Error should not have been thrown", errorThrown.get());
            ProviderFuture request = provider.newProviderFuture();

            switch(operation) {
            case CREATE:
                try {
                    provider.create(resourceInfo, request);
                    fail("Request should have failed");
                } catch (ProviderException e) {
                    // Expected
                }
                break;
            case START:
                try {
                    provider.start(resourceInfo, request);
                    fail("Request should have failed");
                } catch (ProviderException e) {
                    // Expected
                }
                break;
            case STOP:
                try {
                    provider.stop(resourceInfo, request);
                    fail("Request should have failed");
                } catch (ProviderException e) {
                    // Expected
                }
                break;
            case DESTROY:
                try {
                    provider.destroy(resourceInfo, request);
                    fail("Request should have failed");
                } catch (ProviderException e) {
                    // Expected
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected operation given");
            }

            assertFalse("Error should not have been thrown", errorThrown.get());

            provider.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private JmsConnectionInfo createConnectionInfo() {
        JmsConnectionId connectionId = new JmsConnectionId(connectionIdGenerator.generateId());
        JmsConnectionInfo connectionInfo = new JmsConnectionInfo(connectionId);

        connectionInfo.setUsername(TEST_USERNAME);
        connectionInfo.setPassword(TEST_PASSWORD);

        return connectionInfo;
    }

    private enum Op {
        CREATE, START, STOP, DESTROY
    }

    private URI getDefaultURI() throws URISyntaxException {
        return new URI("amqp://localhost:5672");
    }

    private URI getPeerURI(TestAmqpPeer peer) throws URISyntaxException {
        return new URI("amqp://localhost:" + peer.getServerPort());
    }
}
