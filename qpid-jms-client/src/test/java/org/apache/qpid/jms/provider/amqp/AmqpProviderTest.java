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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.util.IdGenerator;
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

    private TestAmqpPeer testPeer;
    private URI peerURI;
    private AmqpProvider provider;

    @Override
    @Before
    public void setUp() throws Exception {
        testPeer = new TestAmqpPeer();
        peerURI = new URI("amqp://localhost:" + testPeer.getServerPort());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }

        if (testPeer != null) {
            testPeer.close();
            testPeer = null;
        }
    }

    @Test(timeout=20000)
    public void testCreate() {
        provider = new AmqpProvider(peerURI);
    }

    @Test(timeout=20000, expected=RuntimeException.class)
    public void testGetMessageFactoryTrowsWhenNotConnected() {
        provider = new AmqpProvider(peerURI);
        provider.getMessageFactory();
    }

    @Test(timeout=20000)
    public void testConnectWithUnknownProtocol() throws Exception {
        provider = new AmqpProvider(peerURI);
        provider.setTransportType("ftp");
        try {
            provider.connect();
            fail("Should have failed to connect.");
        } catch (Exception ex) {
        }
    }

    @Test(timeout=20000)
    public void testConnectThrowsWhenNoPeer() throws Exception {
        provider = new AmqpProvider(peerURI);
        testPeer.close();
        try {
            provider.connect();
            fail("Should have failed to connect.");
        } catch (Exception ex) {
        }
    }

    @Test(timeout=20000)
    public void testStartThrowsIfNoListenerSet() throws Exception {
        provider = new AmqpProvider(peerURI);
        provider.connect();

        try {
            provider.start();
            fail("Should have thrown an error, no listener registered.");
        } catch (Exception ex) {
        }
    }

    @Test(timeout=20000)
    public void testToString() throws IOException {
        provider = new AmqpProvider(peerURI);
        provider.connect();
        assertTrue(provider.toString().contains("localhost"));
        assertTrue(provider.toString().contains(String.valueOf(peerURI.getPort())));
    }

    @Test(timeout=20000)
    public void testClosedProviderThrowsIOException() throws IOException {
        provider = new AmqpProvider(peerURI);
        provider.connect();
        provider.close();

        try {
            provider.start();
            fail("Should have thrown an IOException when closed.");
        } catch (IOException ex) {}

        try {
            provider.connect();
            fail("Should have thrown an IOException when closed.");
        } catch (IOException ex) {}

        ProviderFuture request = new ProviderFuture();
        try {
            provider.unsubscribe("subscription-name", request);
            fail("Should have thrown an IOException when closed.");
        } catch (IOException ex) {}
    }

    @Test(timeout=20000)
    public void testTimeoutsSetFromConnectionInfo() throws IOException, JMSException {
        final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(4);
        final long CLOSE_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
        final long SEND_TIMEOUT = TimeUnit.SECONDS.toMillis(6);
        final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(7);

        provider = new AmqpProvider(peerURI);
        testPeer.expectSaslPlainConnect(TEST_USERNAME, TEST_PASSWORD, null, null);
        testPeer.expectBegin();
        provider.connect();
        testPeer.expectClose();

        JmsConnectionInfo connectionInfo = createConnectionInfo();

        connectionInfo.setConnectTimeout(CONNECT_TIMEOUT);
        connectionInfo.setCloseTimeout(CLOSE_TIMEOUT);
        connectionInfo.setSendTimeout(SEND_TIMEOUT);
        connectionInfo.setRequestTimeout(REQUEST_TIMEOUT);

        ProviderFuture request = new ProviderFuture();
        provider.create(connectionInfo, request);
        request.sync();

        assertEquals(CONNECT_TIMEOUT, provider.getConnectTimeout());
        assertEquals(CLOSE_TIMEOUT, provider.getCloseTimeout());
        assertEquals(SEND_TIMEOUT, provider.getSendTimeout());
        assertEquals(REQUEST_TIMEOUT, provider.getRequestTimeout());
    }

    private JmsConnectionInfo createConnectionInfo() {
        JmsConnectionId connectionId = new JmsConnectionId(connectionIdGenerator.generateId());
        JmsConnectionInfo connectionInfo = new JmsConnectionInfo(connectionId);

        connectionInfo.setUsername(TEST_USERNAME);
        connectionInfo.setPassword(TEST_PASSWORD);

        return connectionInfo;
    }
}
