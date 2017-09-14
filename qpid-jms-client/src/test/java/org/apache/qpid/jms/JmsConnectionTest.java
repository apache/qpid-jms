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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.mock.MockProvider;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsConnectionTest {

    private final IdGenerator clientIdGenerator = new IdGenerator();

    private MockProvider provider;
    private JmsConnection connection;
    private JmsConnectionInfo connectionInfo;

    @Before
    public void setUp() throws Exception {
        provider = (MockProvider) MockProviderFactory.create(new URI("mock://localhost"));
        connectionInfo = new JmsConnectionInfo(new JmsConnectionId("ID:TEST:1"));
        connectionInfo.setClientId(clientIdGenerator.generateId(), false);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testJmsConnectionThrowsJMSExceptionProviderStartFails() throws JMSException, IllegalStateException, IOException {
        provider.getConfiguration().setFailOnStart(true);
        try (JmsConnection connection = new JmsConnection(connectionInfo, provider);) {}
    }

    @Test(timeout=30000)
    public void testStateAfterCreate() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isStarted());
        assertFalse(connection.isClosed());
        assertFalse(connection.isConnected());
    }

    @Test(timeout=30000)
    public void testGetExceptionListener() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        assertNull(connection.getExceptionListener());
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        });

        assertNotNull(connection.getExceptionListener());
    }

    @Test(timeout=30000)
    public void testReplacePrefetchPolicy() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);

        JmsDefaultPrefetchPolicy newPolicy = new JmsDefaultPrefetchPolicy();
        newPolicy.setAll(1);

        assertNotSame(newPolicy, connection.getPrefetchPolicy());
        connection.setPrefetchPolicy(newPolicy);
        assertEquals(newPolicy, connection.getPrefetchPolicy());
    }

    @Test(timeout=30000)
    public void testGetConnectionId() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);
        assertEquals("ID:TEST:1", connection.getId().toString());
    }

    @Test(timeout=30000)
    public void testAddConnectionListener() throws JMSException {
        connection = new JmsConnection(connectionInfo, provider);
        JmsConnectionListener listener = new JmsDefaultConnectionListener();
        assertFalse(connection.removeConnectionListener(listener));
        connection.addConnectionListener(listener);
        assertTrue(connection.removeConnectionListener(listener));
    }

    @Test(timeout=30000)
    public void testConnectionStart() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
    }

    @Test(timeout=30000)
    public void testConnectionMulitpleStartCalls() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
    }

    @Test(timeout=30000)
    public void testConnectionStartAndStop() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.stop();
        assertTrue(connection.isConnected());
    }

    @Test(timeout=30000, expected=InvalidClientIDException.class)
    public void testSetClientIDFromNull() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        assertFalse(connection.isConnected());
        connection.setClientID("");
    }

    @Test(timeout=30000)
    public void testCreateNonTXSessionWithTXAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(false, Session.SESSION_TRANSACTED);
            fail("Should not allow non-TX session with mode SESSION_TRANSACTED");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testCreateNonTXSessionWithUnknownAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(false, 99);
            fail("Should not allow unkown Ack modes.");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testCreateSessionWithUnknownAckMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        try {
            connection.createSession(99);
            fail("Should not allow unkown Ack modes.");
        } catch (JMSException ex) {
        }
    }

    @Test(timeout=30000)
    public void testCreateSessionDefaultMode() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        JmsSession session = (JmsSession) connection.createSession();
        assertEquals(session.getSessionMode(), Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout=30000, expected=InvalidClientIDException.class)
    public void testSetClientIDFromEmptyString() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        assertFalse(connection.isConnected());
        connection.setClientID(null);
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetClientIDFailsOnSecondCall() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.setClientID("TEST-ID");
        assertTrue(connection.isConnected());
        connection.setClientID("TEST-ID");
    }

    @Test(timeout=30000, expected=IllegalStateException.class)
    public void testSetClientIDFailsAfterStart() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);

        assertFalse(connection.isConnected());
        connection.start();
        assertTrue(connection.isConnected());
        connection.setClientID("TEST-ID");
    }

    @Test(timeout=30000)
    public void testDeleteOfTempQueueOnClosedConnection() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue tempQueue = session.createTemporaryQueue();
        assertNotNull(tempQueue);

        connection.close();
        try {
            tempQueue.delete();
            fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
        }
    }

    @Test(timeout=30000)
    public void testDeleteOfTempTopicOnClosedConnection() throws JMSException, IOException {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic tempTopic = session.createTemporaryTopic();
        assertNotNull(tempTopic);

        connection.close();
        try {
            tempTopic.delete();
            fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
        }
    }

    @Test(timeout=30000)
    public void testConnectionCreatedSessionRespectsAcknowledgementMode() throws Exception {
        connection = new JmsConnection(connectionInfo, provider);
        connection.start();

        JmsSession session = (JmsSession) connection.createSession(Session.SESSION_TRANSACTED);
        try {
            session.acknowledge(ACK_TYPE.ACCEPTED);
            fail("Should be in TX mode and not allow explicit ACK.");
        } catch (IllegalStateException ise) {
        }
    }

    @Test(timeout=30000)
    public void testConnectionMetaData() throws Exception {
        connection = new JmsConnection(connectionInfo, provider);

        ConnectionMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertEquals(2, metaData.getJMSMajorVersion());
        assertEquals(0, metaData.getJMSMinorVersion());
        assertEquals("2.0", metaData.getJMSVersion());
        assertNotNull(metaData.getJMSXPropertyNames());

        assertNotNull(metaData.getProviderVersion());
        assertNotNull(metaData.getJMSProviderName());

        int major = metaData.getProviderMajorVersion();
        int minor = metaData.getProviderMinorVersion();
        assertTrue("Expected non-zero provider major(" + major + ") / minor(" + minor +") version.", (major + minor) != 0);
    }
}
