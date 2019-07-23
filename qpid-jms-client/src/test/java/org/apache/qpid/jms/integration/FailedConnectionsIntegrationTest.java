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
package org.apache.qpid.jms.integration;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the handling of various error on connection that should return
 * specific error types to the JMS client.
 */
public class FailedConnectionsIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailedConnectionsIntegrationTest.class);

    @Test(timeout = 20000)
    public void testConnectWithInvalidClientIdThrowsJMSEWhenInvalidContainerHintNotPresent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", null);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSException");
            } catch (JMSException jmsEx) {
                // Expected
            } catch (Exception ex) {
                fail("Should have thrown JMSException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectThrowsTimedOutExceptioWhenResponseNotSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(true);
            testPeer.expectClose();
            try {
                establishAnonymousConnecton(testPeer, true, "jms.connectTimeout=500");
                fail("Should have thrown JmsOperationTimedOutException");
            } catch (JmsOperationTimedOutException jmsEx) {
                // Expected
            } catch (Exception ex) {
                fail("Should have thrown JmsOperationTimedOutException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectWithNotFoundErrorThrowsJMSEWhenInvalidContainerHintNotPresent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(AmqpError.NOT_FOUND, "Virtual Host does not exist", null);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSException");
            } catch (InvalidDestinationException destEx) {
                fail("Should not convert to destination exception for this case.");
            } catch (JMSException jmsEx) {
                LOG.info("Caught expected Exception: {}", jmsEx.getMessage(), jmsEx);
                // Expected
            } catch (Exception ex) {
                fail("Should have thrown JMSException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectWithInvalidClientIdThrowsICIDEWhenInvalidContainerHintPresent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final String remoteURI = "amqp://localhost:" + testPeer.getServerPort();

            Map<Symbol, Object> errorInfo = new HashMap<Symbol, Object>();
            errorInfo.put(AmqpSupport.INVALID_FIELD, AmqpSupport.CONTAINER_ID);

            testPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", errorInfo);

            Connection connection = null;
            try {
                ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
                connection = factory.createConnection();
                connection.setClientID("in-use-client-id");

                fail("Should have thrown InvalidClientIDException");
            } catch (InvalidClientIDException e) {
                // Expected
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectionFactoryCreateConnectionWithInvalidClientIdThrowsICIDEWhenInvalidContainerHintPresent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final String remoteURI = "amqp://localhost:" + testPeer.getServerPort();

            Map<Symbol, Object> errorInfo = new HashMap<Symbol, Object>();
            errorInfo.put(AmqpSupport.INVALID_FIELD, AmqpSupport.CONTAINER_ID);

            testPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", errorInfo);

            Connection connection = null;
            try {
                JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

                // Setting on factory prompts the open to fire on create as opposed to waiting
                // for the setClientID method or the start method on Connection to be called.
                factory.setClientID("in-use-client-id");

                connection = factory.createConnection();

                fail("Should have thrown InvalidClientIDException");
            } catch (InvalidClientIDException e) {
                // Expected
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectSecurityViolation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(AmqpError.UNAUTHORIZED_ACCESS, "Anonymous connections not allowed", null);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSSecurityException");
            } catch (JMSException jmsEx) {
            } catch (Exception ex) {
                fail("Should have thrown JMSSecurityException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectWithRedirect() throws Exception {
        Map<Symbol, Object> redirectInfo = new HashMap<Symbol, Object>();

        redirectInfo.put(OPEN_HOSTNAME, "vhost");
        redirectInfo.put(NETWORK_HOST, "127.0.0.1");
        redirectInfo.put(PORT, 5672);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(ConnectionError.REDIRECT, "Server is full, go away", redirectInfo);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSException");
            } catch (JMSException jmsex) {
                assertTrue(jmsex.getCause() instanceof ProviderConnectionRedirectedException);
                ProviderConnectionRedirectedException redirectEx = (ProviderConnectionRedirectedException) jmsex.getCause();

                URI redirectionURI = redirectEx.getRedirectionURI();

                assertNotNull(redirectionURI);
                assertTrue("vhost", redirectionURI.getQuery().contains("amqp.vhost=vhost"));
                assertEquals("127.0.0.1", redirectionURI.getHost());
                assertEquals(5672, redirectionURI.getPort());
            } catch (Exception ex) {
                fail("Should have thrown JMSException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    Connection establishAnonymousConnecton(TestAmqpPeer testPeer, boolean setClientId) throws JMSException {
        return establishAnonymousConnecton(testPeer, setClientId, null);
    }

    Connection establishAnonymousConnecton(TestAmqpPeer testPeer, boolean setClientId, String connectionQuery) throws JMSException {

        String remoteURI = "amqp://localhost:" + testPeer.getServerPort();

        if (connectionQuery != null && !connectionQuery.isEmpty()) {
            remoteURI += "?" + connectionQuery;
        }

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        Connection connection = factory.createConnection();

        if (setClientId) {
            // Set a clientId to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");
        }

        assertNull(testPeer.getThrowable());
        return connection;
    }
}
