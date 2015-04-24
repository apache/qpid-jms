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
package org.apache.qpid.jms.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.provider.ProviderRedirectedException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.junit.Test;

/**
 * Test the handling of various error on connection that should return
 * specific error types to the JMS client.
 */
public class FailedConnectionsIntegrationTest extends QpidJmsTestCase {

    @Test(timeout = 5000)
    public void testConnectWithInvalidClientId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(AmqpError.INVALID_FIELD, "Client ID already in use", null);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSException");
            } catch (JMSException jmsEx) {
            } catch (Exception ex) {
                fail("Should have thrown JMSException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
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

    @Test(timeout = 5000)
    public void testConnectWithRedirect() throws Exception {
        Map<String, Object> redirectInfo = new HashMap<String, Object>();

        redirectInfo.put("hostname", "localhost");
        redirectInfo.put("network-host", "127.0.0.1");
        redirectInfo.put("port", 5672);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.rejectConnect(ConnectionError.REDIRECT, "Server is full, go away", redirectInfo);
            try {
                establishAnonymousConnecton(testPeer, true);
                fail("Should have thrown JMSException");
            } catch (JMSException jmsex) {
                assertTrue(jmsex.getCause() instanceof ProviderRedirectedException);
                ProviderRedirectedException redirectEx = (ProviderRedirectedException) jmsex.getCause();
                assertEquals("localhost", redirectEx.getHostname());
                assertEquals("127.0.0.1", redirectEx.getNetworkHost());
                assertEquals(5672, redirectEx.getPort());
            } catch (Exception ex) {
                fail("Should have thrown JMSException: " + ex);
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    Connection establishAnonymousConnecton(TestAmqpPeer testPeer, boolean setClientId) throws JMSException {

        final String remoteURI = "amqp://localhost:" + testPeer.getServerPort();

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
