/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.integration;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.provider.ProviderRedirectedException;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.util.MetaDataSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.hamcrest.Matcher;
import org.junit.Test;

public class ConnectionIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCreateAndCloseConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithClientId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, true);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAutoAckSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Session should not be null", session);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateTransactedSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            assertNotNull("Session should not be null", session);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateTransactedSessionFailsWhenNoDetachResponseSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            ((JmsConnection) connection).setRequestTimeout(500);

            testPeer.expectBegin();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectSenderAttach(notNullValue(), txCoordinatorMatcher, true, true, false, 0, null, null);
            testPeer.expectDetach(true, false, false);

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Session create should have failed.");
            } catch (JMSException ex) {
                // Expected
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSessionCreation() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then explicitly close the connection with an error
            testPeer.expectBegin(notNullValue(), false);
            testPeer.remotelyCloseConnection(true, AmqpError.NOT_ALLOWED, BREAD_CRUMB);

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyDropConnectionDuringSessionCreation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then drop connection without without a close frame.
            testPeer.expectBegin(notNullValue(), false);
            testPeer.dropAfterLastHandler();

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectionMetaDataVersion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            ConnectionMetaData meta = connection.getMetaData();
            int result = meta.getProviderMajorVersion() + meta.getProviderMinorVersion();
            assertTrue("Expected non-zero provider major / minor version", result != 0);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectionPropertiesContainExpectedMetaData() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            Matcher<?> connPropsMatcher =  allOf(hasEntry(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME),
                    hasEntry(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION),
                    hasEntry(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS));

            testPeer.expectSaslAnonymousConnect(null, null, connPropsMatcher, null);
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?jms.clientID=foo");
            Connection connection = factory.createConnection();

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testMaxFrameSizeOptionCommunicatedInOpen() throws Exception {
        int frameSize = 39215;
        doMaxFrameSizeOptionTestImpl(frameSize, UnsignedInteger.valueOf(frameSize));
    }

    @Test(timeout = 20000)
    public void testMaxFrameSizeOptionCommunicatedInOpenDefault() throws Exception {
        doMaxFrameSizeOptionTestImpl(-1, UnsignedInteger.MAX_VALUE);
    }

    private void doMaxFrameSizeOptionTestImpl(int uriOption, UnsignedInteger transmittedValue) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslLayerDisabledConnect(equalTo(transmittedValue));
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uri = "amqp://localhost:" + testPeer.getServerPort() + "?amqp.saslLayer=false&amqp.maxFrameSize=" + uriOption;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForAllHandlersToComplete(3000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameSetByDefault() throws Exception {
        doAmqpHostnameTestImpl("localhost", false, equalTo("localhost"));
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameSetByVhostOption() throws Exception {
        String vhost = "myAmqpHost";
        doAmqpHostnameTestImpl(vhost, true, equalTo(vhost));
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameNotSetWithEmptyVhostOption() throws Exception {
        doAmqpHostnameTestImpl("", true, nullValue());
    }

    private void doAmqpHostnameTestImpl(String amqpHostname, boolean setHostnameOption, Matcher<?> hostnameMatcher) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymousConnect(null, hostnameMatcher);
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            String uri = "amqp://localhost:" + testPeer.getServerPort();
            if(setHostnameOption) {
                uri += "?amqp.vhost=" + amqpHostname;
            }

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionListenerInvoked() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch done = new CountDownLatch(1);

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            testPeer.remotelyCloseConnection(true);

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue("Connection should report failure", done.await(5, TimeUnit.SECONDS));

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionWithRedirect() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicReference<JMSException> asyncError = new AtomicReference<JMSException>();

            final String redirectVhost = "vhost";
            final String redirectNetworkHost = "localhost";
            final int redirectPort = 5677;

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            Map<Symbol, Object> errorInfo = new HashMap<Symbol, Object>();
            errorInfo.put(OPEN_HOSTNAME, redirectVhost);
            errorInfo.put(NETWORK_HOST, redirectNetworkHost);
            errorInfo.put(PORT, 5677);

            testPeer.remotelyCloseConnection(true, ConnectionError.REDIRECT, "Connection redirected", errorInfo);

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    asyncError.set(exception);
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue("Connection should report failure", done.await(5, TimeUnit.SECONDS));

            assertTrue(asyncError.get() instanceof JMSException);
            assertTrue(asyncError.get().getCause() instanceof ProviderRedirectedException);

            ProviderRedirectedException redirect = (ProviderRedirectedException) asyncError.get().getCause();
            assertEquals(redirectVhost, redirect.getHostname());
            assertEquals(redirectNetworkHost, redirect.getNetworkHost());
            assertEquals(redirectPort, redirect.getPort());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionWithSessionWithConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, then remotely end the connection afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("connection never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return !((JmsConnection) connection).isConnected();
                }
            }, 10000, 10));

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the consumer is now marked closed
            try {
                consumer.getMessageListener();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Try closing them explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
            session.close();
        }
    }
}
