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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.message.JmsMessageIDBuilder.BUILTIN;
import org.apache.qpid.jms.policy.JmsDefaultMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.policy.JmsMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsPresettlePolicy;
import org.apache.qpid.jms.policy.JmsRedeliveryPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactoryIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactoryIntegrationTest.class);

    @Test(timeout=20000)
    public void testCreateConnectionGoodProviderURI() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqp://127.0.0.1:" + testPeer.getServerPort()));
            Connection connection = factory.createConnection();
            assertNotNull(connection);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testCreateConnectionGoodProviderString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            assertNotNull(connection);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testTopicCreateConnectionGoodProviderString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + testPeer.getServerPort());
            TopicConnection connection = factory.createTopicConnection();
            assertNotNull(connection);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testCreateQueueConnectionGoodProviderString() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + testPeer.getServerPort());
            QueueConnection connection = factory.createQueueConnection();
            assertNotNull(connection);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testUriOptionsAppliedToConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.localMessagePriority=true&jms.forceAsyncSend=true";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            assertTrue(factory.isLocalMessagePriority());
            assertTrue(factory.isForceAsyncSend());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertNotNull(connection);
            assertTrue(connection.isLocalMessagePriority());
            assertTrue(connection.isForceAsyncSend());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testCreateAmqpConnectionWithUserInfoThrowsJMSEx() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://user:pass@127.0.0.1:" + testPeer.getServerPort();
            try {
                new JmsConnectionFactory(uri);
                fail("Should not be able to create a factory with user info value set.");
            } catch (Exception ex) {
                LOG.debug("Caught expected exception on invalid message ID format: {}", ex);
            }
        }
    }

    @Test(timeout=20000)
    public void testSetInvalidMessageIDFormatOption() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=UNKNOWN";
            try {
                new JmsConnectionFactory(uri);
                fail("Should not be able to create a factory with invalid id type option value.");
            } catch (Exception ex) {
                LOG.debug("Caught expected exception on invalid message ID format: {}", ex);
            }
        }
    }

    @Test(timeout=20000)
    public void testSetMessageIDFormatOptionAlteredCase() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            try {
                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=uuid";
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                JmsDefaultMessageIDPolicy policy = (JmsDefaultMessageIDPolicy) factory.getMessageIDPolicy();
                assertEquals(JmsMessageIDBuilder.BUILTIN.UUID.name(), policy.getMessageIDType());
            } catch (Exception ex) {
                fail("Should have succeeded in creating factory");
            }

            try {
                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=Uuid";
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                JmsDefaultMessageIDPolicy policy = (JmsDefaultMessageIDPolicy) factory.getMessageIDPolicy();
                assertEquals(JmsMessageIDBuilder.BUILTIN.UUID.name(), policy.getMessageIDType());
            } catch (Exception ex) {
                fail("Should have succeeded in creating factory");
            }
        }
    }

    @Test(timeout = 20000)
    public void testMessageIDFormatOptionApplied() throws Exception {
        BUILTIN[] formatters = JmsMessageIDBuilder.BUILTIN.values();

        for (BUILTIN formatter : formatters) {
            LOG.info("Testing application of Message ID Format: {}", formatter.name());
            try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
                // Ignore errors from peer close due to not sending any Open / Close frames
                testPeer.setSuppressReadExceptionOnClose(true);

                // DONT create a test fixture, we will drive everything directly.
                testPeer.expectSaslAnonymous();

                String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=" + formatter.name();
                JmsConnectionFactory factory = new JmsConnectionFactory(uri);
                assertEquals(formatter.name(), ((JmsDefaultMessageIDPolicy) factory.getMessageIDPolicy()).getMessageIDType());

                JmsConnection connection = (JmsConnection) factory.createConnection();
                assertEquals(formatter.name(), ((JmsDefaultMessageIDPolicy) connection.getMessageIDPolicy()).getMessageIDBuilder().toString());

                testPeer.waitForAllHandlersToComplete(1000);

                testPeer.expectOpen();
                testPeer.expectClose();

                connection.close();

                testPeer.waitForAllHandlersToCompleteNoAssert(1000);
            }
        }
    }

    @Test(timeout=20000)
    public void testSetCustomMessageIDBuilder() throws Exception {
        CustomJmsMessageIdBuilder custom = new CustomJmsMessageIdBuilder();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            // DONT create a test fixture, we will drive everything directly.
            testPeer.expectSaslAnonymous();

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            ((JmsDefaultMessageIDPolicy) factory.getMessageIDPolicy()).setMessageIDBuilder(custom);
            assertEquals(custom.toString(), ((JmsDefaultMessageIDPolicy) factory.getMessageIDPolicy()).getMessageIDType());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertEquals(custom.toString(), ((JmsDefaultMessageIDPolicy) connection.getMessageIDPolicy()).getMessageIDBuilder().toString());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetCustomMessageIDPolicy() throws Exception {
        CustomJmsMessageIDPolicy custom = new CustomJmsMessageIDPolicy();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            factory.setMessageIDPolicy(custom);
            assertEquals(custom, factory.getMessageIDPolicy());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertTrue(connection.getMessageIDPolicy() instanceof CustomJmsMessageIDPolicy);
            assertNotSame(custom, connection.getMessageIDPolicy());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetCustomPrefetchPolicy() throws Exception {
        CustomJmsPrefetchPolicy custom = new CustomJmsPrefetchPolicy();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            factory.setPrefetchPolicy(custom);
            assertEquals(custom, factory.getPrefetchPolicy());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertTrue(connection.getPrefetchPolicy() instanceof CustomJmsPrefetchPolicy);
            assertNotSame(custom, connection.getPrefetchPolicy());

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetCustomPresettlePolicy() throws Exception {
        CustomJmsPresettlePolicy custom = new CustomJmsPresettlePolicy();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            factory.setPresettlePolicy(custom);
            assertEquals(custom, factory.getPresettlePolicy());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertTrue(connection.getPresettlePolicy() instanceof CustomJmsPresettlePolicy);
            assertNotSame(custom, connection.getPresettlePolicy());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=20000)
    public void testSetCustomRedeliveryPolicy() throws Exception {
        CustomJmsRedeliveryPolicy custom = new CustomJmsRedeliveryPolicy();

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Ignore errors from peer close due to not sending any Open / Close frames
            testPeer.setSuppressReadExceptionOnClose(true);

            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort();

            testPeer.expectSaslAnonymous();

            JmsConnectionFactory factory = new JmsConnectionFactory(uri);
            factory.setRedeliveryPolicy(custom);
            assertEquals(custom, factory.getRedeliveryPolicy());

            JmsConnection connection = (JmsConnection) factory.createConnection();
            assertTrue(connection.getRedeliveryPolicy() instanceof CustomJmsRedeliveryPolicy);
            assertNotSame(custom, connection.getRedeliveryPolicy());

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectOpen();
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToCompleteNoAssert(1000);
        }
    }

    @Test(timeout=10000)
    public void testMessageIDPolicyCannotBeNulled() throws Exception {
        CustomJmsMessageIDPolicy custom = new CustomJmsMessageIDPolicy();

        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertTrue(factory.getMessageIDPolicy() instanceof JmsDefaultMessageIDPolicy);

        factory.setMessageIDPolicy(custom);
        assertTrue(factory.getMessageIDPolicy() instanceof CustomJmsMessageIDPolicy);

        factory.setMessageIDPolicy(null);
        assertTrue(factory.getMessageIDPolicy() instanceof JmsDefaultMessageIDPolicy);
    }

    @Test(timeout=10000)
    public void testPrefetchPolicyCannotBeNulled() throws Exception {
        CustomJmsPrefetchPolicy custom = new CustomJmsPrefetchPolicy();

        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertTrue(factory.getPrefetchPolicy() instanceof JmsDefaultPrefetchPolicy);

        factory.setPrefetchPolicy(custom);
        assertTrue(factory.getPrefetchPolicy() instanceof CustomJmsPrefetchPolicy);

        factory.setPrefetchPolicy(null);
        assertTrue(factory.getPrefetchPolicy() instanceof JmsDefaultPrefetchPolicy);
    }

    @Test(timeout=10000)
    public void testPresettlePolicyCannotBeNulled() throws Exception {
        CustomJmsPresettlePolicy custom = new CustomJmsPresettlePolicy();

        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertTrue(factory.getPresettlePolicy() instanceof JmsDefaultPresettlePolicy);

        factory.setPresettlePolicy(custom);
        assertTrue(factory.getPresettlePolicy() instanceof CustomJmsPresettlePolicy);

        factory.setPresettlePolicy(null);
        assertTrue(factory.getPresettlePolicy() instanceof JmsDefaultPresettlePolicy);
    }

    @Test(timeout=10000)
    public void testRedeliveryPolicyCannotBeNulled() throws Exception {
        CustomJmsRedeliveryPolicy custom = new CustomJmsRedeliveryPolicy();

        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertTrue(factory.getRedeliveryPolicy() instanceof JmsDefaultRedeliveryPolicy);

        factory.setRedeliveryPolicy(custom);
        assertTrue(factory.getRedeliveryPolicy() instanceof CustomJmsRedeliveryPolicy);

        factory.setRedeliveryPolicy(null);
        assertTrue(factory.getRedeliveryPolicy() instanceof JmsDefaultRedeliveryPolicy);
    }

    //----- Custom Policy Objects --------------------------------------------//

    private final class CustomJmsMessageIdBuilder implements JmsMessageIDBuilder {

        @Override
        public Object createMessageID(String producerId, long messageSequence) {
            return UUID.randomUUID();
        }

        @Override
        public String toString() {
            return "TEST";
        }
    }

    private class CustomJmsMessageIDPolicy implements JmsMessageIDPolicy {

        @Override
        public JmsMessageIDPolicy copy() {
            return new CustomJmsMessageIDPolicy();
        }

        @Override
        public JmsMessageIDBuilder getMessageIDBuilder(JmsSession session, JmsDestination destination) {
            return JmsMessageIDBuilder.BUILTIN.UUID_STRING.createBuilder();
        }
    }

    private class CustomJmsPrefetchPolicy implements JmsPrefetchPolicy {

        @Override
        public JmsPrefetchPolicy copy() {
            return new CustomJmsPrefetchPolicy();
        }

        @Override
        public int getConfiguredPrefetch(JmsSession session, JmsDestination destination, boolean durable, boolean browser) {
            return JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH;
        }
    }

    private class CustomJmsPresettlePolicy implements JmsPresettlePolicy {

        @Override
        public JmsPresettlePolicy copy() {
            return new CustomJmsPresettlePolicy();
        }

        @Override
        public boolean isProducerPresttled(JmsSession session, JmsDestination destination) {
            return false;
        }

        @Override
        public boolean isConsumerPresttled(JmsSession session, JmsDestination destination) {
            return false;
        }
    }

    private class CustomJmsRedeliveryPolicy implements JmsRedeliveryPolicy {

        @Override
        public JmsRedeliveryPolicy copy() {
            return new CustomJmsRedeliveryPolicy();
        }

        @Override
        public int getMaxRedeliveries(JmsDestination destination) {
            return JmsDefaultRedeliveryPolicy.DEFAULT_MAX_REDELIVERIES;
        }

        @Override
        public int getOutcome(JmsDestination destination) {
            return JmsDefaultRedeliveryPolicy.DEFAULT_OUTCOME;
        }
    }
}
