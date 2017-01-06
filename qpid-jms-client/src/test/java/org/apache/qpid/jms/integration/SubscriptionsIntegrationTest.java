/*
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
 */
package org.apache.qpid.jms.integration;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SHARED_SUBS;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SUB_NAME_DELIMITER;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.proton.amqp.Symbol;
import org.hamcrest.Matcher;
import org.junit.Test;

public class SubscriptionsIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    // -------------------------------------- //

    @Test
    public void testConstants() throws Exception {
        assertEquals(Symbol.valueOf("SHARED-SUBS"), AmqpSupport.SHARED_SUBS);
        assertEquals("|", AmqpSupport.SUB_NAME_DELIMITER);
        assertEquals(Symbol.valueOf("shared"), AmqpSupport.SHARED);
        assertEquals(Symbol.valueOf("global"), AmqpSupport.GLOBAL);
    }

    /**
     * Verifies that a shared durable subscriber detaches links with closed = false.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCloseSharedDurableTopicSubscriberDetachesWithCloseFalse() throws Exception {
        doSharedTopicSubscriberDetachTestImpl(true);
    }

    /**
     * Verifies that a shared volatile subscriber detaches links with closed = true.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCloseSharedVolatileTopicSubscriberDetachesWithCloseTrue() throws Exception {
        doSharedTopicSubscriberDetachTestImpl(false);
    }

    private void doSharedTopicSubscriberDetachTestImpl(boolean durable) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            if (durable) {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            } else {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            MessageConsumer subscriber;
            if (durable) {
                subscriber = session.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                subscriber = session.createSharedConsumer(dest, subscriptionName);
            }

            testPeer.expectDetach(!durable, true, !durable);
            subscriber.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared durable subscribers name their links such that the first link is the
     * bare subscription name, and subsquent links use a counter suffix to ensure they are unique.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubscriberLinkNames() throws Exception {
        doSharedSubsriberLinkNamesHaveUniqueCounterSuffixTestImpl(true, true);
    }

    /**
     * Verifies that a shared volatile subscribers names their links such that the subscription name
     * is suffixed with a counter to ensure they are unique.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubscriberLinkNamesHaveUniqueCounterSuffix() throws Exception {
        doSharedSubsriberLinkNamesHaveUniqueCounterSuffixTestImpl(false, true);
    }

    /**
     * Verifies that on a connection without a ClientID, shared durable subscribers name their links
     * such that the first link is the subscription name with 'global' suffix, and subsequent links
     * additionally append a counter suffix to ensure they are unique.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubscriberLinkNamesNoClientID() throws Exception {
        doSharedSubsriberLinkNamesHaveUniqueCounterSuffixTestImpl(true, false);
    }

    /**
     * Verifies that on a connection without a ClientID, shared volatile subscribers names their links
     * such that the subscription name is suffixed with a 'global' qualifier an counter to ensure
     * they are unique.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubscriberLinkNamesHaveUniqueCounterSuffixNoClientID() throws Exception {
        doSharedSubsriberLinkNamesHaveUniqueCounterSuffixTestImpl(false, false);
    }

    private void doSharedSubsriberLinkNamesHaveUniqueCounterSuffixTestImpl(boolean durable, boolean useClientID) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            Connection connection;
            if(useClientID) {
                connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            } else {
                connection = testFixture.establishConnectonWithoutClientID(testPeer, serverCapabilities);
            }
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the first shared receiver
            if (durable) {
                String linkName = subscriptionName;
                if(!useClientID) {
                    linkName += SUB_NAME_DELIMITER + "global";
                }
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, equalTo(linkName), useClientID);
            } else {
                String linkName;
                if(useClientID) {
                    linkName = subscriptionName + SUB_NAME_DELIMITER + "volatile1";
                } else {
                    linkName = subscriptionName + SUB_NAME_DELIMITER + "global-volatile1";
                }
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, equalTo(linkName), useClientID);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                session.createSharedConsumer(dest, subscriptionName);
            }

            // Attach the second shared receiver, expect the link name to differ
            if (durable) {
                String linkName = subscriptionName;
                if(useClientID) {
                    linkName += SUB_NAME_DELIMITER + "2";
                } else {
                    linkName += SUB_NAME_DELIMITER + "global2";
                }
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, equalTo(linkName), useClientID);
            } else {
                String linkName;
                if(useClientID) {
                    linkName = subscriptionName + SUB_NAME_DELIMITER + "volatile2";
                } else {
                    linkName = subscriptionName + SUB_NAME_DELIMITER + "global-volatile2";
                }
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, equalTo(linkName), useClientID);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                session.createSharedConsumer(dest, subscriptionName);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared durable subscribers names their links on a per-connection basis, such that
     * suffix counter on one connection is not impacted by subscriptions on another connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubsriberLinkNamesMultipleConnectionSubs() throws Exception {
        doMultipleConnectionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(true);
    }

    /**
     * Verifies that a shared volatile subscribers names their links on a per-connection basis, such that
     * suffix counter on one connection is not impacted by subscriptions on another connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubsriberLinkNamesHaveUniqueCounterSuffixMultipleConnectionSubs() throws Exception {
        doMultipleConnectionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(false);
    }

    private void doMultipleConnectionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(boolean durable) throws Exception {
        try (TestAmqpPeer peer1 = new TestAmqpPeer();
             TestAmqpPeer peer2 = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish first connection
            Connection connection1 = testFixture.establishConnecton(peer1, serverCapabilities);
            connection1.start();

            peer1.expectBegin();
            Session sessionConn1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Establish second connection
            Connection connection2 = testFixture.establishConnecton(peer2, serverCapabilities);
            connection2.start();

            peer2.expectBegin();
            Session sessionConn2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = sessionConn1.createTopic(topicName);
            String subscriptionName = "mySubscription";

            Matcher<?> durSubLinkNameMatcher = equalTo(subscriptionName);
            Matcher<?> volatileSubLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");

            // Attach the first connections shared receiver
            if (durable) {
                peer1.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durSubLinkNameMatcher, true);
            } else {
                peer1.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, volatileSubLinkNameMatcher, true);
            }
            peer1.expectLinkFlow();

            if (durable) {
                sessionConn1.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                sessionConn1.createSharedConsumer(dest, subscriptionName);
            }

            // Attach the second connections shared receiver, expect the link name to be the same since its per-connection
            if (durable) {
                peer2.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durSubLinkNameMatcher, true);
            } else {
                peer2.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, volatileSubLinkNameMatcher, true);
            }
            peer2.expectLinkFlow();

            if (durable) {
                sessionConn2.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                sessionConn2.createSharedConsumer(dest, subscriptionName);
            }

            peer1.expectClose();
            connection1.close();

            peer2.expectClose();
            connection2.close();

            peer1.waitForAllHandlersToComplete(1000);
            peer2.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared durable subscribers names their links on a per-connection basis, such that
     * suffix counter for one session is impacted by subscriptions on another session on the same connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubsriberLinkNamesMultipleSessionSubs() throws Exception {
        doMultipleSessionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(true);
    }

    /**
     * Verifies that a shared volatile subscribers names their links on a per-connection basis, such that
     * suffix counter for one session is impacted by subscriptions on another session on the same connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubsriberLinkNamesHaveUniqueCounterSuffixMultipleSessionSubs() throws Exception {
        doMultipleSessionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(false);
    }

    private void doMultipleSessionSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(boolean durable) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            testPeer.expectBegin();
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session1.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the first sessions shared receiver
            if (durable) {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            } else {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session1.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                session1.createSharedConsumer(dest, subscriptionName);
            }

            // Attach the second sessions shared receiver, expect the link name to be different since its per-connection
            if (durable) {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "2");
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            } else {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile2");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session2.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                session2.createSharedConsumer(dest, subscriptionName);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that shared durable subscribers name their links on a per-subscription name basis, such that
     * suffix counter for one subscription nameis not impacted by those for another subscription on the same connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubsriberLinkNamesMultipleNamedSubs() throws Exception {
        doMultipleNamedSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(true);
    }

    /**
     * Verifies that shared volatile subscribers name their links on a per-subscription name basis, such that
     * suffix counter for one subscription name is not impacted by those for another subscription on the same connection.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubsriberLinkNamesHaveUniqueCounterSuffixMultipleNamedSubs() throws Exception {
        doMultipleNamedSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(false);
    }

    private void doMultipleNamedSharedSubscriberLinkNamesHaveUniqueCounterSuffixTestImpl(boolean durable) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName1 = "mySubscription1";
            String subscriptionName2 = "mySubscription2";

            // Attach the first subscriptions shared receivers
            if (durable) {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName1);
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName1, linkNameMatcher, true);
                testPeer.expectLinkFlow();
                linkNameMatcher = equalTo(subscriptionName1 + SUB_NAME_DELIMITER + "2");
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName1, linkNameMatcher, true);
            } else {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName1 + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName1, linkNameMatcher, true);
                testPeer.expectLinkFlow();
                linkNameMatcher = equalTo(subscriptionName1 + SUB_NAME_DELIMITER + "volatile2");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName1, linkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session.createSharedDurableConsumer(dest, subscriptionName1);
                session.createSharedDurableConsumer(dest, subscriptionName1);
            } else {
                session.createSharedConsumer(dest, subscriptionName1);
                session.createSharedConsumer(dest, subscriptionName1);
            }

            // Attach the first subscriptions shared receivers
            if (durable) {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName2);
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName2, linkNameMatcher, true);
                testPeer.expectLinkFlow();
                linkNameMatcher = equalTo(subscriptionName2 + SUB_NAME_DELIMITER + "2");
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName2, linkNameMatcher, true);
            } else {
                Matcher<?> linkNameMatcher = equalTo(subscriptionName2 + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName2, linkNameMatcher, true);
                testPeer.expectLinkFlow();
                linkNameMatcher = equalTo(subscriptionName2 + SUB_NAME_DELIMITER + "volatile2");
                testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName2, linkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            if (durable) {
                session.createSharedDurableConsumer(dest, subscriptionName2);
                session.createSharedDurableConsumer(dest, subscriptionName2);
            } else {
                session.createSharedConsumer(dest, subscriptionName2);
                session.createSharedConsumer(dest, subscriptionName2);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared volatile subscriber and shared durable subscriber with the same subscription name
     * can be active on the same connection at the same time and names their links appropriately to distinguish
     * themselves from each other.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableAndVolatileSubsCoexistUsingDistinctLinkNames() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the durable shared receiver
            Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            session.createSharedDurableConsumer(dest, subscriptionName);

            // Attach the volatile shared receiver
            Matcher<?> volatileLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
            testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, volatileLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            session.createSharedConsumer(dest, subscriptionName);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that on a connection which doesn't identify as supporting shared subscriptions, the
     * attempt to create a shared durable subscriber fails if the link also doesn't identify as
     * supporting shared subscriptions.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedDurableTopicSubscriberFailsIfNotSupported() throws Exception {
        doSharedSubscriptionNotSupportedTestImpl(true, false);
    }

    /**
     * Verifies that on a connection which doesn't identify as supporting shared subscriptions, the
     * attempt to create a shared volatile subscriber fails if the link also doesn't identify as
     * supporting shared subscriptions.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedVolatileTopicSubscriberFailsIfNotSupported() throws Exception {
        doSharedSubscriptionNotSupportedTestImpl(false, false);
    }

    /**
     * Verifies that when a durable shared subscriber creation fails because the connection and link
     * both lack the capability indicating support, the subscriber is appropriately removed from
     * record, releasing use of the link name such that attempting another subscriber creation
     * uses the same initial link name.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedDurableTopicSubscriberFailsIfNotSupportedReleasesLinkName() throws Exception {
        doSharedSubscriptionNotSupportedTestImpl(true, true);
    }

    /**
     * Verifies that when a volatile shared subscriber creation fails because the connection and link
     * both lack the capability indicating support, the subscriber is appropriately removed from
     * record, releasing use of the link name such that attempting another subscriber creation
     * uses the same initial link name.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedVolatileTopicSubscriberFailsIfNotSupportedReleasesLinkName() throws Exception {
        doSharedSubscriptionNotSupportedTestImpl(false, true);
    }

    private void doSharedSubscriptionNotSupportedTestImpl(boolean durable, boolean repeat) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT include server connection capability to indicate support for shared-subs
            // This will cause the link capability to be desired, and we verify failure if not offered.
            Symbol[] serverCapabilities = new Symbol[]{};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            int iterations = repeat ? 2 : 1;

            // Expect a shared receiver to attach, then detach due to the link also not
            // reporting it offers the shared subs capability, i.e sharing not supported.
            for (int i = 0; i < iterations; i++) {
                try {
                    if (durable) {
                        Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
                        testPeer.expectSharedSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true, false, true, true, false);
                        testPeer.expectDetach(false, true, false);

                        session.createSharedDurableConsumer(dest, subscriptionName);
                    } else {
                        Matcher<?> volatileLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                        testPeer.expectSharedSubscriberAttach(topicName, subscriptionName, volatileLinkNameMatcher, false, false, true, true, false);
                        testPeer.expectDetach(true, true, true);

                        session.createSharedConsumer(dest, subscriptionName);
                    }

                    fail("Expected an exception to be thrown");
                } catch (JMSException jmse) {
                    // expected
                }
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Verifies that on a connection which doesn't identify as supporting shared subscriptions, the
     * attempt to create a shared durable subscriber succeeds if the server offers link capability for
     * shared subscriptions.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedDurableTopicSubscriberSucceedsWithOnlyLinkCapability() throws Exception {
        doSharedSubscriptionLinkCapabilitySupportedTestImpl(true);
    }

    /**
     * Verifies that on a connection which doesn't identify as supporting shared subscriptions, the
     * attempt to create a shared volatile subscriber succeeds if the server offers link capability for
     * shared subscriptions.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testCreateSharedVolatileTopicSubscriberSucceedsWithOnlyLinkCapability() throws Exception {
        doSharedSubscriptionLinkCapabilitySupportedTestImpl(false);
    }

    private void doSharedSubscriptionLinkCapabilitySupportedTestImpl(boolean durable) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT include server connection capability to indicate support for shared-subs.
            // This will cause the link capability to be desired, and we verify success if offered.
            Symbol[] serverCapabilities = new Symbol[]{};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Expect a shared receiver to attach, and succeed due to the server offering
            // the shared subs capability, i.e sharing is supported.
            if (durable) {
                Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true, false, true, true, true);
                testPeer.expectLinkFlow();

                session.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                Matcher<?> volatileLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedSubscriberAttach(topicName, subscriptionName, volatileLinkNameMatcher, false, false, true, true, true);
                testPeer.expectLinkFlow();

                session.createSharedConsumer(dest, subscriptionName);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared durable subscriber and exclusive durable subscriber with the same subscription name
     * can't be active on the same connection at the same time (shared first, then exclusive).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedAndExclusiveDurableSubsCantCoexistSharedFirst() throws Exception {
        doSharedAndExclusiveDurableSubsCantCoexistTestImpl(true);
    }

    /**
     * Verifies that a shared durable subscriber and exclusive durable subscriber with the same subscription name
     * can't be active on the same connection at the same time (exclusive first, then shared).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedAndExclusiveDurableSubsCantCoexistExclusiveFirst() throws Exception {
        doSharedAndExclusiveDurableSubsCantCoexistTestImpl(false);
    }

    private void doSharedAndExclusiveDurableSubsCantCoexistTestImpl(boolean sharedFirst) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            if(sharedFirst) {
                // Attach the durable shared receiver
                Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
                testPeer.expectLinkFlow();

                session.createSharedDurableConsumer(dest, subscriptionName);
            } else {
                // Attach the durable exclusive receiver
                testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.expectLinkFlow();

                session.createDurableConsumer(dest, subscriptionName);
            }

            testPeer.expectClose();

            try {
                if (sharedFirst) {
                    // Now try to attach a durable exclusive receiver
                    session.createDurableConsumer(dest, subscriptionName);
                } else {
                    // Now try to attach a durable shared receiver
                    session.createSharedDurableConsumer(dest, subscriptionName);
                }
                fail("Expected to fail due to concurrent shared + non-shared durable sub attempt");
            } catch (JMSException jmse) {
                // Expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    @Test(timeout = 20000)
    public void testExclusiveDurableSubCanOnlyBeActiveOnceAtATime() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the durable exclusive receiver
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1 = session.createDurableConsumer(dest, subscriptionName);

            try {
                // Now try to attach a second active durable exclusive receiver
                session.createDurableConsumer(dest, subscriptionName);
                fail("Expected to fail due to concurrent active subscriber attempt");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.expectDetach(false, true, false);
            subscriber1.close();

            // Now try to attach a new active durable exclusive receiver
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber2 = session.createDurableConsumer(dest, subscriptionName);
            assertNotNull(subscriber2);

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a upon failure to locate a subscription link during an
     * unsubscribe attempt an [InvalidDestination] exception is thrown.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testUnsubscribeNonExistingSubscription() throws Exception {
        doUnsubscribeNonExistingSubscriptionTestImpl(true);
    }

    /**
     * Verifies that a upon failure to locate a subscription link during an
     * unsubscribe attempt an [InvalidDestination] exception is thrown, but
     * this time using a connection without a ClientID set (and thus
     * expecting a different link name to be used).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testUnsubscribeNonExistingSubscriptionWithoutClientID() throws Exception {
        doUnsubscribeNonExistingSubscriptionTestImpl(false);
    }

    private void doUnsubscribeNonExistingSubscriptionTestImpl(boolean hasClientID) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection;
            if(hasClientID) {
                connection = testFixture.establishConnecton(testPeer);
            } else {
                connection = testFixture.establishConnectonWithoutClientID(testPeer, null);
            }
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String subscriptionName = "myInvalidSubscriptionName";
            // Try to unsubscribe non-existing subscription
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(true, false, subscriptionName, null, hasClientID);
            testPeer.expectDetach(true, true, true);

            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a InvalidDestinationException");
            } catch (InvalidDestinationException ide) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Verifies that a upon attempt to unsubscribe a subscription that is
     * currently in active use by a subscriber, an exception is thrown,
     * and that once the subscriber is closed a further unsubscribe attempt
     * is successfully undertaken.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testUnsubscribeExclusiveDurableSubWhileActiveThenInactive() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the durable exclusive receiver
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull("TopicSubscriber object was null", subscriber);

            // Now try to unsubscribe, should fail
            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
            }

            // Now close the subscriber
            testPeer.expectDetach(false, true, false);

            subscriber.close();

            // Try to unsubscribe again, should work now
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session.unsubscribe(subscriptionName);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Verifies that a upon attempt to unsubscribe a shared durable subscription that is
     * currently in active use by multiple subscribers, an exception is thrown, and that
     * only once the last subscriber is closed is an unsubscribe attempt successful.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testUnsubscribeSharedDurableSubWhileActiveThenInactive() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Attach the first durable shared receiver
            Matcher<?> linkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1 = session.createSharedDurableConsumer(dest, subscriptionName);

            // Attach the second durable shared receiver
            linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "2");
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber2 = session.createSharedDurableConsumer(dest, subscriptionName);

            testPeer.waitForAllHandlersToComplete(1000);

            // Now try to unsubscribe, should fail
            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            // Now close the first subscriber
            testPeer.expectDetach(false, true, false);
            subscriber1.close();

            testPeer.waitForAllHandlersToComplete(1000);

            // Try to unsubscribe again, should still fail
            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(1000);

            // Now close the second subscriber
            testPeer.expectDetach(false, true, false);

            subscriber2.close();

            // Try to unsubscribe again, should now work
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session.unsubscribe(subscriptionName);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a upon a failed attempt to create a shared subscriber that it
     * is possible to unsubscribe (not that it should be required), i.e that the
     * failed creation does not leave an incorrect recording of an active subscriber.
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testUnsubscribeAfterFailedCreation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Fail to attach a shared durable receiver
            Matcher<?> linkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true, true);
            testPeer.expectDetach(true, false, true);

            try {
                session.createSharedDurableConsumer(dest, subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            // Try to unsubscribe, should be able to (strictly speaking an unsub attempt
            // would probably fail normally, due to no subscription, but this test
            // doesn't care about that, just that the attempt proceeds, so overlook that.
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session.unsubscribe(subscriptionName);

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Verifies that subscriber cleanup occurs when the subscriber is remotely closed (after creation).
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testRemotelyDetachLinkWithDurableSharedConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);

            final CountDownLatch subscriberClosed = new CountDownLatch(1);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConsumerClosed(MessageConsumer consumer, Throwable exception) {
                    subscriberClosed.countDown();
                }
            });

            // Create first session
            testPeer.expectBegin();
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session1.createTopic(topicName);

            String subscriptionName = "mySubscription";

            // Attach the first shared receiver on the first session
            Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1 = session1.createSharedDurableConsumer(dest,  subscriptionName);

            // Create second session
            testPeer.expectBegin();
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Attach the second shared receiver on the second session
            durableLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "2");
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);

            testPeer.expectLinkFlow();

            // Then remotely detach the link after the flow is received.
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, false, AmqpError.RESOURCE_LIMIT_EXCEEDED, "TestingRemoteDetach");

            MessageConsumer subscriber2 = session2.createSharedDurableConsumer(dest,  subscriptionName);
            assertNotNull(subscriber2);

            assertTrue("Consumer closed callback didn't trigger", subscriberClosed.await(5, TimeUnit.SECONDS));

            testPeer.waitForAllHandlersToComplete(1000);

            // Now try to unsubscribe (using first session). It should fail due to sub still
            // being in use on the first session. No frames should be sent.
            try {
                session1.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            // Now close the first subscriber
            testPeer.expectDetach(false, true, false);
            subscriber1.close();

            testPeer.waitForAllHandlersToComplete(1000);

            // Try to unsubscribe again (using first session), should now work.
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session1.unsubscribe(subscriptionName);

            testPeer.expectClose();
            connection.close();
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that a shared durable subscriber creation using the same subscription
     * name but different topic than an existing subscriber can't be active on the
     * same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubChangeOfTopic() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(true, true, false);
    }

    /**
     * Verifies that a shared volatile subscriber creation using the same subscription
     * name but different topic than an existing subscriber can't be active on the
     * same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubChangeOfTopic() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(false, true, false);
    }

    /**
     * Verifies that a shared durable subscriber creation using the same subscription
     * name but different selector than an existing subscriber can't be active on the
     * same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubChangeOfSelector() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(true, false, true);
    }

    /**
     * Verifies that a shared volatile subscriber creation using the same subscription
     * name but different selector than an existing subscriber can't be active on the
     * same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubChangeOfSelector() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(false, false, true);
    }

    /**
     * Verifies that a shared durable subscriber creation using the same subscription
     * name but different topic and selector than an existing subscriber can't be active
     * on the same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedDurableSubChangeOfTopicAndSelector() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(true, true, true);
    }

    /**
     * Verifies that a shared volatile subscriber creation using the same subscription
     * name but different topic and selector than an existing subscriber can't be active
     * on the same connection at the same time fails (can't change details in use).
     *
     * @throws Exception if an unexpected exception occurs
     */
    @Test(timeout = 20000)
    public void testSharedVolatileSubChangeOfTopicAndSelector() throws Exception {
        doSharedSubChangeOfDetailsTestImpl(false, true, true);
    }

    private void doSharedSubChangeOfDetailsTestImpl(boolean durable, boolean changeTopic, boolean changeSelector) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName1 = "myTopic";
            String topicName2 = topicName1;
            if (changeTopic) {
                topicName2 += "2";
            }

            String noSelectorNull = null;
            String selector2 = null;
            if (changeSelector) {
                selector2 = "someProperty = 2";
            }

            Topic dest1 = session.createTopic(topicName1);
            Topic dest2 = session.createTopic(topicName2);

            String subscriptionName = "mySubscription";

            // Attach the first shared receiver
            if (durable) {
                Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedDurableSubscriberAttach(topicName1, subscriptionName, durableLinkNameMatcher, true);
            } else {
                Matcher<?> volatileLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName1, subscriptionName, volatileLinkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1;
            if (durable) {
                subscriber1 = session.createSharedDurableConsumer(dest1, subscriptionName, noSelectorNull);
            } else {
                subscriber1 = session.createSharedConsumer(dest1, subscriptionName, noSelectorNull);
            }

            testPeer.waitForAllHandlersToComplete(1000);

            // Now try to create another subscriber, same name but different topic. Should fail. No frames should be sent.
            try {
                if (durable) {
                    session.createSharedDurableConsumer(dest2, subscriptionName, selector2);
                } else {
                    session.createSharedConsumer(dest2, subscriptionName, selector2);
                }

                fail("Expected to fail due to attempting change of subscription details while subscriber is active");
            } catch (JMSException jmse) {
                // Expected
            }

            // Now close the first subscriber
            if (durable) {
                testPeer.expectDetach(false, true, false);
            } else {
                testPeer.expectDetach(true, true, true);
            }
            subscriber1.close();

            // Now try a new subscriber again, with changed topic, it should succeed (note also the verified reuse of link names).
            if (durable) {
                Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
                testPeer.expectSharedDurableSubscriberAttach(topicName2, subscriptionName, durableLinkNameMatcher, true);
            } else {
                Matcher<?> volatileLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "volatile1");
                testPeer.expectSharedVolatileSubscriberAttach(topicName2, subscriptionName, volatileLinkNameMatcher, true);
            }
            testPeer.expectLinkFlow();

            MessageConsumer subscriber2;
            if (durable) {
                subscriber2 = session.createSharedDurableConsumer(dest2, subscriptionName, selector2);
            } else {
                subscriber2 = session.createSharedConsumer(dest2, subscriptionName, selector2);
            }
            assertNotNull(subscriber2);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that subscriber cleanup occurs when the session it is on is remotely closed.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testRemotelyEndSessionWithDurableSharedConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);

            final CountDownLatch sessionClosed = new CountDownLatch(1);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            // Create first session
            testPeer.expectBegin();
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session1.createTopic(topicName);

            String subscriptionName = "mySubscription";

            // Attach the first shared receiver on the first session
            Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1 = session1.createSharedDurableConsumer(dest,  subscriptionName);

            // Create second session
            testPeer.expectBegin();
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Attach the second shared receiver on the second session
            durableLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "2");
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);

            testPeer.expectLinkFlow();

            // Then remotely end the session (and thus the subscriber along with it) after the flow is received.
            testPeer.remotelyEndLastOpenedSession(true, 0, AmqpError.RESOURCE_LIMIT_EXCEEDED, "TestingRemoteClosure");

            MessageConsumer subscriber2 = session2.createSharedDurableConsumer(dest,  subscriptionName);
            assertNotNull(subscriber2);

            assertTrue("Session closed callback didn't trigger", sessionClosed.await(5, TimeUnit.SECONDS));

            testPeer.waitForAllHandlersToComplete(1000);

            // Now try to unsubscribe (using first session, still open). It should fail due to sub still
            // being in use on the first session. No frames should be sent.
            try {
                session1.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            // Now close the first subscriber
            testPeer.expectDetach(false, true, false);
            subscriber1.close();

            testPeer.waitForAllHandlersToComplete(1000);

            // Try to unsubscribe again (using first session, still open), should now work.
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session1.unsubscribe(subscriptionName);

            testPeer.expectClose();
            connection.close();
        }
    }

    /**
     * Verifies that subscriber cleanup occurs when the session it is on is locally closed.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testLocallyEndSessionWithSharedConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);

            final CountDownLatch sessionClosed = new CountDownLatch(1);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            // Create first session
            testPeer.expectBegin();
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session1.createTopic(topicName);

            String subscriptionName = "mySubscription";

            // Attach the first shared receiver on the first session
            Matcher<?> durableLinkNameMatcher = equalTo(subscriptionName);
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber1 = session1.createSharedDurableConsumer(dest,  subscriptionName);

            // Create second session
            testPeer.expectBegin();
            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Attach the second shared receiver on the second session
            durableLinkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "2");
            testPeer.expectSharedDurableSubscriberAttach(topicName, subscriptionName, durableLinkNameMatcher, true);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber2 = session2.createSharedDurableConsumer(dest,  subscriptionName);
            assertNotNull(subscriber2);

            // Now close the second session (and thus the subscriber along with it).
            testPeer.expectEnd();
            session2.close();

            // Now try to unsubscribe (using first session, still open). It should fail due to sub still
            // being in use on the first session. No frames should be sent.
            try {
                session1.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            // Now close the first subscriber
            testPeer.expectDetach(false, true, false);
            subscriber1.close();

            testPeer.waitForAllHandlersToComplete(1000);

            // Try to unsubscribe again (using first session, still open), should now work.
            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session1.unsubscribe(subscriptionName);

            testPeer.expectClose();
            connection.close();
        }
    }

    // -------------------------------------- //

    /**
     * Verifies that subscription name passed is not allowed to have the subscription name
     * delimiter used in the receiver link naming to separate the subscription name from
     * a suffix used to ensure unique link names are used on a connection.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testSubscriptionNameNotAllowedToHaveNameSeparator() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            // Establish connection
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);

            // Create session
            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Try the various methods that take subscription name with a name that
            // contains the delimiter, they should fail. No frames should be sent.
            String subscriptionName = "thisName|hasTheDelimiterAlready";

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            try {
                session.createDurableSubscriber(dest, subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            try {
                session.createDurableConsumer(dest, subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            try {
                session.createSharedDurableConsumer(dest, subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            try {
                session.createSharedConsumer(dest, subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
                // Expected
            }

            testPeer.waitForAllHandlersToComplete(1000);

            // Now close connection, should work.
            testPeer.expectClose();
            connection.close();
        }
    }

    // -------------------------------------- //

    @Test(timeout = 20000)
    public void testSharedTopicSubscriberBehavesLikeNoClientIDWasSetWhenAwaitClientIdOptionIsFalse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int serverPort = testPeer.getServerPort();

            // Add server connection capability to indicate support for shared-subs
            Symbol[] serverCapabilities = new Symbol[]{SHARED_SUBS};

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(new Symbol[] { AmqpSupport.SOLE_CONNECTION_CAPABILITY }, serverCapabilities, null);
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + serverPort + "?jms.awaitClientID=false");
            Connection connection = factory.createConnection();

            // Verify that all handlers complete, i.e. the awaitClientID=false option
            // setting was effective in provoking the AMQP Open immediately even
            // though it has no ClientID and we haven't used the Connection.
            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            // Expect a shared subscription with 'global' capability and link name qualifier,
            // since this connection does not have a ClientID set.
            Matcher<?> linkNameMatcher = equalTo(subscriptionName + SUB_NAME_DELIMITER + "global-volatile1");
            testPeer.expectSharedVolatileSubscriberAttach(topicName, subscriptionName, linkNameMatcher, false);
            testPeer.expectLinkFlow();

            MessageConsumer subscriber = session.createSharedConsumer(dest, subscriptionName);

            testPeer.expectDetach(true, true, true);
            subscriber.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
