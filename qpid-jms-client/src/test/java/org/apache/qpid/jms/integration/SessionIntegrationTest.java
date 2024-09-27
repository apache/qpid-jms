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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DescribedTypeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

public class SessionIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionIntegrationTest.class);

    private static final int INDIVIDUAL_ACK = 101;

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    @Timeout(20)
    public void testCloseSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session, "Session should not be null");
            testPeer.expectEnd();
            testPeer.expectClose();

            session.close();

            // Should send nothing and throw no error.
            session.close();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCloseSessionTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            testPeer.expectBegin();
            testPeer.expectEnd(false);
            testPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(session, "Session should not be null");

            try {
                session.close();
                fail("Should have thrown an timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught exception: {}", jmsEx.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateProducer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testPeer.expectSenderAttach();
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            session.createProducer(queue);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateProducerLinkSupportedSourceOutcomes() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String queueName = "myQueue";

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));
            //TODO: what default outcome for producers?
            //Accepted normally, Rejected for transaction controller?
            //sourceMatcher.withDefaultOutcome(outcomeMatcher);

            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(queueName));

            testPeer.expectSenderAttach(sourceMatcher, targetMatcher, false, false);
            testPeer.expectClose();

            Queue queue = session.createQueue(queueName);
            session.createProducer(queue);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue);

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithEmptySelector() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withFilter(nullValue());

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue, "");
            assertNull(consumer.getMessageSelector());
            consumer = session.createConsumer(queue, "", false);
            assertNull(consumer.getMessageSelector());

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithNullSelector() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withFilter(nullValue());

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue, null);
            assertNull(consumer.getMessageSelector());
            consumer = session.createConsumer(queue, null, false);
            assertNull(consumer.getMessageSelector());

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithInvalidSelector() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic destination = session.createTopic(getTestName());

            try {
                session.createConsumer(destination, "3+5");
                fail("Should have thrown a invalid selector exception");
            } catch (InvalidSelectorException jmsse) {
                // Expected
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithSimpleSelector() throws Exception {
        doCreateConsumerWithSelectorTestImpl("myvar=42", false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithQuotedVariableSelector() throws Exception {
        doCreateConsumerWithSelectorTestImpl("\"my.quoted-var\"='some-value'", false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerWithInvalidSelectorAndDisableValidation() throws Exception {
        // Verifies that with the local validation disabled, the selector filter is still created
        // and sent on the source terminus, containing the desired non-JMS selector string.
        doCreateConsumerWithSelectorTestImpl("my.invalid-var > 'string-value'", true);
    }

    private void doCreateConsumerWithSelectorTestImpl(String messageSelector, boolean disableValidation) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            String options = null;
            if(disableValidation) {
                options = "jms.validateSelector=false";
            }

            Connection connection = testFixture.establishConnecton(testPeer, options);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Matcher<?> filterMapMatcher = hasEntry(equalTo(Symbol.valueOf("jms-selector")),
                    new DescribedTypeMatcher(UnsignedLong.valueOf(0x0000468C00000004L), Symbol.valueOf("apache.org:selector-filter:string"), equalTo(messageSelector)));

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withFilter(filterMapMatcher);

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue, messageSelector);

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateConsumerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateConsumerFailsWhenLinkRefusedTestImpl(true);
    }

    private void doCreateConsumerFailsWhenLinkRefusedTestImpl(boolean deferAttachResponseWrite) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            //Expect a link to a topic node, which we will then refuse
            SourceMatcher targetMatcher = new SourceMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectReceiverAttach(notNullValue(), targetMatcher, true, deferAttachResponseWrite);
            //Expect the detach response to the test peer closing the consumer link after refusal.
            testPeer.expectDetach(true, false, false);
            testPeer.expectClose();

            try {
                //Create a consumer, expect it to throw exception due to the link-refusal
                session.createConsumer(dest);
                fail("Consumer creation should have failed when link was refused");
            } catch(InvalidDestinationException ide) {
                //Expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerFailsWhenLinkRefusalResponseNotSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            ((JmsConnection) connection).setRequestTimeout(500);

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect a link to a topic node, which we will then refuse
            SourceMatcher targetMatcher = new SourceMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectReceiverAttach(notNullValue(), targetMatcher, false, true, true, false, null, null);
            testPeer.expectDetach(true, false, false);
            testPeer.expectClose();

            try {
                // Create a consumer, expect it to throw exception due to the link-refusal
                // even though there is no detach response.
                session.createConsumer(dest);
                fail("Consumer creation should have failed when link was refused");
            } catch(JmsOperationTimedOutException ex) {
                // Expected
                LOG.info("Caught expected error on consumer create: {}", ex.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateBrowserFailsWhenLinkRefusalResponseNotSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            ((JmsConnection) connection).setRequestTimeout(500);

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String queueName = "myQueue";
            Queue dest = session.createQueue(queueName);

            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), true, true, true, false, null, null);
            testPeer.expectDetach(true, false, false);
            testPeer.expectClose();

            try {
                // Create a QueueBrowser, expect it to throw exception due to the link-refusal
                // even though there is no detach response.
                QueueBrowser browser = session.createBrowser(dest);
                browser.getEnumeration();
                fail("Consumer creation should have failed when link was refused");
            } catch(JmsOperationTimedOutException ex) {
                // Expected
                LOG.info("Caught expected error on browser create: {}", ex.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, false);
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryQueueFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(false, true);
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryTopicFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(true, false);
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryTopicFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(true, true);
    }

    private void doCreateTemporaryDestinationFailsWhenLinkRefusedTestImpl(boolean topic, boolean deferAttachResponseWrite) throws Exception {

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            try {
                if (topic) {
                    testPeer.expectAndRefuseTempTopicCreationAttach(AmqpError.UNAUTHORIZED_ACCESS, "Not Authorized to create temp topics.", false);
                    //Expect the detach response to the test peer after refusal.
                    testPeer.expectDetach(true, false, false);

                    session.createTemporaryTopic();
                } else {
                    testPeer.expectAndRefuseTempQueueCreationAttach(AmqpError.UNAUTHORIZED_ACCESS, "Not Authorized to create temp queues.", false);
                    //Expect the detach response to the test peer after refusal.
                    testPeer.expectDetach(true, false, false);

                    session.createTemporaryQueue();
                }
                fail("Should have thrown security exception");
            } catch (JMSSecurityException jmsse) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);

            TemporaryQueue tempQueue = session.createTemporaryQueue();
            assertNotNull(tempQueue, "TemporaryQueue object was null");
            assertNotNull(tempQueue.getQueueName(), "TemporaryQueue queue name was null");
            assertEquals(dynamicAddress, tempQueue.getQueueName(), "TemporaryQueue name not as expected");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryQueueTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectTempQueueCreationAttach(null, false);
            testPeer.expectDetach(true, false, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            try {
                session.createTemporaryQueue();
                fail("Should have timed out on create.");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught expected exception: {}", jmsEx.getMessage());
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAndDeleteTemporaryQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);
            TemporaryQueue tempQueue = session.createTemporaryQueue();

            // Deleting the TemporaryQueue will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempQueue.delete();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testDeleteTemporaryQueueTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);
            TemporaryQueue tempQueue = session.createTemporaryQueue();

            // Deleting the TemporaryQueue will be achieved by closing its creating link.
            testPeer.expectDetach(true, false, true);

            try {
                tempQueue.delete();
                fail("Should have timed out waiting to delete.");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught expected exception: {}", jmsEx.getMessage());
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);

            TemporaryTopic tempTopic = session.createTemporaryTopic();
            assertNotNull(tempTopic, "TemporaryTopic object was null");
            assertNotNull(tempTopic.getTopicName(), "TemporaryTopic name was null");
            assertEquals(dynamicAddress, tempTopic.getTopicName(), "TemporaryTopic name not as expected");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateTemporaryTopicTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectTempTopicCreationAttach(null, false);
            testPeer.expectDetach(true, false, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            try {
                session.createTemporaryTopic();
                fail("Should have timed out on create.");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught expected exception: {}", jmsEx.getMessage());
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAndDeleteTemporaryTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            // Deleting the TemporaryTopic will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempTopic.delete();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testDeleteTemporaryTopicTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            // Deleting the TemporaryTopic will be achieved by closing its creating link.
            testPeer.expectDetach(true, false, true);

            try {
                tempTopic.delete();
                fail("Should have timed out waiting to delete.");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught expected exception: {}", jmsEx.getMessage());
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendToDeletedTemporaryTopicFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            testPeer.expectSenderAttach();

            MessageProducer producer = session.createProducer(tempTopic);

            // Deleting the TemporaryTopic will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempTopic.delete();

            try {
                producer.send(session.createMessage());
                fail("Should detect that the destination was deleted and fail");
            } catch (JMSException ignored) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testSendToDeletedTemporaryQueueFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);
            TemporaryQueue tempQueue = session.createTemporaryQueue();

            testPeer.expectSenderAttach();

            MessageProducer producer = session.createProducer(tempQueue);

            // Deleting the TemporaryTopic will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempQueue.delete();

            try {
                producer.send(session.createMessage());
                fail("Should detect that the destination was deleted and fail");
            } catch (JMSException ignored) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCannotDeleteTemporaryQueueInUse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);
            TemporaryQueue tempQueue = session.createTemporaryQueue();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            MessageConsumer consumer = session.createConsumer(tempQueue);

            try {
                tempQueue.delete();
                fail("Should not be able to delete an in use temp destination");
            } catch (JMSException ex) {
            }

            testPeer.expectDetach(true, true, true);
            consumer.close();

            // Deleting the TemporaryQueue will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempQueue.delete();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCannotDeleteTemporaryTopicInUse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            MessageConsumer consumer = session.createConsumer(tempTopic);

            try {
                tempTopic.delete();
                fail("Should not be able to delete an in use temp destination");
            } catch (JMSException ex) {
            }

            testPeer.expectDetach(true, true, true);
            consumer.close();

            // Deleting the TemporaryQueue will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempTopic.delete();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsQueueCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Queue.class, true);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsQueueCapabilityWithoutClientID() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Queue.class, false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTopicCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Topic.class, true);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTopicCapabilityWithoutClientID() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Topic.class, false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTempQueueCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryQueue.class, true);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTempQueueCapabilityWithoutClientID() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryQueue.class, false);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTempTopicCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryTopic.class, true);
    }

    @Test
    @Timeout(20)
    public void testCreateConsumerSourceContainsTempTopicCapabilityWithoutClientID() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryTopic.class, false);
    }

    private void doCreateConsumerSourceContainsCapabilityTestImpl(Class<? extends Destination> destType, boolean setClientID) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, setClientID);
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String destName = "myDest";
            Symbol nodeTypeCapability = null;

            Destination dest = null;
            if (destType == Queue.class) {
                dest = session.createQueue(destName);
                nodeTypeCapability = AmqpDestinationHelper.QUEUE_CAPABILITY;
            } else if (destType == Topic.class) {
                dest = session.createTopic(destName);
                nodeTypeCapability = AmqpDestinationHelper.TOPIC_CAPABILITY;
            } else if (destType == TemporaryQueue.class) {
                testPeer.expectTempQueueCreationAttach(destName);
                dest = session.createTemporaryQueue();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY;
            } else if (destType == TemporaryTopic.class) {
                testPeer.expectTempTopicCreationAttach(destName);
                dest = session.createTemporaryTopic();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY;
            } else {
                fail("unexpected type");
            }

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withCapabilities(arrayContaining(nodeTypeCapability));

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectClose();

            session.createConsumer(dest);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testConsumerNotAuthorized() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic destination = session.createTopic(topicName);

            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), false, true, false, false, AmqpError.UNAUTHORIZED_ACCESS, "Destination is not readable");
            testPeer.expectDetach(true, true, true);

            try {
                session.createConsumer(destination);
                fail("Should have thrown a security exception");
            } catch (JMSSecurityException jmsse) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testProducerNotAuthorized() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic destination = session.createTopic(topicName);

            testPeer.expectSenderAttach(notNullValue(), notNullValue(), true, false, true, 0L, AmqpError.UNAUTHORIZED_ACCESS, "Destination is not readable");
            testPeer.expectDetach(true, true, true);

            try {
                session.createProducer(destination);
                fail("Should have thrown a security exception");
            } catch (JMSSecurityException jmsse) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateProducerTargetContainsQueueCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(Queue.class);
    }

    @Test
    @Timeout(20)
    public void testCreateProducerTargetContainsTopicCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(Topic.class);
    }

    @Test
    @Timeout(20)
    public void testCreateProducerTargetContainsTempQueueCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(TemporaryQueue.class);
    }

    @Test
    @Timeout(20)
    public void testCreateProducerTargetContainsTempTopicCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(TemporaryTopic.class);
    }

    private void doCreateProducerTargetContainsCapabilityTestImpl(Class<? extends Destination> destType) throws JMSException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String destName = "myDest";
            Symbol nodeTypeCapability = null;

            Destination dest = null;
            if (destType == Queue.class) {
                dest = session.createQueue(destName);
                nodeTypeCapability = AmqpDestinationHelper.QUEUE_CAPABILITY;
            } else if (destType == Topic.class) {
                dest = session.createTopic(destName);
                nodeTypeCapability = AmqpDestinationHelper.TOPIC_CAPABILITY;
            } else if (destType == TemporaryQueue.class) {
                testPeer.expectTempQueueCreationAttach(destName);
                dest = session.createTemporaryQueue();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY;
            } else if (destType == TemporaryTopic.class) {
                testPeer.expectTempTopicCreationAttach(destName);
                dest = session.createTemporaryTopic();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY;
            } else {
                fail("unexpected type");
            }

            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withCapabilities(arrayContaining(nodeTypeCapability));

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectClose();

            session.createProducer(dest);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerTargetContainsNoTypeCapabilityWhenAnonymousRelayNodeIsSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            //Expect and accept a link to the anonymous relay node, check it has no type capability
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(nullValue());
            targetMatcher.withDynamic(nullValue());//default = false
            targetMatcher.withDurable(nullValue());//default = none/0
            targetMatcher.withCapabilities(nullValue());

            testPeer.expectSenderAttach(targetMatcher, false, false);

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull(producer, "Producer object was null");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerTargetContainsQueueCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Queue.class);
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerTargetContainsTopicCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Topic.class);
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerTargetContainsTempQueueCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(TemporaryQueue.class);
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerTargetContainsTempTopicCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(TemporaryQueue.class);
    }

    private void doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            //DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String destName = "myDest";
            Symbol nodeTypeCapability = null;

            Destination dest = null;
            if (destType == Queue.class) {
                dest = session.createQueue(destName);
                nodeTypeCapability = AmqpDestinationHelper.QUEUE_CAPABILITY;
            } else if (destType == Topic.class) {
                dest = session.createTopic(destName);
                nodeTypeCapability = AmqpDestinationHelper.TOPIC_CAPABILITY;
            } else if (destType == TemporaryQueue.class) {
                testPeer.expectTempQueueCreationAttach(destName);
                dest = session.createTemporaryQueue();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY;
            } else if (destType == TemporaryTopic.class) {
                testPeer.expectTempTopicCreationAttach(destName);
                dest = session.createTemporaryTopic();
                nodeTypeCapability = AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY;
            } else {
                fail("unexpected type");
            }

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull(producer, "Producer object was null");

            //Expect a new message sent by the above producer to cause creation of a new
            //sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(destName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));
            targetMatcher.withCapabilities(arrayContaining(nodeTypeCapability));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            Message message = session.createMessage();
            producer.send(dest, message);
            producer.close();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateDurableTopicSubscriber() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull(subscriber, "TopicSubscriber object was null");
            assertFalse(subscriber.getNoLocal(), "TopicSubscriber should not be no-local");
            assertNull(subscriber.getMessageSelector(), "TopicSubscriber should not have a selector");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithExistingDurableTopicSubscriberDoesNotCloseSubscriberLink() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull(subscriber, "TopicSubscriber object was null");

            testPeer.expectEnd();
            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateDurableConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            MessageConsumer consumer = session.createDurableConsumer(dest, subscriptionName);
            assertNotNull(consumer, "MessageConsumer object was null");
            assertNull(consumer.getMessageSelector(), "MessageConsumer should not have a selector");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testDurableSubscriptionUnsubscribeInUseThrowsJMSEx() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull(subscriber, "TopicSubscriber object was null");

            try {
                session.unsubscribe(subscriptionName);
                fail("Should have thrown a JMSException");
            } catch (JMSException ex) {
            }

            testPeer.expectDetach(false, true, false);

            subscriber.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateDurableTopicSubscriberFailsIfConnectionDoesntHaveExplicitClientID() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection without an explicit clientId
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            try {
                // Verify this fails, a clientID is required and only one chosen by the application makes sense
                session.createDurableSubscriber(dest, subscriptionName);
                fail("expected exception to be thrown due to lack of explicit clientID");
            } catch(IllegalStateException ise) {
                // Expected
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }


    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerWhenAnonymousRelayNodeIsSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            //Expect and accept a link to the anonymous relay node
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(nullValue());
            targetMatcher.withDynamic(nullValue());//default = false
            targetMatcher.withDurable(nullValue());//default = none/0

            testPeer.expectSenderAttach(targetMatcher, false, false);

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull(producer, "Producer object was null");

            //Expect a new message sent with this producer to use the link to the anonymous relay matched above
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            producer.send(dest, message);

            //Repeat the send and observe another transfer on the existing link.
            testPeer.expectTransfer(messageMatcher);

            producer.send(dest, message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(true);
    }

    private void doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(boolean deferAttachFrameWrite) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            //Expect and refuse a link to the anonymous relay node
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(nullValue());
            targetMatcher.withDynamic(nullValue());//default = false
            targetMatcher.withDurable(nullValue());//default = none/0

            testPeer.expectSenderAttach(targetMatcher, true, false);
            //Expect the detach response to the test peer closing the producer link after refusal.
            testPeer.expectDetach(true, false, false);
            testPeer.expectClose();

            try {
                session.createProducer(null);
                fail("Expected producer creation to fail if anonymous-relay link refused");
            } catch (JMSException jmse) {
                //expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test
    @Timeout(20)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(true);
    }

    private void doCreateProducerFailsWhenLinkRefusedTestImpl(boolean deferAttachResponseWrite) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            //Expect a link to a topic node, which we will then refuse
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectSenderAttach(targetMatcher, true, deferAttachResponseWrite);
            //Expect the detach response to the test peer closing the producer link after refusal.
            testPeer.expectDetach(true, false, false);
            testPeer.expectClose();

            try {
                //Create a producer, expect it to throw exception due to the link-refusal
                session.createProducer(dest);
                fail("Producer creation should have failed when link was refused");
            } catch(InvalidDestinationException ide) {
                //Expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateProducerFailsWhenLinkRefusedNoDetachSent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            ((JmsConnection) connection).setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect a link to a topic node, which we will then refuse
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            testPeer.expectSenderAttach(notNullValue(), targetMatcher, true, true, false, 0, null, null);
            // Expect the detach response to the test peer closing the producer link after refusal.
            testPeer.expectDetach(true, false, false);

            try {
                // Create a producer, expect it to throw exception due to the link-refusal
                session.createProducer(dest);
                fail("Producer creation should have failed when link was refused");
            } catch(JmsOperationTimedOutException ex) {
                // Expected
                LOG.info("Caught expected exception on create: {}", ex.getMessage());
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateAnonymousProducerWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            // Configure for a known state such that no fallback producers are cached.
            Connection connection = testFixture.establishConnecton(testPeer,
                "?amqp.anonymousFallbackCacheSize=0&amqp.anonymousFallbackCacheTimeout=0");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            // Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull(producer, "Producer object was null");

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            Message message = session.createMessage();
            producer.send(dest, message);

            // Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testIncomingMessageExceedsMaxRedeliveries() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int COUNT = 5;

            Connection connection = testFixture.establishConnecton(testPeer, "?jms.redeliveryPolicy.maxRedeliveries=1");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            HeaderDescribedType header = new HeaderDescribedType();
            header.setDeliveryCount(new UnsignedInteger(2));

            testPeer.expectReceiverAttach();
            // Send some messages that have exceeded the specified re-delivery count
            testPeer.expectLinkFlowRespondWithTransfer(header, null, null, null, new AmqpValueDescribedType("redelivered-content"), COUNT);
            // Send a message that has not exceeded the delivery count
            String expectedContent = "not-redelivered";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(expectedContent), COUNT + 1);

            for (int i = 0; i < COUNT; i++) {
                // Then expect an *settled* Modified disposition that rejects each message once
                ModifiedMatcher modified = new ModifiedMatcher();
                modified.withDeliveryFailed(equalTo(true));
                modified.withUndeliverableHere(equalTo(true));
                testPeer.expectDisposition(true, modified);
            }

            // Then expect an Accepted disposition for the good message
            testPeer.expectDisposition(true, new AcceptedMatcher());

            final MessageConsumer consumer = session.createConsumer(queue);

            Message m = consumer.receive(6000);
            assertNotNull(m, "Should have reiceved the final message");
            assertTrue(m instanceof TextMessage, "Should have received the final message");
            assertEquals(expectedContent, ((TextMessage)m).getText(), "Unexpected content");

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test
    @Timeout(20)
    public void testPrefetchPolicyInfluencesCreditFlow() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int newPrefetch = 263;
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + newPrefetch);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(newPrefetch)));

            session.createConsumer(queue);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testLocallyCloseSessionWithConsumersAndProducers() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create some consumers, don't give them any messages
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue);
            session.createConsumer(queue);

            // Create some producers
            testPeer.expectSenderAttach();
            testPeer.expectSenderAttach();

            session.createProducer(queue);
            session.createProducer(queue);

            //Expect the session close
            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionWithProducers() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch sessionClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Create a producer
            testPeer.expectSenderAttach();
            final MessageProducer producer = session.createProducer(queue);
            assertNotNull(producer);

            // Create a second producer, then remotely end the session afterwards.
            testPeer.expectSenderAttach();
            testPeer.remotelyEndLastOpenedSession(true, 50, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            final MessageProducer producer2 = session.createProducer(queue);

            testPeer.waitForAllHandlersToComplete(1000);

            // Verify the producers get marked closed
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        producer.getDestination();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 6000, 10), "producer never closed.");

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        producer2.getDestination();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 6000, 10), "producer2 never closed.");

            assertTrue(sessionClosed.await(10, TimeUnit.SECONDS), "Session closed callback didn't trigger");

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_DELETED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Try closing producers explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything unexpected.
            producer.close();
            producer2.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionWithProducerSendWaitingForCredit() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, don't give it credit.
            testPeer.expectSenderAttachWithoutGrantingCredit();

            // Producer has no credit so the send should block waiting for it.
            testPeer.remotelyEndLastOpenedSession(true, 50, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull(jmse.getMessage(), "Expected exception to have a message");
                assertTrue(jmse.getMessage().contains(BREAD_CRUMB), "Expected breadcrumb to be present in message");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionWithProducerCompletesAsyncSends() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch sessionClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end the session afterwards.
            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            final int MSG_COUNT = 3;

            for (int i = 0; i < MSG_COUNT; ++i) {
                testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            }

            testPeer.remotelyEndLastOpenedSession(true, 0, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            TestJmsCompletionListener listener = new TestJmsCompletionListener(MSG_COUNT);
            try {
                for (int i = 0; i < MSG_COUNT; ++i) {
                    Message message = session.createTextMessage("content");
                    producer.send(message, listener);
                }
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            testPeer.waitForAllHandlersToComplete(2000);

            // Verify the producer gets marked closed
            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertEquals(MSG_COUNT, listener.errorCount);
            assertEquals(0, listener.successCount);

            assertTrue(sessionClosed.await(10, TimeUnit.SECONDS), "Session closed callback didn't trigger");

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String errorMessage = jmsise.getCause().getMessage();
                assertTrue(errorMessage.contains(AmqpError.RESOURCE_DELETED.toString()));
                assertTrue(errorMessage.contains(BREAD_CRUMB));
            }

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionWithConsumers() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch sessionClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            final MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull(consumer);

            // Create a second consumer, then remotely end the session afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyEndLastOpenedSession(true, 0, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            final MessageConsumer consumer2 = session.createConsumer(queue);

            // Verify the consumers get marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 6000, 10), "consumer never closed.");

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        consumer2.getMessageListener();
                    } catch (IllegalStateException jmsise) {
                        if (jmsise.getCause() != null) {
                            String message = jmsise.getCause().getMessage();
                            return message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                   message.contains(BREAD_CRUMB);
                        } else {
                            return false;
                        }
                    }
                    return false;
                }
            }, 6000, 10), "consumer2 never closed.");

            assertTrue(sessionClosed.await(10, TimeUnit.SECONDS), "Session closed callback didn't trigger");

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_DELETED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Try closing consumers explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything unexpected.
            consumer.close();
            consumer2.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithConsumerThatRemoteDetaches() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            // Create a consumer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            // Then locally close the session, provoke a remote-detach when the end reaches the
            // test peer, followed by the session end 'response'. The test peer should not
            // expect a reply to the detach, as the session was already ended at the client.
            testPeer.expectEnd(false);
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(false, true);
            testPeer.remotelyEndLastOpenedSession(false);

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue);

            session.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithConsumerThatRemoteDetachesWithUnackedMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, don't give it any messages
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue);

            //Expect the session close
            testPeer.expectEnd(false);
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType("content"), 1);
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(false, true);
            testPeer.remotelyEndLastOpenedSession(false, 200);

            session.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testSessionHasExpectedDefaultOutgoingWindow() throws Exception {
        doSessionHasExpectedOutgoingWindowTestImpl(Integer.MAX_VALUE, null);
    }

    @Test
    @Timeout(20)
    public void testSessionHasExpectedConfiguredOutgoingWindow() throws Exception {
        int windowSize = 13579;
        doSessionHasExpectedOutgoingWindowTestImpl(windowSize, "?amqp.sessionOutgoingWindow=" + windowSize);
    }

    private void doSessionHasExpectedOutgoingWindowTestImpl(int value, String options) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, options);

            testPeer.expectBegin(equalTo(UnsignedInteger.valueOf(value)), true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertNotNull(session, "Session should not be null");

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testAsyncDeliveryOrder() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            // Create a consumer, don't expect any flow as the connection is stopped
            testPeer.expectReceiverAttach();

            int messageCount = 10;
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                    messageCount, false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, true);

            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(3000);

            for (int i = 1; i <= messageCount; i++) {
                // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                //TODO: could also match on delivery ID's
                testPeer.expectDisposition(true, stateMatcher);
            }

            final CountDownLatch done = new CountDownLatch(messageCount);
            final AtomicInteger index = new AtomicInteger(-1);

            consumer.setMessageListener(new DeliveryOrderListener(done, index));

            testPeer.waitForAllHandlersToComplete(3000);
            assertTrue(done.await(10, TimeUnit.SECONDS), "Not all messages received in given time");
            assertEquals(messageCount - 1, index.get(), "Messages were not in expected order, final index was wrong");

            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private static class DeliveryOrderListener implements MessageListener {
        private final CountDownLatch done;
        private final AtomicInteger index;

        private DeliveryOrderListener(CountDownLatch done, AtomicInteger index) {
            this.done = done;
            this.index = index;
        }

        @Override
        public void onMessage(Message message) {
            try {
                int messageNumber = message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER);

                LOG.info("Listener received message: {}", messageNumber);

                index.compareAndSet(messageNumber - 1, messageNumber);

                done.countDown();
            } catch (Exception e) {
                LOG.error("Caught exception in listener", e);
            }
        }
    }

    @Test
    @Timeout(20)
    public void testSessionSnapshotsPolicyObjects() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            JmsSession session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertNotSame(session.getMessageIDPolicy(), connection.getMessageIDPolicy());
            assertNotSame(session.getPrefetchPolicy(), connection.getPrefetchPolicy());
            assertNotSame(session.getPresettlePolicy(), connection.getPresettlePolicy());
            assertNotSame(session.getRedeliveryPolicy(), connection.getRedeliveryPolicy());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testAcknowledgeIndividualMessages()  throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(INDIVIDUAL_ACK);
            Queue queue = session.createQueue("myQueue");

            int msgCount = 5;
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(null), msgCount, false, false,
                    Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(msgCount)), 1, false, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);

            List<Message> messages = new ArrayList<>();
            Message lastReceivedMessage = null;
            for (int i = 0; i < msgCount; i++) {
                lastReceivedMessage = messageConsumer.receive(3000);
                assertNotNull(lastReceivedMessage, "Message " + i + " was not received");
                messages.add(lastReceivedMessage);

                assertEquals(i, lastReceivedMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER), "unexpected message number property");
            }

            // Acknowledge the messages in a random order, verify only that messages disposition arrives each time.
            Random rand = new Random();
            for (int i = 0; i < msgCount; i++) {
                Message msg = messages.remove(rand.nextInt(msgCount - i));

                int deliveryNumber =  msg.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1;

                testPeer.expectDisposition(true, new AcceptedMatcher(), deliveryNumber, deliveryNumber);

                msg.acknowledge();

                testPeer.waitForAllHandlersToComplete(3000);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        private final CountDownLatch completed;

        public volatile int successCount;
        public volatile int errorCount;

        public TestJmsCompletionListener(int expected) {
            completed = new CountDownLatch(expected);
        }

        public boolean hasCompleted() {
            return completed.getCount() == 0;
        }

        public boolean awaitCompletion(long timeout, TimeUnit units) throws InterruptedException {
            return completed.await(timeout, units);
        }

        @Override
        public void onCompletion(Message message) {
            LOG.info("JmsCompletionListener onCompletion called with message: {}", message);
            successCount++;
            completed.countDown();
        }

        @Override
        public void onException(Message message, Exception exception) {
            LOG.info("JmsCompletionListener onException called with message: {} error {}", message, exception);
            errorCount++;
            completed.countDown();
        }
    }

    @Test
    @Timeout(20)
    public void testCloseConsumerWithUnackedClientAckMessagesThenRecoverSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);

            int msgCount = 2;
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), msgCount, false, false,
                    Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(msgCount)), 1, false, true);

            Queue destination = session.createQueue(getTestName());
            MessageConsumer consumer = session.createConsumer(destination);

            TextMessage receivedTextMessage = null;
            assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected a message");
            assertEquals(1,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");
            assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected a message");
            assertEquals(2,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");

            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH - msgCount)));

            consumer.close();

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 1, 1);
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 2, 2);
            testPeer.expectDetach(true, true, true);

            session.recover();

            // Verify the expectations happen in response to the recover() and not the following close().
            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRecoveredClientAckSessionWithDurableSubscriber() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, "?jms.clientID=myClientId", null, null, false);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);

            String subscriptionName = "mySubName";
            String topicName = "myTopic";
            Topic topic = session.createTopic(topicName);

            int msgCount = 3;
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), msgCount, false, false,
                    Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(msgCount)), 1, false, true);

            MessageConsumer subscriber = session.createDurableConsumer(topic, subscriptionName);

            TextMessage receivedTextMessage = null;
            assertNotNull(receivedTextMessage = (TextMessage) subscriber.receive(3000), "Expected a message");
            assertEquals(1,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");
            assertNotNull(receivedTextMessage = (TextMessage) subscriber.receive(3000), "Expected a message");
            assertEquals(2,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");

            session.recover();

            assertNotNull(receivedTextMessage = (TextMessage) subscriber.receive(3000), "Expected a message");
            int deliveryNumber = receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1;
            assertEquals(1,  deliveryNumber,  "Unexpected delivery number");

            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            receivedTextMessage.acknowledge();

            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 2, 2);
            testPeer.expectDetach(false, true, false);
            testPeer.expectDisposition(true, new ReleasedMatcher(), 3, 3);

            subscriber.close();

            testPeer.waitForAllHandlersToComplete(1000);

            testPeer.expectDurableSubUnsubscribeNullSourceLookup(false, false, subscriptionName, topicName, true);
            testPeer.expectDetach(true, true, true);

            session.unsubscribe(subscriptionName);

            testPeer.expectClose();

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithWithUnackedClientAckMessages() throws Exception {
        doCloseWithWithUnackedClientAckMessagesTestImpl(true);
    }

    @Test
    @Timeout(20)
    public void testCloseConnectionWithUnackedClientAckMessages() throws Exception {
        doCloseWithWithUnackedClientAckMessagesTestImpl(false);
    }

    private void doCloseWithWithUnackedClientAckMessagesTestImpl(boolean closeSession) throws JMSException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, "?jms.clientID=myClientId", null, null, false);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);

            String subscriptionName = "mySubName";
            String topicName = "myTopic";
            Topic topic = session.createTopic(topicName);

            int msgCount = 2;
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), msgCount, false, false,
                    Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(msgCount)), 1, false, true);

            MessageConsumer subscriber = session.createDurableConsumer(topic, subscriptionName);

            TextMessage receivedTextMessage = null;
            assertNotNull(receivedTextMessage = (TextMessage) subscriber.receive(3000), "Expected a message");
            assertEquals(1,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");
            assertNotNull(receivedTextMessage = (TextMessage) subscriber.receive(3000), "Expected a message");
            assertEquals(2,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");

            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 1, 1);
            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 2, 2);

            if(closeSession) {
                testPeer.expectEnd();

                session.close();
            }

            testPeer.expectClose();

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testCloseConnectionWithRecoveredUndeliveredAndRedeliveredClientAckMessages() throws Exception {
        // Send 6, recover 4, redeliver 2, close connection (and so implicitly, session)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(false, false, 6, 4, 2);
    }

    @Test
    @Timeout(20)
    public void testCloseConnectionWithRecoveredUndeliveredClientAckMessages() throws Exception {
        // Send 4, recover 2, redeliver none, close connection (and so implicitly, session)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(false, false, 4, 2, 0);
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithRecoveredUndeliveredAndRedeliveredClientAckMessages() throws Exception {
        // Send 6, recover 4, redeliver 2, close session (then connection)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(false, true, 6, 4, 2);
    }

    @Test
    @Timeout(20)
    public void testCloseSessionWithRecoveredUndeliveredClientAckMessages() throws Exception {
        // Send 4, recover 2, redeliver none, close session (then connection)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(false, true, 4, 2, 0);
    }

    @Test
    @Timeout(20)
    public void testCloseConsumerWithRecoveredUndeliveredAndRedeliveredClientAckMessages() throws Exception {
        // Send 6, recover 4, redeliver 2, close consumer then connection (and so implicitly, session)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(true, false, 6, 4, 2);
    }

    @Test
    @Timeout(20)
    public void testCloseConsumerWithRecoveredUndeliveredClientAckMessages() throws Exception {
        // Send 4, recover 2, redeliver none, close consumer then connection (and so implicitly, session)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(true, false, 4, 2, 0);
    }

    @Test
    @Timeout(20)
    public void testCloseConsumerAndSessionWithRecoveredAndRedeliveredUndeliveredClientAckMessages() throws Exception {
        // Send 6, recover 4, redeliver 2, close consumer then session (then connection)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(true, true, 6, 4, 2);
    }

    @Test
    @Timeout(20)
    public void testCloseConsumerAndSessionWithRecoveredUndeliveredClientAckMessages() throws Exception {
        // Send 6, recover 4, redeliver 2, close consumer then session (then connection)
        doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(true, true, 4, 2, 0);
    }

    private void doCloseWithWithRecoveredUndeliveredClientAckMessagesTestImpl(
            boolean closeConsumer, boolean closeSession, int msgCount, int deliverBeforeRecoverCount, int deliverAfterRecoverCount) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic topic = session.createTopic(topicName);

            final CountDownLatch incoming = new CountDownLatch(msgCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    incoming.countDown();
                }
            });

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), msgCount, false, false,
                    equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            MessageConsumer consumer = session.createConsumer(topic);

            TextMessage receivedTextMessage = null;
            for (int i = 1; i <= deliverBeforeRecoverCount; i++) {
                assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected message did not arrive: " + i);
                assertEquals(i,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");
            }

            // Await all incoming messages to arrive at consumer before we recover, ensure deterministic test behaviour.
            assertTrue(incoming.await(3, TimeUnit.SECONDS), "Messages did not arrive in a timely fashion");

            session.recover();

            testPeer.waitForAllHandlersToComplete(1000);

            for (int i = 1; i <= deliverAfterRecoverCount; i++) {
                assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected message did not arrive after recover: " + i);
                assertEquals(i,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number after recover");
            }

            int deliveredAtAnyPoint = Math.max(deliverBeforeRecoverCount, deliverAfterRecoverCount);

            if (closeConsumer) {
                if(deliverAfterRecoverCount > 0) {
                    // Remaining credit will be drained if there are delivered messages yet to be acknowledged or recovered again.
                    testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH - msgCount)));
                }

                // Any message delivered+recovered before but not then delivered again afterwards, will have disposition sent now.
                for (int i = deliverAfterRecoverCount + 1; i <= deliverBeforeRecoverCount; i++) {
                    testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), i, i);
                }

                if(deliverAfterRecoverCount > 0) {
                    // Any further remaining messages prefetched will be released.
                    for (int i = deliveredAtAnyPoint + 1; i <= msgCount; i++) {
                        testPeer.expectDisposition(true, new ReleasedMatcher(), i, i);
                    }
                } else {
                    // The link will close now
                    testPeer.expectDetach(true, true, true);

                    // Dispositions sent by proton when the link is freed
                    for (int i = deliveredAtAnyPoint + 1; i <= msgCount; i++) {
                        testPeer.expectDisposition(true, new ReleasedMatcher(), i, i);
                    }
                }

                consumer.close();

                testPeer.waitForAllHandlersToComplete(1000);

                if(deliverAfterRecoverCount > 0) {
                    // When the session or connection is closed, outstanding delivered messages will have disposition sent.
                    for (int i = 1; i <= deliverAfterRecoverCount; i++) {
                        testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), i, i);
                    }
                    testPeer.expectDetach(true, true, true);
                }
            } else {
                // If we dont close the consumer first, all previously delivered messages will have
                // disposition sent when the session or connection is closed.
                for (int i = 1; i <= deliveredAtAnyPoint; i++) {
                    testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), i, i);
                }
            }

            if(closeSession) {
                testPeer.expectEnd();

                session.close();

                testPeer.waitForAllHandlersToComplete(1000);
            }

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testAcknowledgeAllPreviouslyRecoveredClientAckMessages() throws Exception {
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(true, false, true);
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(false, true, true);
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(false, false, true);
    }

    @Test
    @Timeout(20)
    public void testAcknowledgeSomePreviouslyRecoveredClientAckMessages() throws Exception {
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(true, false, false);
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(false, true, false);
        doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(false, false, false);
    }

    private void doAcknowledgePreviouslyRecoveredClientAckMessagesTestImpl(boolean closeConsumer, boolean closeSession, boolean consumeAllRecovered) throws JMSException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, "?jms.clientID=myClientId", null, null, false);
            connection.start();

            int msgCount = 7;
            int deliverBeforeRecoverCount = 4;
            int acknowledgeAfterRecoverCount = consumeAllRecovered ? 5 : 2;

            testPeer.expectBegin();

            Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic topic = session.createTopic(topicName);

            final CountDownLatch incoming = new CountDownLatch(msgCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    incoming.countDown();
                }
            });

            testPeer.expectReceiverAttach();

            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), msgCount, false, false,
                    equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            MessageConsumer consumer = session.createConsumer(topic);

            TextMessage receivedTextMessage = null;
            for (int i = 1; i <= deliverBeforeRecoverCount; i++) {
                assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected message did not arrive: " + i);
                assertEquals(i,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number");
            }

            // Await all incoming messages to arrive at consumer before we recover, ensure deterministic test behaviour.
            assertTrue(incoming.await(3, TimeUnit.SECONDS), "Messages did not arrive in a timely fashion");

            session.recover();

            testPeer.waitForAllHandlersToComplete(1000);

            for (int i = 1; i <= acknowledgeAfterRecoverCount; i++) {
                assertNotNull(receivedTextMessage = (TextMessage) consumer.receive(3000), "Expected message did not arrive after recover: " + i);
                assertEquals(i,  receivedTextMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1,  "Unexpected delivery number after recover");
                testPeer.expectDisposition(true, new AcceptedMatcher(), i, i);
                receivedTextMessage.acknowledge();
            }

            testPeer.waitForAllHandlersToComplete(1000);

            if(!consumeAllRecovered) {
                // Any message delivered+recovered before but not then delivered and acknowledged afterwards, will have
                // disposition sent as consumer/session/connection is closed.
                for (int i = acknowledgeAfterRecoverCount + 1; i <= deliverBeforeRecoverCount; i++) {
                    testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), i, i);
                }
            }

            if(closeConsumer) {
                testPeer.expectDetach(true,  true,  true);

                // Dispositions sent by proton when the link is freed
                for (int i = Math.max(deliverBeforeRecoverCount, acknowledgeAfterRecoverCount) + 1; i <= msgCount; i++) {
                    testPeer.expectDisposition(true, new ReleasedMatcher(), i, i);
                }

                consumer.close();
            }

            if(closeSession) {
                testPeer.expectEnd();

                session.close();
            }

            testPeer.expectClose();

            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionWithMessageListener() throws Exception {
        final String BREAD_CRUMB = "ErrorDescriptionBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");

            final CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            final AtomicReference<JMSException> asyncError = new AtomicReference<JMSException>();
            connection.setExceptionListener(ex -> {
                asyncError.compareAndSet(null, ex);
                exceptionListenerFired.countDown();
            });

            final CountDownLatch sessionClosed = new CountDownLatch(1);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer
            testPeer.expectReceiverAttach();
            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect credit to be sent when listener is added, then remotely close the session.
            testPeer.expectLinkFlow();
            testPeer.remotelyEndLastOpenedSession(true, 0, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            consumer.setMessageListener(m -> {
                // No-op
            });

            // Verify ExceptionListener fired
            assertTrue(exceptionListenerFired.await(5, TimeUnit.SECONDS), "ExceptionListener did not fire");

            JMSException jmsException = asyncError.get();
            assertNotNull(jmsException, "Exception from listener was not set");
            String message = jmsException.getMessage();
            assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()) && message.contains(BREAD_CRUMB));

            // Verify the session (and  consumer) got marked closed and listener fired
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue(sessionClosed.await(5, TimeUnit.SECONDS), "Session closed callback did not fire");
            assertTrue(verifyConsumerClosure(BREAD_CRUMB, consumer), "consumer never closed.");
            assertTrue(verifySessionClosure(BREAD_CRUMB, session), "session never closed.");

            // Try closing consumer and session explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything unexpected.
            consumer.close();
            session.close();

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test
    @Timeout(20)
    public void testRemotelyEndSessionCompletesAsyncSends() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch sessionClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onSessionClosed(Session session, Throwable exception) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end the session afterwards.
            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            final int MSG_COUNT = 3;

            for (int i = 0; i < MSG_COUNT; ++i) {
                testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            }

            TestJmsCompletionListener listener = new TestJmsCompletionListener(MSG_COUNT);
            try {
                for (int i = 0; i < MSG_COUNT; ++i) {
                    Message message = session.createTextMessage("content");
                    producer.send(message, listener);
                }
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            testPeer.waitForAllHandlersToComplete(2000);

            assertFalse(listener.hasCompleted());

            testPeer.expectSenderAttach();
            testPeer.remotelyEndLastOpenedSession(true, 50, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            session.createProducer(queue);

            // Verify the session gets marked closed
            assertTrue(sessionClosed.await(5, TimeUnit.SECONDS), "Session closed callback didn't trigger");

            try {
                producer.getDeliveryMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String errorMessage = jmsise.getCause().getMessage();
                assertTrue(errorMessage.contains(AmqpError.RESOURCE_DELETED.toString()));
                assertTrue(errorMessage.contains(BREAD_CRUMB));
            }

            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertEquals(MSG_COUNT, listener.errorCount); // All sends should have been failed

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private boolean verifyConsumerClosure(final String BREAD_CRUMB, final MessageConsumer consumer) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                try {
                    consumer.getMessageListener();
                } catch (IllegalStateException jmsise) {
                    if (jmsise.getCause() != null) {
                        String message = jmsise.getCause().getMessage();
                        return message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()) &&
                                message.contains(BREAD_CRUMB);
                    } else {
                        return false;
                    }
                }
                return false;
            }
        }, 5000, 10);
    }

    private boolean verifySessionClosure(final String BREAD_CRUMB, final Session session) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                try {
                    session.getTransacted();
                } catch (IllegalStateException jmsise) {
                    if (jmsise.getCause() != null) {
                        String message = jmsise.getCause().getMessage();
                        return message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()) &&
                                message.contains(BREAD_CRUMB);
                    } else {
                        return false;
                    }
                }
                return false;
            }
        }, 5000, 10);
    }
}
