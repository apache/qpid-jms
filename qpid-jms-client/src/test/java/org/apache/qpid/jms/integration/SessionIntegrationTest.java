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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConnectionProperties;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declare;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declared;
import org.apache.qpid.jms.test.testpeer.describedtypes.Discharge;
import org.apache.qpid.jms.test.testpeer.describedtypes.Modified;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransactionalState;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Ignore;
import org.junit.Test;

public class SessionIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 5000)
    public void testCloseSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Session should not be null", session);
            testPeer.expectEnd();
            session.close();
        }
    }

    @Test(timeout = 5000)
    public void testCreateProducer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            session.createProducer(queue);
        }
    }

    @Test(timeout = 5000)
    public void testCreateProducerLinkSupportsAcceptedAndRejectedOutcomes() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String queueName = "myQueue";

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL));
            //TODO: what default outcome for producers?
            //Accepted normally, Rejected for transaction controller?
            //sourceMatcher.withDefaultOutcome(outcomeMatcher);

            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(queueName));

            testPeer.expectSenderAttach(sourceMatcher, targetMatcher, false, false);

            Queue queue = session.createQueue(queueName);
            session.createProducer(queue);
        }
    }

    @Test(timeout = 5000)
    public void testCreateConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = session.createQueue("myQueue");
            session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateTemporaryQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);

            TemporaryQueue tempQueue = session.createTemporaryQueue();
            assertNotNull("TemporaryQueue object was null", tempQueue);
            assertNotNull("TemporaryQueue queue name was null", tempQueue.getQueueName());
            assertEquals("TemporaryQueue name not as expected", dynamicAddress, tempQueue.getQueueName());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Ignore // Need to complete implementation and update test peer link handle behaviour
    @Test(timeout = 5000)
    public void testCreateAndDeleteTemporaryQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempQueueAddress";
            testPeer.expectTempQueueCreationAttach(dynamicAddress);
            TemporaryQueue tempQueue = session.createTemporaryQueue();

            // Deleting the TemporaryQueue will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempQueue.delete();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateTemporaryTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);

            TemporaryTopic tempTopic = session.createTemporaryTopic();
            assertNotNull("TemporaryTopic object was null", tempTopic);
            assertNotNull("TemporaryTopic name was null", tempTopic.getTopicName());
            assertEquals("TemporaryTopic name not as expected", dynamicAddress, tempTopic.getTopicName());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Ignore // Need to complete implementation and update test peer link handle behaviour
    @Test(timeout = 5000)
    public void testCreateAndDeleteTemporaryTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String dynamicAddress = "myTempTopicAddress";
            testPeer.expectTempTopicCreationAttach(dynamicAddress);
            TemporaryTopic tempTopic = session.createTemporaryTopic();

            // Deleting the TemporaryTopic will be achieved by closing its creating link.
            testPeer.expectDetach(true, true, true);
            tempTopic.delete();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateConsumerSourceContainsQueueCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Queue.class);
    }

    @Test(timeout = 5000)
    public void testCreateConsumerSourceContainsTopicCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(Topic.class);
    }

    @Test(timeout = 5000)
    public void testCreateConsumerSourceContainsTempQueueCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryQueue.class);
    }

    @Test(timeout = 5000)
    public void testCreateConsumerSourceContainsTempTopicCapability() throws Exception {
        doCreateConsumerSourceContainsCapabilityTestImpl(TemporaryTopic.class);
    }

    private void doCreateConsumerSourceContainsCapabilityTestImpl(Class<? extends Destination> destType) throws JMSException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);

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

            session.createConsumer(dest);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateProducerTargetContainsQueueCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(Queue.class);
    }

    @Test(timeout = 5000)
    public void testCreateProducerTargetContainsTopicCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(Topic.class);
    }

    @Test(timeout = 5000)
    public void testCreateProducerTargetContainsTempQueueCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(TemporaryQueue.class);
    }

    @Test(timeout = 5000)
    public void testCreateProducerTargetContainsTempTopicCapability() throws Exception {
        doCreateProducerTargetContainsCapabilityTestImpl(TemporaryTopic.class);
    }

    private void doCreateProducerTargetContainsCapabilityTestImpl(Class<? extends Destination> destType) throws JMSException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);

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

            session.createProducer(dest);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerTargetContainsNoTypeCapabilityWhenAnonymousRelayNodeIsSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{AmqpConnectionProperties.ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin(true);
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
            assertNotNull("Producer object was null", producer);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerTargetContainsQueueCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Queue.class);
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerTargetContainsTopicCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Topic.class);
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerTargetContainsTempQueueCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(TemporaryQueue.class);
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerTargetContainsTempTopicCapabilityWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(TemporaryQueue.class);
    }

    private void doCreateAnonymousProducerTargetContainsCapabilityWhenAnonymousRelayNodeIsNotSupportedTestImpl(Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            //DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
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
            assertNotNull("Producer object was null", producer);

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

            Message message = session.createMessage();
            producer.send(dest, message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateDurableTopicSubscriber() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull("TopicSubscriber object was null", subscriber);
            assertFalse("TopicSubscriber should not be no-local", subscriber.getNoLocal());
            assertNull("TopicSubscriber should not have a selector", subscriber.getMessageSelector());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateDurableTopicSubscriberFailsIfConnectionDoesntHaveExplicitClientID() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection without an explicit clientId
            Connection connection = testFixture.establishConnecton(testPeer, null, null, null, false);
            connection.start();

            testPeer.expectBegin(true);
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

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCloseDurableTopicSubscriberDetachesWithCloseFalse() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlow();

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);

            testPeer.expectDetach(false, true, false);
            subscriber.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerWhenAnonymousRelayNodeIsSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{AmqpConnectionProperties.ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin(true);
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
            assertNotNull("Producer object was null", producer);

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

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(false);
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(true);
    }

    private void doCreateAnonymousProducerFailsWhenAnonymousRelayNodeIsSupportedButLinkRefusedTestImpl(boolean deferAttachFrameWrite) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{AmqpConnectionProperties.ANONYMOUS_RELAY};

            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            //Expect and refuse a link to the anonymous relay node
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(nullValue());
            targetMatcher.withDynamic(nullValue());//default = false
            targetMatcher.withDurable(nullValue());//default = none/0

            testPeer.expectSenderAttach(targetMatcher, true, false);
            //Expect the detach response to the test peer closing the producer link after refusal.
            testPeer.expectDetach(true, false, false);

            try {
                session.createProducer(null);
                fail("Expected producer creation to fail if anonymous-relay link refused");
            } catch (JMSException jmse) {
                //expected
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsNotDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(false);
    }

    @Test(timeout = 5000)
    public void testCreateProducerFailsWhenLinkRefusedAndAttachResponseWriteIsDeferred() throws Exception {
        doCreateProducerFailsWhenLinkRefusedTestImpl(true);
    }

    private void doCreateProducerFailsWhenLinkRefusedTestImpl(boolean deferAttachResponseWrite) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
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

            try {
                //Create a producer, expect it to throw exception due to the link-refusal
                session.createProducer(dest);
                fail("Producer creation should have failed when link was refused");
            } catch(JMSException jmse) {
                //Expected
            }

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            //DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            //Expect a new message sent by the above producer to cause creation of a new
            //sender link to the given destination, then closing the link after the message is sent.
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

            //Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1);
    }

    @Test(timeout=5000)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2);
    }

    private void doCommitTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            for (int i = 1; i <= consumeCount; i++) {
                // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(1000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the commit succeeded
            Discharge discharge = new Discharge();
            discharge.setFail(false);
            discharge.setTxnId(txnId);
            TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
            dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));
            testPeer.expectTransfer(dischargeMatcher, nullValue(), false, new Accepted(), true);

            session.commit();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testProducedMessagesOnTransactedSessionCarryTxnId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer  = session.createProducer(queue);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            // Expect the message which provoked creating the transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, false, txState, true);

            producer.send(session.createMessage());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testRollbackTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(1, 1);
    }

    @Test(timeout=5000)
    public void testRollbackTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(5, 2);
    }

    private void doRollbackTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            for (int i = 1; i <= consumeCount; i++) {
                // Then expect a *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(1000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            Discharge discharge = new Discharge();
            discharge.setFail(true);
            discharge.setTxnId(txnId);
            TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
            dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));
            testPeer.expectTransfer(dischargeMatcher, nullValue(), false, new Accepted(), true);

            // Expect the messages that were not consumed to be released
            int unconsumed = transferCount - consumeCount;
            for (int i = 1; i <= unconsumed; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            // Expect the consumer to be 'started' again as rollback completes
            testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));

            session.rollback();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testRollbackTransactedSessionWithPrefetchFullBeforeStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            int messageCount = 5;
            ((JmsConnection) connection).getPrefetchPolicy().setAll(messageCount);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer and fill the prefetch with messages, which we wont consume any of
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            session.createConsumer(queue);

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer  = session.createProducer(queue);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            // Expect the message which provoked creating the transaction
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, false, txState, true);

            producer.send(session.createMessage());

            // The consumer will be 'stopped' prior to rollback, however we will NOT send a 'drain' Flow
            // frame as we have manipulated that all the credit was already used, i.e. it already stopped.

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            Discharge discharge = new Discharge();
            discharge.setFail(true);
            discharge.setTxnId(txnId);
            TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
            dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));
            testPeer.expectTransfer(dischargeMatcher, nullValue(), false, new Accepted(), true);

            // Expect the messages that were not consumed to be released
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            // Expect the consumer to be 'started' again as rollback completes
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(messageCount)));

            session.rollback();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testRollbackTransactedSessionWithPrefetchFullyUtilisedByDrainWhenStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            int messageCount = 5;
            ((JmsConnection) connection).getPrefetchPolicy().setAll(messageCount);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer, expect it to flow credit, but don't send it any messages
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(messageCount)));

            session.createConsumer(queue);

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer  = session.createProducer(queue);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            // Expect the message which provoked creating the transaction
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, false, txState, true);

            producer.send(session.createMessage());

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain' Flow.
            // Action the drain by filling the prefetch (which is equivalent to this having happened while
            // the Flow was in flight to the peer), and then DONT send a flow frame back to the client
            // as it can tell from the messages that all the credit has been used.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, true, false, equalTo(UnsignedInteger.valueOf(messageCount)), 1);

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            Discharge discharge = new Discharge();
            discharge.setFail(true);
            discharge.setTxnId(txnId);
            TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
            dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));
            testPeer.expectTransfer(dischargeMatcher, nullValue(), false, new Accepted(), true);

            // Expect the messages that were not consumed to be released
            for (int i = 1; i <= messageCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }

            // Expect the consumer to be 'started' again as rollback completes
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(messageCount)));

            session.rollback();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testDefaultOutcomeIsModifiedForConsumerSourceOnTransactedSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(queueName));
            sourceMatcher.withDynamic(equalTo(false));
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));
            ModifiedMatcher outcomeMatcher = new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(equalTo(false));
            sourceMatcher.withDefaultOutcome(outcomeMatcher);

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();

            session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=5000)
    public void testPrefetchPolicyInfluencesCreditFlow() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            int newPrefetch = 263;
            ((JmsConnection) connection).getPrefetchPolicy().setAll(newPrefetch);
            connection.start();

            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(newPrefetch)));

            session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
