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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusExpiryPolicy;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.junit.Test;

public class SessionIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 5000)
    public void testCloseSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
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
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            session.createProducer(queue);
        }
    }

    @Test(timeout = 5000)
    public void testCreateConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
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
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
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

    @Test(timeout = 5000)
    public void testCreateDurableTopicSubscriber() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);
            String subscriptionName = "mySubscription";

            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);

            TopicSubscriber subscriber = session.createDurableSubscriber(dest, subscriptionName);
            assertNotNull("TopicSubscriber object was null", subscriber);
            assertFalse("TopicSubscriber should not be no-local", subscriber.getNoLocal());
            assertNull("TopicSubscriber should not have a selector", subscriber.getMessageSelector());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 5000)
    public void testCreateAnonymousProducerWhenAnonymousRelaySupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
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

            testPeer.expectSenderAttach(targetMatcher);

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            //Expect a new message sent on the above link to the anonymous relay
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            producer.send(dest, message);

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
