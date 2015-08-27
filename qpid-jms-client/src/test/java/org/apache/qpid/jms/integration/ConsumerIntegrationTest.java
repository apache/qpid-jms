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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

public class ConsumerIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCloseConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a consumer, then remotely end it afterwards.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageConsumer consumer = session.createConsumer(queue);

            // Verify the consumer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("consumer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
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
            }, 10000, 10));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
        }
    }

    /**
     * Test that a message is received when calling receive with a timeout
     * of 0, which means wait indefinitely.
     */
    @Test(timeout = 20000)
    public void testReceiveMessageWithRecieveZeroTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(0);

            assertNotNull("A message should have been recieved", receivedMessage);

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout=20000)
    public void testCloseDurableSubscriberWithUnackedAnUnconsumedPrefetchedMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            String topicName = "myTopic";
            String subscriptionName = "mySubscription";

            Topic topic = session.createTopic(topicName);

            int messageCount = 5;
            // Create a consumer and fill the prefetch with some messages,
            // which we will consume some of but ack none of.
            testPeer.expectDurableSubscriberAttach(topicName, subscriptionName);
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            MessageConsumer consumer = session.createDurableSubscriber(topic, subscriptionName);

            int consumeCount = 2;
            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = consumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            testPeer.expectDetach(false, true, false);

            // Expect the messages that were not acked to be to be either
            // modified or released depending on whether the app saw them
            for (int i = 1; i <= consumeCount; i++) {
                testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)));
            }
            for (int i = consumeCount + 1 ; i <= messageCount; i++) {
                testPeer.expectDisposition(true, new ReleasedMatcher());
            }
            testPeer.expectEnd();

            consumer.close();
            session.close();

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();
        }
    }
}
