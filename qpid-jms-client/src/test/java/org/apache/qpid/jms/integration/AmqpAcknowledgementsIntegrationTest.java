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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.message.JmsMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.RejectedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

public class AmqpAcknowledgementsIntegrationTest extends QpidJmsTestCase {

    private static final int INDIVIDUAL_ACK = 101;
    private static final int SKIP = -1;
    private static final int INVALID = 99;

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testAcknowledgeFailsAfterSessionIsClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(null), 1);

            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message receivedMessage = messageConsumer.receive(6000);
            assertNotNull("Message was not recieved", receivedMessage);

            testPeer.expectDisposition(true, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), 1, 1);
            testPeer.expectEnd();

            session.close();

            try {
                receivedMessage.acknowledge();
                fail("Should not be able to acknowledge the message after session closed");
            } catch (JMSException jmsex) {}

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testDefaultAcceptMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(SKIP, new AcceptedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestAcceptMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.ACCEPTED, new AcceptedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestRejectMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.REJECTED, new RejectedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestReleaseMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.RELEASED, new ReleasedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestReleaseMessagesClearPropsFirst() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.RELEASED, new ReleasedMatcher(), true);
    }

    @Test(timeout = 20000)
    public void testRequestModifiedFailedMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.MODIFIED_FAILED, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), false);
    }

    @Test(timeout = 20000)
    public void testRequestModifiedFailedUndeliverableHereMessages() throws Exception {
        doTestAmqpAcknowledgementTestImpl(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE, new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(equalTo(true)), false);
    }

    @Test(timeout = 20000)
    public void testRequestAcknowledgeMessagesWithInvalidDisposition() throws Exception {
        doTestAmqpAcknowledgementTestImpl(INVALID, new AcceptedMatcher(), false);
    }

    private void doTestAmqpAcknowledgementTestImpl(int disposition, Matcher<?> descriptorMatcher, boolean clearPropsFirst) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            int msgCount = 3;
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(null), msgCount);
            for (int i = 1; i <= msgCount; i++) {
                testPeer.expectDisposition(true, descriptorMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message lastReceivedMessage = null;
            for (int i = 1; i <= msgCount; i++) {
                lastReceivedMessage = messageConsumer.receive(6000);
                assertNotNull("Message " + i + " was not recieved", lastReceivedMessage);
            }

            if (disposition != SKIP) {
                if (clearPropsFirst) {
                    lastReceivedMessage.clearProperties();
                }

                lastReceivedMessage.setIntProperty(JmsMessageSupport.JMS_AMQP_ACK_TYPE, disposition);
            }

            if (disposition == INVALID) {
                try {
                    lastReceivedMessage.acknowledge();
                    fail("Should throw exception due to invalid ack type");
                } catch (JMSException jmsex) {}

                lastReceivedMessage.setIntProperty(JmsMessageSupport.JMS_AMQP_ACK_TYPE, JmsMessageSupport.ACCEPTED);
                lastReceivedMessage.acknowledge();
            } else {
                lastReceivedMessage.acknowledge();
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testDefaultAcceptMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(SKIP, new AcceptedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestAcceptMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.ACCEPTED, new AcceptedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestRejectMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.REJECTED, new RejectedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestReleaseMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.RELEASED, new ReleasedMatcher(), false);
    }

    @Test(timeout = 20000)
    public void testRequestReleaseMessagesClearPropsFirstWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.RELEASED, new ReleasedMatcher(), true);
    }

    @Test(timeout = 20000)
    public void testRequestModifiedFailedMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.MODIFIED_FAILED, new ModifiedMatcher().withDeliveryFailed(equalTo(true)), false);
    }

    @Test(timeout = 20000)
    public void testRequestModifiedFailedUndeliverableHereMessagesWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE, new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(equalTo(true)), false);
    }

    @Test(timeout = 20000)
    public void testRequestAcknowledgeMessagesWithInvalidDispositionWithMessageListener() throws Exception {
        doTestAmqpAcknowledgementAsyncTestImpl(INVALID, new AcceptedMatcher(), false);
    }

    private void doTestAmqpAcknowledgementAsyncTestImpl(int disposition, Matcher<?> descriptorMatcher, boolean clearPropsFirst) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            final int msgCount = 3;
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(null), msgCount);
            for (int i = 1; i <= msgCount; i++) {
                testPeer.expectDisposition(true, descriptorMatcher);
            }

            final CountDownLatch receiveCountDown = new CountDownLatch(msgCount);
            final AtomicReference<Message> lastReceivedMessage = new AtomicReference<Message>();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    lastReceivedMessage.set(message);
                    receiveCountDown.countDown();
                }
            });

            assertTrue("Did not get all messages", receiveCountDown.await(10, TimeUnit.SECONDS));
            assertNotNull("Message was not received", lastReceivedMessage.get());

            if (disposition != SKIP) {
                if (clearPropsFirst) {
                    lastReceivedMessage.get().clearProperties();
                }
                lastReceivedMessage.get().setIntProperty(JmsMessageSupport.JMS_AMQP_ACK_TYPE, disposition);
            }

            if (disposition == INVALID) {
                try {
                    lastReceivedMessage.get().acknowledge();
                    fail("Should throw exception due to invalid ack type");
                } catch (JMSException jmsex) {}

                lastReceivedMessage.get().setIntProperty(JmsMessageSupport.JMS_AMQP_ACK_TYPE, JmsMessageSupport.ACCEPTED);
                lastReceivedMessage.get().acknowledge();
            } else {
                lastReceivedMessage.get().acknowledge();
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testAcknowledgeIndividualMessages()  throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(INDIVIDUAL_ACK);
            Queue queue = session.createQueue("myQueue");

            int msgCount = 6;
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType(null), msgCount, false, false,
                    Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(msgCount)), 1, false, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);

            List<Message> messages = new ArrayList<>();
            Message lastReceivedMessage = null;
            for (int i = 0; i < msgCount; i++) {
                lastReceivedMessage = messageConsumer.receive(3000);
                assertNotNull("Message " + i + " was not received", lastReceivedMessage);
                messages.add(lastReceivedMessage);

                assertEquals("unexpected message number property", i, lastReceivedMessage.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
            }

            List<Integer> ackTypes = new ArrayList<>();
            ackTypes.add(SKIP);
            ackTypes.add(JmsMessageSupport.ACCEPTED);
            ackTypes.add(JmsMessageSupport.REJECTED);
            ackTypes.add(JmsMessageSupport.RELEASED);
            ackTypes.add(JmsMessageSupport.MODIFIED_FAILED);
            ackTypes.add(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE);

            Matcher<?>[] dispositionMatchers = new Matcher<?>[msgCount];
            dispositionMatchers[0] = null;
            dispositionMatchers[JmsMessageSupport.ACCEPTED] = new AcceptedMatcher();
            dispositionMatchers[JmsMessageSupport.REJECTED] = new RejectedMatcher();
            dispositionMatchers[JmsMessageSupport.RELEASED] = new ReleasedMatcher();
            dispositionMatchers[JmsMessageSupport.MODIFIED_FAILED] = new ModifiedMatcher().withDeliveryFailed(equalTo(true));
            dispositionMatchers[JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE] = new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(equalTo(true));

            // Acknowledge the messages in a random order with random amqp ack type set (leaving one message without
            // any specific set, to check it accepts), verify the individual dispositions have expected delivery state.
            Random rand = new Random();
            for (int i = 0; i < msgCount; i++) {
                Message msg = messages.remove(rand.nextInt(msgCount - i));

                int deliveryNumber =  msg.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER) + 1;
                int ackType = ackTypes.remove(rand.nextInt(msgCount - i));

                if(ackType != SKIP) {
                    msg.setIntProperty(JmsMessageSupport.JMS_AMQP_ACK_TYPE, ackType);

                    testPeer.expectDisposition(true, dispositionMatchers[ackType], deliveryNumber, deliveryNumber);
                } else {
                    testPeer.expectDisposition(true, new AcceptedMatcher(), deliveryNumber, deliveryNumber);
                }

                msg.acknowledge();

                testPeer.waitForAllHandlersToComplete(3000);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
