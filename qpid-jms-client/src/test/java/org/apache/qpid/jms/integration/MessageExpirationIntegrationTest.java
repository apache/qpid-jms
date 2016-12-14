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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageExpirationIntegrationTest extends QpidJmsTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(MessageExpirationIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=20000)
    public void testIncomingExpiredMessageGetsFiltered() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach and send credit, then send it an
            // already-expired message followed by a live message.
            testPeer.expectReceiverAttach();

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType("already-expired"));

            String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            final MessageConsumer consumer = session.createConsumer(queue);

            // Call receive, expect the first message to be filtered due to expiry,
            // and the second message to be given to the test app and accepted.
            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            Message m = consumer.receive(3000);
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage)m).getText());

            // Verify the other message is not there. Will drain to be sure there are no messages.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH - 2)));
            // Then reopen the credit window afterwards
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));

            m = consumer.receive(10);
            assertNull("Message should not have been received", m);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testIncomingExpiredMessageGetsConsumedWhenFilterDisabled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.localMessageExpiry=false");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach and send credit, then send it an
            // already-expired message followed by a live message.
            testPeer.expectReceiverAttach();

            String expiredMsgContent = "already-expired";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType(expiredMsgContent));

            String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            final MessageConsumer consumer = session.createConsumer(queue);

            // Call receive, expect the expired message as we disabled local expiry.
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            Message m = consumer.receive(3000);
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", expiredMsgContent, ((TextMessage)m).getText());

            // Verify the other message is there
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            m = consumer.receive(3000);
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage)m).getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testIncomingExpiredMessageGetsFilteredAsync() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach and send credit, then send it an
            // already-expired message followed by a live message.
            testPeer.expectReceiverAttach();

            String expiredMessageContent = "already-expired";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType(expiredMessageContent));

            final String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            final MessageConsumer consumer = session.createConsumer(queue);

            // Add message listener, expect the first message to be filtered due to expiry,
            // and the second message to be given to the test app and accepted.
            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            final CountDownLatch success = new CountDownLatch(1);
            final AtomicBoolean listenerFailure = new AtomicBoolean();
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message incoming) {
                    try {
                        TextMessage textMessage = (TextMessage) incoming;
                        if (liveMsgContent.equals(textMessage.getText())) {
                            success.countDown();
                        } else {
                            listenerFailure.set(true);
                            LOG.error("Received unexpected message:" + incoming);
                        }
                    } catch (Exception e) {
                        listenerFailure.set(true);
                        LOG.error("Exception in listener", e);
                    }
                }
            });

            assertTrue("didn't get expected message", success.await(5, TimeUnit.SECONDS));
            assertFalse("There was a failure in the listener, see logs", listenerFailure.get());

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testIncomingExpiredMessageGetsConsumedWhenFilterDisabledAsync() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.localMessageExpiry=false");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach and send credit, then send it an
            // already-expired message followed by a live message.
            testPeer.expectReceiverAttach();

            final String expiredMessageContent = "already-expired";
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType(expiredMessageContent));

            final String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            final MessageConsumer consumer = session.createConsumer(queue);

            // Add message listener, expect both messages as the filter is disabled
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            final CountDownLatch success = new CountDownLatch(2);
            final AtomicBoolean listenerFailure = new AtomicBoolean();
            consumer.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message incoming) {
                    try {
                        TextMessage textMessage = (TextMessage) incoming;
                        if (expiredMessageContent.equals(textMessage.getText()) || liveMsgContent.equals(textMessage.getText())) {
                            success.countDown();
                        } else {
                            listenerFailure.set(true);
                            LOG.error("Received unexpected message:" + incoming);
                        }
                    } catch (Exception e) {
                        listenerFailure.set(true);
                        LOG.error("Exception in listener", e);
                    }
                }
            });

            assertTrue("didn't get expected messages", success.await(5, TimeUnit.SECONDS));
            assertFalse("There was a failure in the listener, see logs", listenerFailure.get());

            testPeer.waitForAllHandlersToComplete(3000);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
