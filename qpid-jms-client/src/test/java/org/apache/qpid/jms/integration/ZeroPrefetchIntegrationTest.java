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

import java.util.Date;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class ZeroPrefetchIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveWithMessageExpiredInFlight() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            // Expect that once receive is called, it flows a credit, give it an already-expired message.
            // Expect it to be filtered due to local expiration checking.
            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, props, null, new AmqpValueDescribedType("already-expired"));

            ModifiedMatcher modifiedMatcher = new ModifiedMatcher();
            modifiedMatcher.withDeliveryFailed(equalTo(true));
            modifiedMatcher.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modifiedMatcher, 1, 1);

            // Expect the client to then flow another credit requesting a message.
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(1)));

            // Send it a live message, expect it to get accepted.
            String liveMsgContent = "valid";
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(liveMsgContent), 2);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            Message m = consumer.receive(5000);
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage) m).getText());

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=20000)
    public void testZeroPrefetchConsumerReceiveNoWaitDrainsWithOneCredit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Create a connection with zero prefetch
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=0");
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the consumer to attach but NOT send credit
            testPeer.expectReceiverAttach();

            final MessageConsumer consumer = session.createConsumer(queue);

            String msgContent = "content";
            // Expect that once receiveNoWait is called, it drains with 1 credit, then give it a message.
            testPeer.expectLinkFlow(true, false, equalTo(UnsignedInteger.ONE));
            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, null, null, null, new AmqpValueDescribedType(msgContent), 1);

            // Expect it to be accepted.
            testPeer.expectDisposition(true, new AcceptedMatcher(), 1, 1);

            Message m = consumer.receiveNoWait();
            assertNotNull("Message should have been received", m);
            assertTrue(m instanceof TextMessage);
            assertEquals("Unexpected message content", msgContent, ((TextMessage) m).getText());

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
