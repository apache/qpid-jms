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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class JMSContextIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private Symbol[] SERVER_ANONYMOUS_RELAY = new Symbol[]{ANONYMOUS_RELAY};

    @Test(timeout = 20000)
    public void testCreateAndCloseContext() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateContextWithClientId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, false, null, null, null, true);
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateContextAndSetClientID() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, false, null, null, null, false);
            context.setClientID(UUID.randomUUID().toString());
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateAutoAckSessionByDefault() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());
            testPeer.expectBegin();
            context.createTopic("TopicName");
            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateContextWithTransactedSessionMode() throws Exception {
        Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, JMSContext.SESSION_TRANSACTED);
            assertEquals(JMSContext.SESSION_TRANSACTED, context.getSessionMode());

            // Session should be created and a coordinator should be attached since this
            // should be a TX session, then a new TX is declared, once closed the TX should
            // be discharged as a roll back.
            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();
            testPeer.expectClose();

            context.createTopic("TopicName");

            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateContextFromContextWithSessionsActive() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer);
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());
            testPeer.expectBegin();
            context.createTopic("TopicName");

            // Create a second should not create a new session yet, once a new connection is
            // create on demand then close of the second context should only close the session
            JMSContext other = context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
            assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, other.getSessionMode());
            testPeer.expectBegin();
            testPeer.expectEnd();
            other.createTopic("TopicName");
            other.close();

            testPeer.waitForAllHandlersToComplete(1000);

            // Now the connection should close down.
            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testOnlyOneProducerCreatedInSingleContext() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            // One producer created should send an attach.
            JMSProducer producer1 = context.createProducer();
            assertNotNull(producer1);

            // An additional one should not result in an attach
            JMSProducer producer2 = context.createProducer();
            assertNotNull(producer2);

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testEachContextGetsItsOwnProducer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());
            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            // One producer created should send an attach.
            JMSProducer producer1 = context.createProducer();
            assertNotNull(producer1);

            // An additional one should not result in an attach
            JMSContext other = context.createContext(JMSContext.AUTO_ACKNOWLEDGE);
            JMSProducer producer2 = other.createProducer();
            assertNotNull(producer2);

            testPeer.expectEnd();
            testPeer.expectEnd();
            testPeer.expectClose();

            other.close();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
