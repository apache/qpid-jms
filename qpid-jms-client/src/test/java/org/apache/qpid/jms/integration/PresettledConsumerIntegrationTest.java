/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.junit.Test;

/**
 * Test for Consumer state when various consumer presettle options are applied.
 */
public class PresettledConsumerIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    //----- Test the amqp.presettleConsumers option --------------------------//

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTopic() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToQueue() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTempTopic() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTempQueue() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTopic() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToQueue() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempTopic() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempQueue() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTopicNoRelaySupport() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToQueueNoRelaySupport() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempTopicNoRelaySupport() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempQueueNoRelaySupport() throws Exception {
        String presettleConfig = "?amqp.presettleConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryQueue.class);
    }

    //----- Test Method implementation ---------------------------------------//

    private void doTestConsumerWithPresettleOptions(String uriOptions, boolean transacted, boolean senderSettled, boolean transferSettled, Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, uriOptions);
            connection.start();
            testPeer.expectBegin();

            Session session = null;
            Binary txnId = null;

            if (transacted) {
                // Expect the session, with an immediate link to the transaction coordinator
                // using a target with the expected capabilities only.
                CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
                txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
                testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
                testPeer.expectDeclare(txnId);

                session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
                // Use client ack so the receipt of the settled disposition is controllable.
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

            Destination destination = null;
            if (destType == Queue.class) {
                destination = session.createQueue("MyQueue");
            } else if (destType == Topic.class) {
                destination = session.createTopic("MyTopis");
            } else if (destType == TemporaryQueue.class) {
                String dynamicAddress = "myTempQueueAddress";
                testPeer.expectTempQueueCreationAttach(dynamicAddress);
                destination = session.createTemporaryQueue();
            } else if (destType == TemporaryTopic.class) {
                String dynamicAddress = "myTempTopicAddress";
                testPeer.expectTempTopicCreationAttach(dynamicAddress);
                destination = session.createTemporaryTopic();
            } else {
                fail("unexpected type");
            }

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            if (senderSettled) {
                testPeer.expectSettledReceiverAttach();
            } else {
                testPeer.expectReceiverAttach();
            }

            // Send a settled transfer, client should not send any dispositions
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, true);

            MessageConsumer consumer = session.createConsumer(destination);
            assertNotNull(consumer.receive(100));

            if (transacted) {
                testPeer.expectDischarge(txnId, true);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
