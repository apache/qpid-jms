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
import static org.hamcrest.Matchers.equalTo;
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
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.junit.Test;

/**
 * Test for Consumer state when various consumer presettle options are applied.
 */
public class PresettledConsumerIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    //----- Test the jms.presettlePolicy.presettleAll option -----------------//

    @Test(timeout = 20000)
    public void testPresettleAllConfigurationAppliedToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledAllConfigurationAppliedToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledAllConfigurationAppliedToTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledAllConfigurationAppliedToTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryQueue.class);
    }

    //----- Test the jms.presettlePolicy.presettleTopicConsumers option ------//

    @Test(timeout = 20000)
    public void testPresettleTopicConsumersConfigurationAppliedToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledTopicConsumersConfigurationAppliedToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledTopicConsumersConfigurationAppliedToTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledTopicConsumersConfigurationAppliedToTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, false, false, TemporaryQueue.class);
    }

    //----- Test the jms.presettlePolicy.presettleQueueConsumers option ----- //

    @Test(timeout = 20000)
    public void testPresettleQueueConsumersConfigurationAppliedToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledQueueConsumersConfigurationAppliedToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledQueueConsumersConfigurationAppliedToTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledQueueConsumersConfigurationAppliedToTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, true, TemporaryQueue.class);
    }

    //----- Test the presettled consumer still settles if needed -------------//

    @Test(timeout = 20000)
    public void testPresettledTopicConsumerSettlesWhenNeeded() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledQueueConsumerSettlesWhenNeeded() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testPresettledTempTopicConsumerSettlesWhenNeeded() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testPresettledTempQueueConsumerSettlesWhenNeeded() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, false, true, false, TemporaryQueue.class);
    }

    //----- Test that transacted consumers always send unsettled -------------//

    @Test(timeout = 20000)
    public void testTransactedTopicConsumerNotSettledPresettleAll() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testTransactedQueueConsumerNotSettledPresettleAll() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testTransactedTempTopicConsumerNotSettledPresettleAll() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testTransactedTempQueueConsumerNotSettledPresettleAll() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testTransactedTopicConsumerNotSettledPresettleTopicConsumers() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testTransactedQueueConsumerNotSettledPresettleQueueConsumers() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testTransactedTempTopicConsumerNotSettledPresettleTopicConsumers() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testTransactedTempQueueConsumerNotSettledPresettleQueueConsumers() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueConsumers=true";
        doTestConsumerWithPresettleOptions(presettleConfig, true, false, false, TemporaryQueue.class);
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
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, transferSettled);
            if (!transferSettled) {
                if (!transacted) {
                    testPeer.expectDispositionThatIsAcceptedAndSettled();
                } else {
                    // Then expect an *settled* TransactionalState disposition for each message
                    // once received by the consumer
                    TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                    stateMatcher.withTxnId(equalTo(txnId));
                    stateMatcher.withOutcome(new AcceptedMatcher());

                    testPeer.expectDisposition(true, stateMatcher);
                }
            }

            MessageConsumer consumer = session.createConsumer(destination);
            assertNotNull(consumer.receive(3000));

            if (transacted) {
                testPeer.expectDischarge(txnId, true);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
