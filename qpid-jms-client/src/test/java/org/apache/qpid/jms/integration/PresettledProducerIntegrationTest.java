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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.ListDescribedType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransactionalState;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test MessageProducers created using various configuration of the presettle options
 */
public class PresettledProducerIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private final Symbol[] serverCapabilities = new Symbol[] { ANONYMOUS_RELAY };

    //----- Test the jms.presettleAll option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleAll=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, true);
        }
    }

    //----- Test the amqp.presettleProducers option --------------------------//

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedToTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testPresettledProducersConfigurationAppliedAnonymousSendToTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, true);
        }
    }

    //----- Test the jms.presettleProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, true, true, false, true);
        }
    }

    //----- Test the jms.presettleTopicProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, false, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTopicProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, false, false, true);
        }
    }

    //----- Test the jms.presettleQueueProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, false, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, false, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleQueueProducers=true", serverCapabilities, null);
            doTestProducerWithPresettleOptions(testPeer, connection, false, true, false, true, false, true);
        }
    }

    //----- Test the jms.presettleTransactedProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempTopic() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempQueue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, true, true, true, false, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTopicNoTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, true, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersQueueNoTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, false, false);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempTopicNoTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, true, true);
        }
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempQueueNoTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.presettlePolicy.presettleTransactedProducers=true");
            doTestProducerWithPresettleOptions(testPeer, connection, false, false, false, false, true);
        }
    }

    //----- Test Method implementation ---------------------------------------//

    private void doTestProducerWithPresettleOptions(TestAmqpPeer testPeer, Connection connection, boolean senderSettled, boolean transferSettled, boolean topic, boolean temporary) throws Exception {
        doTestProducerWithPresettleOptions(testPeer, connection, false, senderSettled, transferSettled, topic, temporary);
    }

    private void doTestProducerWithPresettleOptions(TestAmqpPeer testPeer, Connection connection, boolean transacted, boolean senderSettled, boolean transferSettled, boolean topic, boolean temporary) throws Exception {
        doTestProducerWithPresettleOptions(testPeer, connection, transacted, false, senderSettled, transferSettled, topic, temporary);
    }

    private void doTestProducerWithPresettleOptions(TestAmqpPeer testPeer, Connection connection, boolean transacted, boolean anonymous, boolean senderSettled, boolean transferSettled, boolean topic, boolean temporary) throws Exception {
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
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        Destination destination = null;
        if (topic) {
            if (temporary) {
                String dynamicAddress = "myTempTopicAddress";
                testPeer.expectTempTopicCreationAttach(dynamicAddress);
                destination = session.createTemporaryTopic();
            } else {
                destination = session.createTopic("myTopic");
            }
        } else {
            if (temporary) {
                String dynamicAddress = "myTempQueueAddress";
                testPeer.expectTempQueueCreationAttach(dynamicAddress);
                destination = session.createTemporaryQueue();
            } else {
                destination = session.createQueue("myTopic");
            }
            destination = session.createQueue("myQueue");
        }

        if (senderSettled) {
            testPeer.expectSettledSenderAttach();
        } else {
            testPeer.expectSenderAttach();
        }

        MessageProducer producer = null;
        if (anonymous) {
            producer = session.createProducer(null);
        } else {
            producer = session.createProducer(destination);
        }

        // Create and transfer a new message
        MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
        headersMatcher.withDurable(equalTo(true));
        MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
        TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
        messageMatcher.setHeadersMatcher(headersMatcher);
        messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

        Matcher<?> stateMatcher = nullValue();
        if (transacted) {
            stateMatcher = new TransactionalStateMatcher();
            ((TransactionalStateMatcher) stateMatcher).withTxnId(equalTo(txnId));
            ((TransactionalStateMatcher) stateMatcher).withOutcome(nullValue());
        }

        ListDescribedType responseState = new Accepted();
        if (transacted) {
            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());
        }

        if (transferSettled) {
            testPeer.expectTransfer(messageMatcher, stateMatcher, true, false, responseState, false);
        } else {
            testPeer.expectTransfer(messageMatcher, stateMatcher, false, true, responseState, true);
        }

        Message message = session.createTextMessage();

        if (anonymous) {
            producer.send(destination, message);
        } else {
            producer.send(message);
        }

        if (transacted) {
            testPeer.expectDischarge(txnId, true);
        }

        testPeer.expectClose();

        connection.close();

        testPeer.waitForAllHandlersToComplete(1000);
    }
}
