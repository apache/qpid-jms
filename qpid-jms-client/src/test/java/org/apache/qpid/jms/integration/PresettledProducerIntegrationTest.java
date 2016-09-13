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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test MessageProducers created using various configuration of the presettle options
 */
public class PresettledProducerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(PresettledProducerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private final Symbol[] serverCapabilities = new Symbol[] { ANONYMOUS_RELAY };

    //----- Test the jms.presettleAll option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllSendToTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleAllAnonymousSendToTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, TemporaryQueue.class);
    }

    //----- Test the jms.presettleProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleProducersAnonymousTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, true, true, TemporaryQueue.class);
    }

    //----- Test the jms.presettleTopicProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTopicProducersAnonymousTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTopicProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, false, TemporaryQueue.class);
    }

    //----- Test the jms.presettleQueueProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleQueueProducersAnonymousTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleQueueProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, true, false, true, TemporaryQueue.class);
    }

    //----- Test the jms.presettleTransactedProducers option ---------------------------------//

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, true, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, true, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, true, false, true, true, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, true, false, true, true, TemporaryQueue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTopicNoTX() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, Topic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersQueueNoTX() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, Queue.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempTopicNoTX() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testJmsPresettlePolicyPresettleTransactedProducersTempQueueNoTX() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleTransactedProducers=true";
        doTestProducerWithPresettleOptions(presettleConfig, false, false, false, false, TemporaryQueue.class);
    }

    //----- Test Method implementation ---------------------------------------//

    private void doTestProducerWithPresettleOptions(String uriOptions, boolean transacted, boolean anonymous, boolean senderSettled, boolean transferSettled, Class<? extends Destination> destType) throws Exception {
        doTestProducerWithPresettleOptions(uriOptions, transacted, anonymous, true, senderSettled, transferSettled, destType);
    }

    private void doTestProducerWithPresettleOptions(String uriOptions, boolean transacted, boolean anonymous, boolean relaySupported, boolean senderSettled, boolean transferSettled, Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, uriOptions, relaySupported ? serverCapabilities : null, null);
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

            if (anonymous && !relaySupported) {
                testPeer.expectDetach(true, true, true);
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

    //----- Test the jms.presettleAll with asynchronous completion -----------//

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleAllSendToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleAllSendToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testsyncCompletionPresettleAllAnonymousSendToTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, true, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testsyncCompletionPresettleAllAnonymousSendToQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleAll=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, true, true, true, Queue.class);
    }

    //----- Test the jms.presettleProducers with asynchronous completion -----//

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleProducersTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, false, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleProducersQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, false, true, true, Queue.class);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleProducersAnonymousTopic() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, true, true, true, Topic.class);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionPresettleProducersAnonymousQueue() throws Exception {
        String presettleConfig = "?jms.presettlePolicy.presettleProducers=true";
        doTestAsyncCompletionProducerWithPresettleOptions(presettleConfig, false, true, true, true, Queue.class);
    }

    //----- Asynchronous Completion test method implementation ---------------//

    private void doTestAsyncCompletionProducerWithPresettleOptions(String uriOptions, boolean transacted, boolean anonymous, boolean senderSettled, boolean transferSettled, Class<? extends Destination> destType) throws Exception {
        doTestAsyncCompletionProducerWithPresettleOptions(uriOptions, transacted, anonymous, true, senderSettled, transferSettled, destType);
    }

    private void doTestAsyncCompletionProducerWithPresettleOptions(String uriOptions, boolean transacted, boolean anonymous, boolean relaySupported, boolean senderSettled, boolean transferSettled, Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, uriOptions, relaySupported ? serverCapabilities : null, null);
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

            if (senderSettled) {
                testPeer.expectSettledSenderAttach();
            } else {
                testPeer.expectSenderAttach();
            }

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
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

            if (anonymous && !relaySupported) {
                testPeer.expectDetach(true, true, true);
            }

            Message message = session.createTextMessage();

            if (anonymous) {
                producer.send(destination, message, listener);
            } else {
                producer.send(message, listener);
            }

            if (transacted) {
                testPeer.expectDischarge(txnId, true);
            }

            testPeer.expectClose();

            assertTrue("Did not get async callback", listener.awaitCompletion(2000, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);
            assertEquals(1, listener.successCount);
            assertEquals(0, listener.errorCount);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        private final CountDownLatch completed;

        public volatile int successCount;
        public volatile int errorCount;

        public volatile Message message;
        public volatile Exception exception;

        public TestJmsCompletionListener() {
            this(1);
        }

        public TestJmsCompletionListener(int expected) {
            this.completed = new CountDownLatch(expected);
        }

        public boolean awaitCompletion(long timeout, TimeUnit units) throws InterruptedException {
            return completed.await(timeout, units);
        }

        @Override
        public void onCompletion(Message message) {
            LOG.info("JmsCompletionListener onCompletion called with message: {}", message);
            this.message = message;
            this.successCount++;

            completed.countDown();
        }

        @Override
        public void onException(Message message, Exception exception) {
            LOG.info("JmsCompletionListener onException called with message: {} error {}", message, exception);

            this.message = message;
            this.exception = exception;
            this.errorCount++;

            completed.countDown();
        }
    }
}
