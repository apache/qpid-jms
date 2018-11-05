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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declare;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declared;
import org.apache.qpid.jms.test.testpeer.describedtypes.Error;
import org.apache.qpid.jms.test.testpeer.describedtypes.Modified;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransactionalState;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ReleasedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for behavior of Transacted Session operations.
 */
@RunWith(QpidJMSTestRunner.class)
public class TransactionsIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionsIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=20000)
    public void testTransactionRolledBackOnSessionClose() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // Closed session should roll-back the TX with a failed discharge
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTransactionCommitFailWithEmptyRejectedDisposition() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId1);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId1));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId1);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the commit failed
            testPeer.expectDischarge(txnId1, false, new Rejected());

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            // session should roll back on close
            testPeer.expectDischarge(txnId2, true);
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testProducedMessagesAfterCommitOfSentMessagesFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId1);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId1));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId1);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the commit failed
            Rejected commitFailure = new Rejected(new Error(Symbol.valueOf("failed"), "Unknown error"));
            testPeer.expectDischarge(txnId1, false, commitFailure);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId2);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId2));
            stateMatcher.withOutcome(nullValue());

            txState = new TransactionalState();
            txState.setTxnId(txnId2);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId2, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testProducedMessagesAfterRollbackSentMessagesFails() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId1));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId1);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with rejected and settled disposition to indicate the rollback failed
            Rejected commitFailure = new Rejected(new Error(Symbol.valueOf("failed"), "Unknown error"));
            testPeer.expectDischarge(txnId1, true, commitFailure);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            testPeer.expectDeclare(txnId2);

            try {
                session.rollback();
                fail("Rollback operation should have failed.");
            } catch (JMSException jmsex) {
            }

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId2));
            stateMatcher.withOutcome(nullValue());

            txState = new TransactionalState();
            txState.setTxnId(txnId2);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId2, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, false, false);
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseBefore() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, true, true);
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseAfter() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(1, 1, true, false);
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, false, false);
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesBefore() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, true, true);
    }

    @Test(timeout=20000)
    public void testCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesAfter() throws Exception {
        doCommitTransactedSessionWithConsumerTestImpl(5, 2, true, false);
    }

    private void doCommitTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, boolean closeConsumer, boolean closeBeforeCommit) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int DEFAULT_PREFETCH = 100;

            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer, "jms.prefetchPolicy.all=" + DEFAULT_PREFETCH);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            for (int i = 1; i <= consumeCount; i++) {
                // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);
            }

            final CountDownLatch expected = new CountDownLatch(transferCount);
            ((JmsConnection) connection).addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                    expected.countDown();
                }
            });

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Ensure all the messages arrived so that the matching below is deterministic
            assertTrue("Expected transfers didnt occur: " + expected.getCount(), expected.await(10, TimeUnit.SECONDS));

            // Expect the consumer to close now
            if (closeConsumer && closeBeforeCommit) {

                // Expect the client to then drain off all credit from the link.
                testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(DEFAULT_PREFETCH - transferCount)));

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
                testPeer.expectDeclare(txnId);

                // Now the deferred close should be performed.
                testPeer.expectDetach(true, true, true);

                messageConsumer.close();
            } else {
                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
                testPeer.expectDeclare(txnId);
            }

            session.commit();

            if (closeConsumer && !closeBeforeCommit) {
                testPeer.expectDetach(true, true, true);

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                messageConsumer.close();
            }

            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConsumerWithNoMessageCanCloseBeforeCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            assertNull(messageConsumer.receiveNoWait());

            messageConsumer.close();

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the commit succeeded
            testPeer.expectDischarge(txnId, false);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);

            session.commit();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testConsumerWithNoMessageCanCloseBeforeRollback() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            assertNull(messageConsumer.receiveNoWait());

            messageConsumer.close();

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the commit succeeded
            testPeer.expectDischarge(txnId, true);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);

            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testProducedMessagesOnTransactedSessionCarryTxnId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId, true);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testProducedMessagesOnTransactedSessionCanBeReused() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which was sent under the current transaction. Check it carries
            // TransactionalState with the above txnId but has no outcome. Respond with a
            // TransactionalState with Accepted outcome.

            Message message = session.createMessage();

            for(int i = 0; i < 3; ++i) {
                TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
                messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
                messageMatcher.setMessageAnnotationsMatcher(new MessageAnnotationsSectionMatcher(true));

                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(nullValue());

                TransactionalState txState = new TransactionalState();
                txState.setTxnId(txnId);
                txState.setOutcome(new Accepted());

                testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

                message.setIntProperty("sequence", i);

                producer.send(message);
            }

            // Expect rollback on close without a commit call.
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithConsumerReceivingAllMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(1, 1, false);
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithConsumerReceivingAllMessagesThenCloses() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(1, 1, true);
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithConsumerReceivingSomeMessages() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(5, 2, false);
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithConsumerReceivingSomeMessagesThenCloses() throws Exception {
        doRollbackTransactedSessionWithConsumerTestImpl(5, 2, true);
    }

    private void doRollbackTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, boolean closeConsumer) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // Set to fixed known value to reduce breakage if defaults are changed.
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), transferCount);

            for (int i = 1; i <= consumeCount; i++) {
                // Then expect a *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);
            }

            MessageConsumer messageConsumer = session.createConsumer(queue);

            for (int i = 1; i <= consumeCount; i++) {
                Message receivedMessage = messageConsumer.receive(3000);

                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
            }

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
            testPeer.expectLinkFlow(true, true, greaterThan(UnsignedInteger.ZERO));

            if (closeConsumer) {

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDispositionThatIsReleasedAndSettled();
                }

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.expectDischarge(txnId, true);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
                testPeer.expectDeclare(txnId);

                // Now the deferred close should be performed.
                testPeer.expectDetach(true, true, true);

                messageConsumer.close();
            } else {
                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.expectDischarge(txnId, true);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
                testPeer.expectDeclare(txnId);

                // Expect the messages that were not consumed to be released
                int unconsumed = transferCount - consumeCount;
                for (int i = 1; i <= unconsumed; i++) {
                    testPeer.expectDisposition(true, new ReleasedMatcher());
                }

                // Expect the consumer to be 'started' again as rollback completes
                testPeer.expectLinkFlow(false, false, greaterThan(UnsignedInteger.ZERO));
            }

            testPeer.expectDischarge(txnId, true);
            session.rollback();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithPrefetchFullBeforeStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 5;
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + messageCount);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer and fill the prefetch with messages, which we wont consume any of
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), messageCount);

            session.createConsumer(queue);

            // Create a producer
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // The consumer will be 'stopped' prior to rollback, however we will NOT send a 'drain' Flow
            // frame as we have manipulated that all the credit was already used, i.e. it already stopped.

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            // Now expect an unsettled 'declare' transfer to the txn coordinator for the next txn, and
            // reply with a declared disposition state containing the txnId.
            txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            session.rollback();

            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testRollbackTransactedSessionWithPrefetchFullyUtilisedByDrainWhenStoppingConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final int messageCount = 5;
            Connection connection = testFixture.establishConnecton(testPeer, "?jms.prefetchPolicy.all=" + messageCount);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer, expect it to flow credit, but don't send it any messages
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(messageCount)));

            session.createConsumer(queue);

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();
            MessageProducer producer = session.createProducer(queue);

            // Expect the message which provoked creating the transaction
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            messageMatcher.setMessageAnnotationsMatcher( new MessageAnnotationsSectionMatcher(true));

            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());

            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);

            producer.send(session.createMessage());

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain' Flow.
            // Action the drain by filling the prefetch (which is equivalent to this having happened while
            // the Flow was in flight to the peer), and then DONT send a flow frame back to the client
            // as it can tell from the messages that all the credit has been used.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"),
                                                       messageCount, true, false, equalTo(UnsignedInteger.valueOf(messageCount)), 1, false);

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            session.rollback();

            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testDefaultOutcomeIsModifiedForConsumerSourceOnTransactedSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withAddress(equalTo(queueName));
            sourceMatcher.withDynamic(equalTo(false));
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));
            ModifiedMatcher outcomeMatcher = new ModifiedMatcher().withDeliveryFailed(equalTo(true)).withUndeliverableHere(nullValue());
            sourceMatcher.withDefaultOutcome(outcomeMatcher);

            testPeer.expectReceiverAttach(notNullValue(), sourceMatcher);
            testPeer.expectLinkFlow();
            testPeer.expectDischarge(txnId, true);
            session.createConsumer(queue);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testCoordinatorLinkSupportedOutcomes() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            // Expect session, then coordinator link
            testPeer.expectBegin();

            SourceMatcher sourceMatcher = new SourceMatcher();
            sourceMatcher.withOutcomes(arrayContaining(Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL));

            testPeer.expectCoordinatorAttach(sourceMatcher);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            connection.createSession(true, Session.SESSION_TRANSACTED);

            //Expect rollback on close
            testPeer.expectDischarge(txnId, true);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testRollbackErrorCoordinatorClosedOnCommit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});

            testPeer.expectDeclare(txnId1);
            testPeer.remotelyCloseLastCoordinatorLinkOnDischarge(txnId1, false, true, txnId2);
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId2);
            testPeer.expectDischarge(txnId2, true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.commit();
                fail("Transaction should have rolled back");
            } catch (TransactionRolledBackException ex) {
                LOG.info("Caught expected TransactionRolledBackException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testRollbackErrorWhenCoordinatorRemotelyClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            testPeer.remotelyCloseLastCoordinatorLink();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);

            testPeer.expectDischarge(txnId, true);

            try {
                session.commit();
                fail("Transaction should have rolled back");
            } catch (TransactionRolledBackException ex) {
                LOG.info("Caught expected TransactionRolledBackException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testJMSErrorCoordinatorClosedOnRollback() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});

            testPeer.expectDeclare(txnId1);
            testPeer.remotelyCloseLastCoordinatorLinkOnDischarge(txnId1, true, true, txnId2);
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId2);
            testPeer.expectDischarge(txnId2, true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.rollback();
                fail("Transaction should have rolled back");
            } catch (JMSException ex) {
                LOG.info("Caught expected JMSException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testJMSExceptionOnRollbackWhenCoordinatorRemotelyClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            testPeer.remotelyCloseLastCoordinatorLink();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            testPeer.waitForAllHandlersToComplete(2000);

            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);

            testPeer.expectDischarge(txnId, true);

            try {
                session.rollback();
                fail("Rollback should have thrown a JMSException");
            } catch (JMSException ex) {
                LOG.info("Caught expected JMSException");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSendAfterCoordinatorLinkClosedDuringTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a producer to use in provoking creation of the AMQP transaction
            testPeer.expectSenderAttach();

            // Close the link, the messages should now just get dropped on the floor.
            testPeer.remotelyCloseLastCoordinatorLink();

            MessageProducer producer = session.createProducer(queue);

            testPeer.waitForAllHandlersToComplete(2000);

            producer.send(session.createMessage());

            // Expect that a new link will be created in order to start the next TX.
            txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);

            // Expect that the session TX will rollback on close.
            testPeer.expectDischarge(txnId, true);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testReceiveAfterCoordinatorLinkClosedDuringTX() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            // Create a consumer and send it an initial message for receive to process.
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            // Close the link, the messages should now just get dropped on the floor.
            testPeer.remotelyCloseLastCoordinatorLink();

            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForAllHandlersToComplete(2000);

            // receiving the message would normally ack it, since the TX is failed this
            // should not result in a disposition going out.
            Message received = consumer.receive();
            assertNotNull(received);

            // Expect that a new link will be created in order to start the next TX.
            txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclare(txnId);

            // Expect that the session TX will rollback on close.
            testPeer.expectDischarge(txnId, true);

            try {
                session.commit();
                fail("Commit operation should have failed.");
            } catch (TransactionRolledBackException jmsTxRb) {
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSessionCreateFailsOnDeclareTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();
            testPeer.expectDeclareButDoNotRespond();
            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSessionCreateFailsOnDeclareRejection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a Rejected disposition state to indicate failure.
            testPeer.expectDeclareAndReject();
            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("should have thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testSessionCreateFailsOnCoordinatorLinkRefusal() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            // Expect coordinator link, refuse it, expect detach reply
            String errorMessage = "CoordinatorLinkRefusal-breadcrumb";
            testPeer.expectCoordinatorAttach(true, false, AmqpError.NOT_IMPLEMENTED, errorMessage);
            testPeer.expectDetach(true, false, false);

            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("should have thrown");
            } catch (JMSException jmse) {
                assertNotNull(jmse.getMessage());
                assertTrue("Expected exception message to contain breadcrumb", jmse.getMessage().contains(errorMessage));
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTransactionRolledBackOnSessionCloseTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            // Closed session should roll-back the TX with a failed discharge
            testPeer.expectDischargeButDoNotRespond(txnId, true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.close();
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTransactionRolledBackTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1);

            // Expect discharge but don't respond so that the request timeout kicks in and fails
            // the discharge.  The pipelined declare should arrive as well and be discharged as the
            // client attempts to recover to a known good state.
            testPeer.expectDischargeButDoNotRespond(txnId1, true);

            // Session should throw from the rollback and then try and recover.
            testPeer.expectDeclare(txnId2);
            testPeer.expectDischarge(txnId2, true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.rollback();
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTransactionCommitTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1);

            // Expect discharge but don't respond so that the request timeout kicks in and fails
            // the discharge.  The pipelined declare should arrive as well and we respond with
            // successful declare.
            testPeer.expectDischargeButDoNotRespond(txnId1, false);
            testPeer.expectDeclare(txnId2);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            try {
                session.commit();
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            // Session rolls back on close
            testPeer.expectDischarge(txnId2, true);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=20000)
    public void testTransactionCommitTimesOutAndNoNextBeginTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setRequestTimeout(500);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Binary txnId1 = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            Binary txnId2 = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            testPeer.expectDeclare(txnId1);

            // Expect discharge and don't respond so that the request timeout kicks in
            // Expect pipelined declare and don't response so that the request timeout kicks in.
            // The commit operation should throw a timed out exception at that point.
            testPeer.expectDischargeButDoNotRespond(txnId1, false);
            testPeer.expectDeclareButDoNotRespond();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            // After the pipelined operations both time out, the session should attempt to
            // recover by creating a new TX, then on close the session should roll it back
            testPeer.expectDeclare(txnId2);
            testPeer.expectDischarge(txnId2, true);

            try {
                session.commit();
                fail("Should have timed out waiting for declare.");
            } catch (JmsOperationTimedOutException jmsEx) {
            } catch (Throwable error) {
                fail("Should have caught an timed out exception:");
                LOG.error("Caught -> ", error);
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRollbackWithNoResponseForSuspendConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.drainTimeout=1000");
            connection.start();

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, new AmqpValueDescribedType("content"), 1);

            // Then expect a *settled* TransactionalState disposition for the message once received by the consumer
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(new AcceptedMatcher());

            testPeer.expectDisposition(true, stateMatcher);

            // Read one so we try to suspend on rollback
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);

            // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
            testPeer.expectLinkFlow(true, false, greaterThan(UnsignedInteger.ZERO));

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);

            // Then expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);
            testPeer.expectDischarge(txnId, true);

            try {
                session.rollback();
                fail("Should throw a timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {}

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout=30000)
    @Repeat(repetitions = 1)
    public void testConsumerMessageOrderOnTransactedSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final int messageCount = 10;

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), new Declared().setTxnId(txnId), true);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expect the browser enumeration to create a underlying consumer
            testPeer.expectReceiverAttach();
            // Expect initial credit to be sent, respond with some messages that are tagged with
            // a sequence number we can use to determine if order is maintained.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                messageCount, false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            for (int i = 1; i <= messageCount; i++) {
                // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < messageCount; ++i) {
                Message message = consumer.receive(500);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));
            }

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=60000)
    public void testConsumeManyWithSingleTXPerMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final int messageCount = 10;

            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            Deque<Binary> txnIdDeque = new ArrayDeque<>(3);
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4}));
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 2, (byte) 4, (byte) 6, (byte) 8}));
            txnIdDeque.offer(new Binary(new byte[]{ (byte) 5, (byte) 4, (byte) 3, (byte) 2}));

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = txnIdDeque.removeFirst();
            txnIdDeque.addLast(txnId);
            testPeer.expectDeclare(txnId);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expect the browser enumeration to create a underlying consumer
            testPeer.expectReceiverAttach();
            // Expect initial credit to be sent, respond with some messages that are tagged with
            // a sequence number we can use to determine if order is maintained.
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                messageCount, false, false, equalTo(UnsignedInteger.valueOf(JmsDefaultPrefetchPolicy.DEFAULT_QUEUE_PREFETCH)), 1, false, true);

            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < messageCount; i++) {
                // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
                stateMatcher.withTxnId(equalTo(txnId));
                stateMatcher.withOutcome(new AcceptedMatcher());

                testPeer.expectDisposition(true, stateMatcher);

                Message message = consumer.receive(500);
                assertNotNull(message);
                assertEquals(i, message.getIntProperty(TestAmqpPeer.MESSAGE_NUMBER));

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.expectDischarge(txnId, false);

                // Expect the next transaction to start.
                txnId = txnIdDeque.removeFirst();
                txnIdDeque.addLast(txnId);
                testPeer.expectDeclare(txnId);

                session.commit();
            }

            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with accepted and settled disposition to indicate the rollback succeeded
            testPeer.expectDischarge(txnId, true);
            testPeer.expectEnd();

            session.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
