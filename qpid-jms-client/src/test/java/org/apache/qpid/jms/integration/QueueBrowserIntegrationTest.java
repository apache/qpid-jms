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

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declare;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declared;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

public class QueueBrowserIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=30000)
    public void testCreateQueueBrowserWithoutEnumeration() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectEnd();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Creating the browser should send nothing until an Enumeration is created.
            QueueBrowser browser = session.createBrowser(queue);
            browser.close();

            session.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testCreateQueueBrowserAndEnumeration() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer and send credit.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testCreateQueueBrowseAutoAckSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expected the browser to create a consumer and send credit, once hasMoreElements
            // is called a message that is received should be accepted when the session is in
            // Auto acknowledge mode.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertTrue(queueView.hasMoreElements());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testCreateQueueBrowseTransactedSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            // Expected the browser to create a consumer and send credit, once hasMoreElements
            // is called a message that is received should be accepted when the session is in
            // a transacted session, the browser should not participate.
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertTrue(queueView.hasMoreElements());

            // Browser should close without delay as it does not participate in the TX
            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testCreateQueueBrowseClientAckSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expected the browser to create a consumer and send credit, once hasMoreElements
            // is called a message that is received should be accepted when the session is in
            // Auto acknowledge mode.
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent);
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertTrue(queueView.hasMoreElements());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
