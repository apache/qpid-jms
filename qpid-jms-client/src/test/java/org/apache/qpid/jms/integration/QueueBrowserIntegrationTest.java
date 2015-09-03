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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsPrefetchPolicy;
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
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matchers;
import org.junit.Test;

public class QueueBrowserIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    //----- Test basic create and destroy mechanisms -------------------------//

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
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    //----- Tests for expected behaviors of a QueueBrowser implementation ----//

    @Test(timeout=30000)
    public void testQueueBrowserNextElementWithNoMessage() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer and send credit.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertNull(queueView.nextElement());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testQueueBrowserPrefetchOne() throws IOException, Exception {
        final DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            JmsConnection jmsConnection = (JmsConnection) connection;
            jmsConnection.getPrefetchPolicy().setAll(1);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer and send credit, then drain it when
            // no message arrives before hasMoreElements is called, at which point we send one.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.ONE));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                1, true, true, Matchers.equalTo(UnsignedInteger.ONE), 1, true, false);
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.ONE));
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);

            assertTrue(queueView.hasMoreElements());
            assertNotNull(queueView.nextElement());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    //----- Tests that cover QueueBrowser and Session Ack mode interaction ---//

    @Test(timeout=30000)
    public void testCreateQueueBrowserAutoAckSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expected the browser to create a consumer and send credit, then drain it when
            // no message arrives before hasMoreElements is called, at which point we send one.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                1, true, true, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)), 1, true, false);
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
    public void testCreateQueueBrowserClientAckSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            // Expected the browser to create a consumer and send credit, then drain it when
            // no message arrives before hasMoreElements is called, at which point we send one.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                1, true, true, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)), 1, true, false);
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertTrue(queueView.hasMoreElements());

            browser.close();

            testPeer.expectEnd();
            session.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testCreateQueueBrowserTransactedSession() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            testPeer.expectSenderAttach(txCoordinatorMatcher, false, false);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(false, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)));
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent,
                1, true, true, equalTo(UnsignedInteger.valueOf(JmsPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH)), 1, true, false);

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4});
            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));
            testPeer.expectTransfer(declareMatcher, nullValue(), false, new Declared().setTxnId(txnId), true);

            // Expected the browser to create a consumer and send credit, once hasMoreElements
            // is called a message that is received should be delivered when the session is in
            // a transacted session, the browser should not participate and close when asked.
            testPeer.expectLinkFlow();
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

    //----- Tests that cover QueueBrowser when prefetch is zero --------------//

    @Test(timeout=30000)
    public void testCreateQueueBrowserAndEnumerationZeroPrefetch() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            JmsConnection jmsConnection = (JmsConnection) connection;
            jmsConnection.getPrefetchPolicy().setAll(0);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer and NOT send credit.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testQueueBrowserHasMoreElementsZeroPrefetchNoMessage() throws IOException, Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            JmsConnection jmsConnection = (JmsConnection) connection;
            jmsConnection.getPrefetchPolicy().setAll(0);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer, then drain with 1 credit.
            testPeer.expectQueueBrowserAttach();
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.ONE));
            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertFalse(queueView.hasMoreElements());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout=30000)
    public void testQueueBrowserHasMoreElementsZeroPrefetchDrainedMessage() throws IOException, Exception {
        DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            JmsConnection jmsConnection = (JmsConnection) connection;
            jmsConnection.getPrefetchPolicy().setAll(0);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Expected the browser to create a consumer.
            testPeer.expectQueueBrowserAttach();

            // When hasMoreElements is called, the browser should drain with 1 credit,
            // at which point we send it a message to use all the credit.
            testPeer.expectLinkFlowRespondWithTransfer(
                null, null, null, null, amqpValueNullContent, 1, true,
                false, equalTo(UnsignedInteger.ONE), 1, true, false);

            // Next attempt should not get a message, we just send a response flow indicating
            // the credit was cleared, triggering a false on hasMoreElemets.
            testPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.ONE));

            testPeer.expectDetach(true, true, true);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> queueView = browser.getEnumeration();
            assertNotNull(queueView);
            assertTrue(queueView.hasMoreElements());
            Message message = (Message) queueView.nextElement();
            assertNotNull(message);
            assertFalse(queueView.hasMoreElements());

            browser.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
