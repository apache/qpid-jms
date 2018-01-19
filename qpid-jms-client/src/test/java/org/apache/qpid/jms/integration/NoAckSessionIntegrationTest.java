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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.ListDescribedType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.DescribedType;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test for Session that has been created with a supported NoAck Session Mode
 */
public class NoAckSessionIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    public void testNoAckSessionDoesNotPresettleProducers() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();

            Session session = connection.createSession(false, 100);

            Destination destination = session.createQueue("MyQueue");

            testPeer.expectSenderAttach();

            MessageProducer producer = session.createProducer(destination);

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            Matcher<?> stateMatcher = nullValue();
            ListDescribedType responseState = new Accepted();

            // Expect an unsettled transfer and respond with acceptance.
            testPeer.expectTransfer(messageMatcher, stateMatcher, false, true, responseState, true);

            Message message = session.createTextMessage();

            producer.send(message);

            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //----- Test the NoAck Session Mode for consumers ------------------------//

    @Test(timeout = 20000)
    public void testNoAckSessionAppliedToTopic() throws Exception {
        doTestConsumerWithPresettleOptions(100, Topic.class);
    }

    @Test(timeout = 20000)
    public void testNoAckSessionAppliedToTopicAltMode() throws Exception {
        doTestConsumerWithPresettleOptions(257, Topic.class);
    }

    @Test(timeout = 20000)
    public void testNoAckSessionAppliedToQueue() throws Exception {
        doTestConsumerWithPresettleOptions(100, Queue.class);
    }

    @Test(timeout = 20000)
    public void testNoAckSessionAppliedToTempTopic() throws Exception {
        doTestConsumerWithPresettleOptions(100, TemporaryTopic.class);
    }

    @Test(timeout = 20000)
    public void testNoAckSessionAppliedToTempQueue() throws Exception {
        doTestConsumerWithPresettleOptions(100, TemporaryQueue.class);
    }

    //----- Test Method implementation ---------------------------------------//

    private void doTestConsumerWithPresettleOptions(int ackMode, Class<? extends Destination> destType) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();
            testPeer.expectBegin();

            Session session = connection.createSession(false, ackMode);

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

            testPeer.expectSettledReceiverAttach();

            // Send a settled transfer, client should not send any dispositions
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueNullContent, true);

            MessageConsumer consumer = session.createConsumer(destination);
            assertNotNull(consumer.receive(3000));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
