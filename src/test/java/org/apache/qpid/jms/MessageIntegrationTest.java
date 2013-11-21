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
package org.apache.qpid.jms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.ApplicationPropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.ApplicationPropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

public class MessageIntegrationTest extends QpidJmsTestCase
{
    private final IntegrationTestFixture _testFixture = new IntegrationTestFixture();

    @Test
    public void testSendTextMessageWithApplicationProperties() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            String propertyName = "prop";
            boolean propertyValue = true;

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            ApplicationPropertiesSectionMatcher appPropsMatcher = new ApplicationPropertiesSectionMatcher(true);
            appPropsMatcher.withEntry(propertyName, equalTo(propertyValue));
            messageMatcher.setApplicationPropertiesMatcher(appPropsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(null));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage();
            message.setObjectProperty(propertyName, propertyValue);

            producer.send(message);
        }
    }

    @Test
    public void testReceiveTextMessageWithApplicationProperties() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);)
        {
            Connection connection = _testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            String propertyName = "name";
            String propertyValue = "value";
            ApplicationPropertiesDescribedType appProperties = new ApplicationPropertiesDescribedType();
            appProperties.setApplicationProperty(propertyName, propertyValue);
            DescribedType amqpValueNullContent = new AmqpValueDescribedType(null);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, appProperties, amqpValueNullContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(1000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            assertNull(((TextMessage)receivedMessage).getText());

            assertTrue(receivedMessage.propertyExists(propertyName));
            assertTrue(propertyValue.equals(receivedMessage.getObjectProperty(propertyName)));
        }
    }
}
