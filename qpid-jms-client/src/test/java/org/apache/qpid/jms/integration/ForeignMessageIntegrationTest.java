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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.message.foreign.ForeignJmsBytesMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsTextMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedDataMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class ForeignMessageIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testSendForeignBytesMessageWithContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            byte[] content = "myBytes".getBytes();

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(content)));

            testPeer.expectTransfer(messageMatcher);

            //Create a foreign message and send it
            ForeignJmsBytesMessage foreign = new ForeignJmsBytesMessage();
            foreign.writeBytes(content);

            producer.send(foreign);

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSentForeignMessageHasMessageId() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withMessageId(notNullValue(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);

            testPeer.expectTransfer(messageMatcher);

            ForeignJmsTextMessage foreign = new ForeignJmsTextMessage();

            producer.send(foreign);

            assertNotNull("JMSMessageID should not be null", foreign.getJMSMessageID());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Test that after sending a message with the disableMessageID hint set, which already had
     * a JMSMessageID value, that the message object then has a null JMSMessageID value, and no
     * message-id field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendForeignMessageWithDisableMessageIDHintAndExistingMessageID() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            byte[] content = "myBytes".getBytes();

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withMessageId(nullValue()); // Check there is no message-id value;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(new Binary(content)));

            testPeer.expectTransfer(messageMatcher);

            // Create a foreign message, [erroneously] set a JMSMessageID value
            ForeignJmsBytesMessage foreign = new ForeignJmsBytesMessage();
            foreign.writeBytes(content);

            assertNull("JMSMessageID should not yet be set", foreign.getJMSMessageID());
            String existingMessageId = "ID:this-should-be-overwritten-in-send";
            foreign.setJMSMessageID(existingMessageId);
            assertEquals("JMSMessageID should now be set", existingMessageId, foreign.getJMSMessageID());

            // Toggle the DisableMessageID hint, then send it
            producer.setDisableMessageID(true);
            producer.send(foreign);

            assertNull("JMSMessageID should now be null", foreign.getJMSMessageID());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testSendForeignMessageWithDeliveryDelay() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String topicName = "myTopic";

            // add connection capability to indicate server support for DELAYED-DELIVERY
            Connection connection = testFixture.establishConnecton(testPeer, new Symbol[]{ DELAYED_DELIVERY });

            connection.start();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            int deliveryDelay = 100000;
            long currentTime = System.currentTimeMillis();
            long deliveryTimeLower = currentTime + deliveryDelay;
            long deliveryTimeUpper = deliveryTimeLower + 5000;

            // Create matcher to expect the deliverytime annotation to be set to
            // a value greater than 'now'+deliveryDelay, within a delta for test execution.
            Matcher<Long> inRange = both(greaterThanOrEqualTo(deliveryTimeLower)).and(lessThanOrEqualTo(deliveryTimeUpper));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_DELIVERY_TIME, inRange);

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic dest = session.createTopic(topicName);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryDelay(deliveryDelay);

            // Create a foreign message, [erroneously] set a JMSDeliveryTime value, expect it to be overwritten
            ForeignJmsMessage foreign = new ForeignJmsMessage();
            assertEquals("JMSDeliveryTime should not yet be set", 0, foreign.getJMSDeliveryTime());
            foreign.setJMSDeliveryTime(1234);
            assertEquals("JMSDeliveryTime should now (erroneously) be set", 1234, foreign.getJMSDeliveryTime());

            // Now send the message, peer will verify the actual delivery time was set as expected
            producer.send(foreign);
            testPeer.waitForAllHandlersToComplete(3000);

            // Now verify the local message also has the deliveryTime set as expected
            MatcherAssert.assertThat("JMSDeliveryTime should now be set in expected range", foreign.getJMSDeliveryTime(), inRange);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

}
