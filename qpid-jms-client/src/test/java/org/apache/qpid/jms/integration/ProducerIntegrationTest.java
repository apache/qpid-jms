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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.message.foreign.ForeignJmsMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageIdHelper;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.ListDescribedType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.Modified;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransactionalState;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransactionalStateMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(QpidJMSTestRunner.class)
public class ProducerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout = 20000)
    public void testCloseSender() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.close();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseSenderTimesOut() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectDetach(true, false, true);
            testPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            try {
                producer.close();
                fail("Should have thrown a timed out exception");
            } catch (JmsOperationTimedOutException jmsEx) {
                LOG.info("Caught excpected exception", jmsEx);
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSentTextMessageCanBeModified() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            producer.send(message);

            assertEquals(text, message.getText());
            message.setText(text + text);
            assertEquals(text + text, message.getText());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testDefaultDeliveryModeProducesDurableMessages() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            producer.send(message);
            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testProducerOverridesMessageDeliveryMode() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message, explicitly setting the deliveryMode on the
            // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
            // that the producer ignores this value and sends the message as PERSISTENT(/durable)
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();
            message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());

            producer.send(message);

            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent and the producer is set to send as NON_PERSISTENT
     * the resulting sent message has durable false, in this case due to setting the
     * header field false (header is only being sent due to Priority also being set).
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentProducerSetDurableFalse() throws Exception {
        doSendingMessageNonPersistentTestImpl(false, true, true);
    }


    //As above but with an anonymous producer.
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentProducerSetDurableFalseAnonymousProducer() throws Exception {
        doSendingMessageNonPersistentTestImpl(true, true, true);
    }

    /**
     * Test that when a message is sent and the send is passed NON_PERSISTENT delivery mode
     * the resulting sent message has durable false, in this case due to setting the
     * header field false (header is only being sent due to Priority also being set).
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentSendSetDurableFalse() throws Exception {
        doSendingMessageNonPersistentTestImpl(false, true, false);
    }

    //As above but with an anonymous producer.
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentSendSetDurableFalseAnonymousProducer() throws Exception {
        doSendingMessageNonPersistentTestImpl(true, true, false);
    }

    /**
     * Test that when a message is sent and the producer is set to send as NON_PERSISTENT
     * the resulting sent message has durable false, in this case due to omitting the
     * header section due to it having all default values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentProducerOmitsHeader() throws Exception {
        doSendingMessageNonPersistentTestImpl(false, false, true);
    }

    //As above but with an anonymous producer.
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentProducerOmitsHeaderAnonymousProducer() throws Exception {
        doSendingMessageNonPersistentTestImpl(true, false, true);
    }

    /**
     * Test that when a message is sent and the send is passed NON_PERSISTENT delivery mode
     * the resulting sent message has durable false, in this case due to omitting the
     * header section due to it having all default values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentSendOmitsHeader() throws Exception {
        doSendingMessageNonPersistentTestImpl(false, false, false);
    }

    //As above but with an anonymous producer.
    @Test(timeout = 20000)
    public void testSendingMessageNonPersistentSendOmitsHeaderAnonymousProducer() throws Exception {
        doSendingMessageNonPersistentTestImpl(true, false, false);
    }

    private void doSendingMessageNonPersistentTestImpl(boolean anonymousProducer, boolean setPriority, boolean setOnProducer) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            //Add capability to indicate support for ANONYMOUS-RELAY
            Symbol[] serverCapabilities = new Symbol[]{ANONYMOUS_RELAY};
            Connection connection = testFixture.establishConnecton(testPeer, serverCapabilities);

            testPeer.expectBegin();

            String queueName = "myQueue";
            TargetMatcher targetMatcher = new TargetMatcher();
            if(anonymousProducer) {
                targetMatcher.withAddress(nullValue());
            } else {
                targetMatcher.withAddress(equalTo(queueName));
            }

            testPeer.expectSenderAttach(targetMatcher, false, false);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            MessageProducer producer;
            if(anonymousProducer) {
                producer = session.createProducer(null);
            } else {
                producer = session.createProducer(queue);
            }

            byte priority = 5;
            String text = "myMessage";
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            if(setPriority) {
                MessageHeaderSectionMatcher headerMatcher = new MessageHeaderSectionMatcher(true);
                headerMatcher.withDurable(equalTo(false));
                headerMatcher.withPriority(equalTo(UnsignedByte.valueOf(priority)));

                messageMatcher.setHeadersMatcher(headerMatcher);
            }
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            if(setOnProducer) {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                if(setPriority) {
                    producer.setPriority(priority);
                }

                if(anonymousProducer) {
                    producer.send(queue, message);
                } else {
                    producer.send(message);
                }
            } else {
                if(anonymousProducer) {
                    producer.send(queue, message, DeliveryMode.NON_PERSISTENT, setPriority ? priority : Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                } else {
                    producer.send(message, DeliveryMode.NON_PERSISTENT, setPriority ? priority : Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                }
            }

            assertEquals("Should have NON_PERSISTENT delivery mode set", DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent the JMSDestination header is set to
     * the Destination used by the producer.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSDestination() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            producer.send(message);

            assertEquals("Should have had JMSDestination set", queue, message.getJMSDestination());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSTimestamp() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            long currentTime = System.currentTimeMillis();
            Date creationLower = new Date(currentTime);
            Date creationUpper = new Date(currentTime + 3000);
            Matcher<Date> inRange = both(greaterThanOrEqualTo(creationLower)).and(lessThanOrEqualTo(creationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withCreationTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that after sending a message with the disableMessageTimestamp hint set, the
     * message object has a 0 JMSTimestamp value, and no creation-time field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageTimestampHint() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withCreationTime(nullValue()); // Check there is no creation-time value;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertEquals("JMSTimestamp should not yet be set", 0, message.getJMSTimestamp());

            producer.setDisableMessageTimestamp(true);
            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);

            assertEquals("JMSTimestamp should still not be set", 0, message.getJMSTimestamp());
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSExpirationRelatedAbsoluteExpiryAndTtlFields() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            long currentTime = System.currentTimeMillis();
            long ttl = 100_000;

            Date expirationLower = new Date(currentTime + ttl);
            Date expirationUpper = new Date(currentTime + ttl + 5000);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            Matcher<Date> inRange = both(greaterThanOrEqualTo(expirationLower)).and(lessThanOrEqualTo(expirationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            headersMatcher.withTtl(equalTo(UnsignedInteger.valueOf(ttl)));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withAbsoluteExpiryTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageWithJMS_AMQP_TTLSetPositive() throws Exception {
        sendingMessageWithJMS_AMQP_TTLSetTestImpl(100_000, 20_000);
    }

    @Test(timeout = 20000)
    public void testSendingMessageWithJMS_AMQP_TTLSetZero() throws Exception {
        sendingMessageWithJMS_AMQP_TTLSetTestImpl(50_000, 0);
    }

    public void sendingMessageWithJMS_AMQP_TTLSetTestImpl(long jmsTtl, long amqpTtl) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            long currentTime = System.currentTimeMillis();
            Date expirationLower = new Date(currentTime + jmsTtl);
            Date expirationUpper = new Date(currentTime + jmsTtl + 3000);

            // Create matcher to expect the absolute-expiry-time field of the properties section to
            // be set to a value greater than 'now'+ttl, within a delta.
            Matcher<Date> inRange = both(greaterThanOrEqualTo(expirationLower)).and(lessThanOrEqualTo(expirationUpper));

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            headersMatcher.withDurable(equalTo(true));
            // verify the ttl field matches the JMS_AMQP_TTL value, rather than the standard JMS send TTL value.
            if (amqpTtl == 0) {
                headersMatcher.withTtl(nullValue());
            } else {
                headersMatcher.withTtl(equalTo(UnsignedInteger.valueOf(amqpTtl)));
            }
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withAbsoluteExpiryTime(inRange);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);
            message.setLongProperty(AmqpMessageSupport.JMS_AMQP_TTL, amqpTtl);

            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, jmsTtl);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    /**
     * Test that when a message is sent with default priority of 4, the emitted AMQP message has no value in the header
     * priority field, since the default for that field is already 4.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testDefaultPriorityProducesMessagesWithoutPriorityField() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withPriority(equalTo(null));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message);

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that when a message is sent with a non-default priority, the emitted AMQP message has that value in the
     * header priority field, and the JMS message has had JMSPriority set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsJMSPriority() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            byte priority = 5;

            // Create and transfer a new message
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true)
                    .withPriority(equalTo(UnsignedByte.valueOf(priority)));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage();

            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

            producer.send(message, DeliveryMode.PERSISTENT, priority, Message.DEFAULT_TIME_TO_LIVE);

            assertEquals(priority, message.getJMSPriority());

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /**
     * Test that upon sending a message, the sender sets the JMSMessageID on the Message object,
     * and that the value is included in the AMQP message sent by the client.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSMessageID() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a string, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);
            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected string message id to be sent", receivedMessageId instanceof String);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.equals(receivedMessageId));
        }
    }

    @Test(timeout=20000)
    public void testSendingMessageWithUUIDStringMessageIdFormat() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=UUID_STRING";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));
            String noIdPrefix = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_NO_PREFIX;
            assertTrue("The 'No ID prefix' encoding hint was not found", jmsMessageID.startsWith(noIdPrefix));

            connection.close();
            testPeer.waitForAllHandlersToComplete(1000);

            // Get the value that was actually transmitted/received, verify it is a String,
            // verify it is only the UUID toString and has no "ID", check the encoded
            // JMSMessageID value that we have locally.
            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            String expected = jmsMessageID.substring(noIdPrefix.length());
            UUID.fromString(expected);
            assertTrue("Expected String message id to be sent", receivedMessageId instanceof String);
            assertEquals("Expected UUID toString value to be present in AMQP message", expected, receivedMessageId);
        }
    }

    @Test(timeout=20000)
    public void testSendingMessageWithUUIDMessageIdFormat() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=UUID";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(UUID.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));
            String uuidEncodingPrefix = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX;
            assertTrue("The 'UUID prefix' encoding hint was not found", jmsMessageID.startsWith(uuidEncodingPrefix));

            connection.close();

            // Get the value that was actually transmitted/received, verify it is a UUID, compare to what we have locally
            testPeer.waitForAllHandlersToComplete(1000);

            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            assertTrue("Expected UUID message id to be sent", receivedMessageId instanceof UUID);
            assertTrue("Expected JMSMessageId value to be present in AMQP message", jmsMessageID.endsWith(receivedMessageId.toString()));
        }
    }

    @Test(timeout=20000)
    public void testSendingMessageWithPrefixedUUIDStringMessageIdFormat() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // DONT create a test fixture, we will drive everything directly.
            String uri = "amqp://127.0.0.1:" + testPeer.getServerPort() + "?jms.messageIDPolicy.messageIDType=PREFIXED_UUID_STRING";
            JmsConnectionFactory factory = new JmsConnectionFactory(uri);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withMessageId(isA(String.class));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            producer.send(message);

            String jmsMessageID = message.getJMSMessageID();
            assertNotNull("JMSMessageID should be set", jmsMessageID);
            assertTrue("JMS 'ID:' prefix not found", jmsMessageID.startsWith("ID:"));

            connection.close();
            testPeer.waitForAllHandlersToComplete(1000);

            // Get the value that was actually transmitted/received, verify it is a String,
            // verify it is the "ID:" prefix followed by the UUID toString, check the
            // JMSMessageID value that we have locally matches exactly.
            Object receivedMessageId = propsMatcher.getReceivedMessageId();

            String uuidToString = jmsMessageID.substring("ID:".length());
            UUID.fromString(uuidToString);
            assertTrue("Expected String message id to be sent", receivedMessageId instanceof String);
            assertEquals("Expected UUID toString value to be present in AMQP message", jmsMessageID, receivedMessageId);
        }
    }

    /**
     * Test that after sending a message with the disableMessageID hint set, the message
     * object has a null JMSMessageID value, and no message-id field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageIDHint() throws Exception {
        doSendingMessageWithDisableMessageIDHintTestImpl(false);
    }

    /**
     * Test that after sending a message with the disableMessageID hint set, which already had
     * a JMSMessageID value, that the message object then has a null JMSMessageID value, and no
     * message-id field value was set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testSendingMessageWithDisableMessageIDHintAndExistingMessageID() throws Exception {
        doSendingMessageWithDisableMessageIDHintTestImpl(true);
    }

    private void doSendingMessageWithDisableMessageIDHintTestImpl(boolean existingId) throws JMSException, InterruptedException, Exception, IOException {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            propsMatcher.withMessageId(nullValue()); // Check there is no message-id value;
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            Message message = session.createTextMessage(text);

            assertNull("JMSMessageID should not yet be set", message.getJMSMessageID());

            if (existingId) {
                // [erroneously] set a JMSMessageID value
                String existingMessageId = "ID:this-should-be-overwritten-in-send";
                message.setJMSMessageID(existingMessageId);
                assertEquals("JMSMessageID should now be set", existingMessageId, message.getJMSMessageID());
            }

            producer.setDisableMessageID(true);
            producer.send(message);

            assertNull("JMSMessageID should be null", message.getJMSMessageID());

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseProducer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch producerClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onProducerClosed(MessageProducer producer, Throwable exception) {
                    producerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end it afterwards.
            testPeer.expectSenderAttach();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_DELETED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            // Verify the producer gets marked closed
            testPeer.waitForAllHandlersToComplete(1000);
            assertTrue("producer never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        producer.getDestination();
                    } catch (Exception ex) {
                        if (ex instanceof IllegalStateException && ex.getCause() != null) {
                            String message = ex.getCause().getMessage();
                            if (message.contains(AmqpError.RESOURCE_DELETED.toString()) &&
                                message.contains(BREAD_CRUMB)) {
                                return true;
                            }
                        }

                        LOG.debug("Caught unexpected exception: {}", ex);
                    }

                    return false;
                }
            }, 10000, 10));

            assertTrue("Producer closed callback didn't trigger", producerClosed.await(10, TimeUnit.SECONDS));

            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseProducerDuringSyncSend() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, give it credit.
            testPeer.expectSenderAttach();

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));

            // Expect a message to be sent, but don't send a disposition in
            // response, simply remotely close the producer instead.
            testPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, false);
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage(text);

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseProducerWithSendWaitingForCredit() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, don't give it credit.
            testPeer.expectSenderAttachWithoutGrantingCredit();

            // Producer has no credit so the send should block waiting for it, then fail when the remote close occurs
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, "Producer closed", 50);
            testPeer.expectClose();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown due to close of producer");
            } catch (ResourceAllocationException rae) {
                // Expected if remote close beat the send to the provider
            } catch (IllegalStateException ise) {
                // Can happen if send fires before remote close if processed.
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndProducerCompletesAsyncSends() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final CountDownLatch producerClosed = new CountDownLatch(1);
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onProducerClosed(MessageProducer producer, Throwable exception) {
                    producerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer, then remotely end the session afterwards.
            testPeer.expectSenderAttach();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            final int MSG_COUNT = 3;

            for (int i = 0; i < MSG_COUNT; ++i) {
                testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            }

            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB, 50);

            TestJmsCompletionListener listener = new TestJmsCompletionListener(MSG_COUNT);
            try {
                for (int i = 0; i < MSG_COUNT; ++i) {
                    Message message = session.createTextMessage("content");
                    producer.send(message, listener);
                }
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            testPeer.waitForAllHandlersToComplete(2000);

            // Verify the sends gets marked errored
            assertTrue(listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertEquals(MSG_COUNT, listener.errorCount);

            // Verify the producer gets marked closed
            assertTrue("Producer closed callback didn't trigger", producerClosed.await(5, TimeUnit.SECONDS));
            try {
                producer.getDeliveryMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String errorMessage = jmsise.getCause().getMessage();
                assertTrue(errorMessage.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(errorMessage.contains(BREAD_CRUMB));
            }


            // Try closing it explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            producer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSyncSend() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, give it credit.
            testPeer.expectSenderAttach();

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));

            // Expect a message to be sent, but don't send a disposition in
            // response, simply remotely close the connection instead.
            testPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, false);
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage(text);

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionAndDropDuringSyncSend() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Expect producer creation, give it credit.
            testPeer.expectSenderAttach();

            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));

            // Expect a message to be sent, but don't send a disposition in
            // response, simply remotely close the connection instead.
            testPeer.expectTransfer(messageMatcher, nullValue(), false, false, null, false);
            testPeer.remotelyCloseConnection(true, AmqpError.RESOURCE_LIMIT_EXCEEDED, BREAD_CRUMB);
            testPeer.dropAfterLastHandler();

            Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage(text);

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForAllHandlersToComplete(3000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsDelayed() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer, "?amqp.traceFrames=true&amqp.traceBytes=true");
            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach. Delay sending credit when it does.
            testPeer.expectSenderAttach(100);

            testPeer.expectTransfer(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);

            producer.send(message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWhenLinkCreditIsZeroAndTimeout() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");

            // Expect the producer to attach. Don't send any credit so that the client will
            // block on a send and we can test our timeouts.
            testPeer.expectSenderAttachWithoutGrantingCredit();
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);

            try {
                producer.send(message);
                fail("Send should time out.");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught expected error: {}", jmsEx.getMessage());
            } catch (Throwable error) {
                fail("Send should time out, but got: " + error.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendTimesOutWhenNoDispostionArrives() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response for which should cause the
            // send operation to time out.
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);

            try {
                producer.send(message);
                fail("Send should time out.");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught expected error: {}", jmsEx.getMessage());
            } catch (Throwable error) {
                fail("Send should time out, but got: " + error.getMessage());
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionGetsTimedOutErrorWhenNoDispostionArrives() throws Exception {
        try(TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            Message message = session.createTextMessage("text");
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response for which should cause the
            // send operation to time out.
            testPeer.expectSenderAttach();
            testPeer.expectTransferButDoNotRespond(messageMatcher);
            testPeer.expectClose();

            MessageProducer producer = session.createProducer(queue);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            try {
                producer.send(message, listener);
            } catch (Throwable error) {
                LOG.info("Caught unexpected error: {}", error.getMessage());
                fail("Send should not fail for async.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertTrue(listener.exception instanceof JmsSendTimedOutException);
            assertNotNull(listener.message);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageRejected() throws Exception {
        doSyncSendMessageNotAcceptedTestImpl(new Rejected());
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageReleased() throws Exception {
        doSyncSendMessageNotAcceptedTestImpl(new Released());
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedDeliveryFailed() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setUndeliverableHere(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testSyncSendMessageModifiedDeliveryFailedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(true);

        doSyncSendMessageNotAcceptedTestImpl(modified);
    }

    private void doSyncSendMessageNotAcceptedTestImpl(ListDescribedType responseState) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createTextMessage("content");

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), responseState, true);
            testPeer.expectClose();

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            try {
                producer.send(message);
                fail("Expected an exception to be thrown");
            } catch (JMSException e) {
                //TODO: more explicit exception type?
                // Expected
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageRejected() throws Exception {
        doAsyncSendMessageNotAcceptedTestImpl(new Rejected());
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageReleased() throws Exception {
        doAsyncSendMessageNotAcceptedTestImpl(new Released());
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedDeliveryFailed() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setUndeliverableHere(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncSendMessageModifiedDeliveryFailedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(true);

        doAsyncSendMessageNotAcceptedTestImpl(modified);
    }

    private void doAsyncSendMessageNotAcceptedTestImpl(ListDescribedType responseState) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            final CountDownLatch asyncError = new CountDownLatch(1);

            connection.setForceAsyncSend(true);
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    LOG.debug("ExceptionListener got error: {}", exception.getMessage());
                    asyncError.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Create a second producer which allows for a safe wait for credit for the
            // first producer without the need for a sleep.  Otherwise the first producer
            // might not do an actual async send due to not having received credit yet.
            session.createProducer(queue);

            Message message = session.createTextMessage("content");

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), responseState, true);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            try {
                producer.send(message);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Should get a non-fatal error", asyncError.await(10, TimeUnit.SECONDS));

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            try {
                producer.send(message);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksWhenConnectionNotStarted() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectDetach(true, true, true);
            producer.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksAfterConnectionStopped() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            connection.stop();

            producer.send(session.createMessage());

            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.close();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreditDrainedAfterSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(500);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(destination);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            // After the first send lets drain off the remaining credit from the sender
            testPeer.expectTransferRespondWithDrain(messageMatcher, 1);
            testPeer.expectLinkFlow(true, false, Matchers.equalTo(UnsignedInteger.ZERO));
            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            producer.send(session.createMessage());

            // We don't have any credit now since we were drained, so the send should
            // block until more credit is issued.
            try {
                producer.send(session.createMessage());
                fail("Should have timed out waiting for credit to send.");
            } catch (JmsSendTimedOutException jmsEx) {
                LOG.info("Caught expected send timeout.");
            }

            producer.close();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdSetWhenConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSetWhenNotConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenNotConfiguredForInclusion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = session.createMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private class CustomForeignMessage extends ForeignJmsMessage {

        @Override
        public Enumeration<?> getPropertyNames() throws JMSException {
            Enumeration<?> properties = super.getPropertyNames();

            Set<Object> names = new HashSet<Object>();
            while (properties.hasMoreElements()) {
                names.add(properties.nextElement());
            }

            names.add("JMSXUserID");

            return Collections.enumeration(names);
        }

        @Override
        public Object getObjectProperty(String name) throws JMSException {
            if (name.equals("JMSXUserID")) {
                return "spoofed";
            }

            return message.getObjectProperty(name);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenConfiguredForInclusionWithForgeinMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(true);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            Binary binaryUserId = new Binary(user.getBytes(Charset.forName("UTF-8")));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(equalTo(binaryUserId));
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = new CustomForeignMessage();
            message.setStringProperty("JMSXUserID", "spoofed");

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testUserIdNotSpoofedWhenNotConfiguredForInclusionWithForeignMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // Expect a PLAIN connection
            String user = "user";
            String pass = "qwerty123456";

            testPeer.expectSaslPlain(user, pass);
            testPeer.expectOpen();
            testPeer.expectBegin();
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JmsConnectionFactory factory = new JmsConnectionFactory(
                "amqp://localhost:" + testPeer.getServerPort());
            factory.setPopulateJMSXUserID(false);

            Connection connection = factory.createConnection(user, pass);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("TestQueue");
            MessageProducer producer = session.createProducer(queue);

            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withUserId(nullValue());
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = new CustomForeignMessage();
            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendFailsWhenDelayedDeliveryIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // DO NOT add capability to indicate server support for DELAYED-DELIVERY

            Connection connection = testFixture.establishConnecton(testPeer);

            connection.start();

            testPeer.expectBegin();

            Matcher<Symbol[]> desiredCapabilitiesMatcher = arrayContaining(new Symbol[] { DELAYED_DELIVERY });
            Symbol[] offeredCapabilities = null;
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryDelay(5000);

            // Producer should fail to send when message has delivery delay since remote
            // did not report that it supports that option.
            Message message = session.createMessage();
            try {
                producer.send(message);
                fail("Send should fail");
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
            }

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksWhenDelayedDeliveryIsSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String topicName = "myTopic";

            // add connection capability to indicate server support for DELAYED-DELIVERY
            Connection connection = testFixture.establishConnecton(testPeer, new Symbol[]{ DELAYED_DELIVERY });

            connection.start();

            testPeer.expectBegin();

            int deliveryDelay = 100000;
            long currentTime = System.currentTimeMillis();
            long deliveryTimeLower = currentTime + deliveryDelay;
            long deliveryTimeUpper = deliveryTimeLower + 5000;

            // Create matcher to expect the deliverytime annotation to be set to
            // a value greater than 'now'+deliveryDelay, within a delta for test execution.
            Matcher<Long> inRange = both(greaterThanOrEqualTo(deliveryTimeLower)).and(lessThanOrEqualTo(deliveryTimeUpper));

            Matcher<Object> desiredCapabilitiesMatcher = nullValue();
            Symbol[] offeredCapabilities = null;
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_DELIVERY_TIME, inRange);

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic dest = session.createTopic(topicName);

            // Create a message, [erroneously] set a JMSDeliveryTime value, expect it to be overwritten
            Message message = session.createMessage();
            assertEquals("JMSDeliveryTime should not yet be set", 0, message.getJMSDeliveryTime());
            message.setJMSDeliveryTime(1234);
            assertEquals("JMSDeliveryTime should now (erroneously) be set", 1234, message.getJMSDeliveryTime());

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryDelay(deliveryDelay);

            // Now send the message, peer will verify the actual delivery time was set as expected
            producer.send(message);

            testPeer.waitForAllHandlersToComplete(3000);

            // Now verify the local message also has the deliveryTime set as expected
            MatcherAssert.assertThat("JMSDeliveryTime should now be set in expected range", message.getJMSDeliveryTime(), inRange);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendWorksWhenDelayedDeliveryIsSupportedOnlyLinkCapability() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            String topicName = "myTopic";

            // DONT add connection capability to indicate support for DELAYED-DELIVERY
            Connection connection = testFixture.establishConnecton(testPeer, new Symbol[]{ });

            connection.start();

            testPeer.expectBegin();

            Matcher<Symbol[]> desiredCapabilitiesMatcher = arrayContaining(new Symbol[] { DELAYED_DELIVERY });
            Symbol[] offeredCapabilities = new Symbol[] { DELAYED_DELIVERY };
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_DELIVERY_TIME, notNullValue());

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic dest = session.createTopic(topicName);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryDelay(5000);
            producer.send(session.createMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionAfterSendMessageGetDispoation() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionGetsNotifiedWhenProducerClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            // Use a send timeout to trigger the completion event
            connection.setSendTimeout(500);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            testPeer.expectDetach(true, true, true);
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);
            producer.close();

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionGetsNotifiedWhenSessionClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            testPeer.expectEnd();
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);

            session.close();

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionGetsNotifiedWhenConnectionClosed() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);

            connection.close();

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionResetsBytesMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            Binary payload = new Binary(new byte[] {1, 2, 3, 4});
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload.getArray());

            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof BytesMessage);

            BytesMessage completed = (BytesMessage) listener.message;
            assertEquals(payload.getLength(), completed.getBodyLength());
            byte[] data = new byte[payload.getLength()];
            completed.readBytes(data);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMessageRejected() throws Exception {
        doAsyncCompletionSendMessageNotAcceptedTestImpl(new Rejected());
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMessageReleased() throws Exception {
        doAsyncCompletionSendMessageNotAcceptedTestImpl(new Released());
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMessageModifiedDeliveryFailed() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);

        doAsyncCompletionSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMessageModifiedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setUndeliverableHere(true);

        doAsyncCompletionSendMessageNotAcceptedTestImpl(modified);
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMessageModifiedDeliveryFailedUndeliverable() throws Exception {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(true);

        doAsyncCompletionSendMessageNotAcceptedTestImpl(modified);
    }

    private void doAsyncCompletionSendMessageNotAcceptedTestImpl(ListDescribedType responseState) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            final CountDownLatch asyncError = new CountDownLatch(1);

            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    LOG.debug("ExceptionListener got error: {}", exception.getMessage());
                    asyncError.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create a second producer which allows for a safe wait for credit for the
            // first producer without the need for a sleep.  Otherwise the first producer
            // might not do an actual async send due to not having received credit yet.
            session.createProducer(queue);

            Message message = session.createTextMessage("content");

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), responseState, true);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSessionCloseThrowsIllegalStateException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            final AtomicReference<JMSException> closeError = new AtomicReference<JMSException>(null);
            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    try {
                        session.close();
                    } catch (JMSException jmsEx) {
                        closeError.set(jmsEx);
                    }

                    super.onCompletion(message);
                };
            };

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);
            assertNotNull(closeError.get());
            assertTrue(closeError.get() instanceof IllegalStateException);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionConnectionCloseThrowsIllegalStateException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            testPeer.expectTransfer(new TransferPayloadCompositeMatcher());
            testPeer.expectClose();

            final AtomicReference<JMSException> closeError = new AtomicReference<JMSException>(null);
            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener() {

                @Override
                public void onCompletion(Message message) {

                    try {
                        connection.close();
                    } catch (JMSException jmsEx) {
                        closeError.set(jmsEx);
                    }

                    super.onCompletion(message);
                };
            };

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertNotNull(closeError.get());
            assertTrue(closeError.get() instanceof IllegalStateException);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSessionCommitThrowsIllegalStateException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            testPeer.expectSenderAttach();

            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());
            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            final AtomicReference<JMSException> commitError = new AtomicReference<JMSException>(null);
            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener() {

                @Override
                public void onCompletion(Message message) {

                    try {
                        session.commit();
                    } catch (JMSException jmsEx) {
                        commitError.set(jmsEx);
                    }

                    super.onCompletion(message);
                };
            };

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertNotNull(commitError.get());
            assertTrue(commitError.get() instanceof IllegalStateException);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSessionRollbackThrowsIllegalStateException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin();
            testPeer.expectCoordinatorAttach();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            Binary txnId = new Binary(new byte[]{ (byte) 5, (byte) 6, (byte) 7, (byte) 8});
            testPeer.expectDeclare(txnId);

            testPeer.expectSenderAttach();

            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create and transfer a new message
            String text = "myMessage";
            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            TransactionalStateMatcher stateMatcher = new TransactionalStateMatcher();
            stateMatcher.withTxnId(equalTo(txnId));
            stateMatcher.withOutcome(nullValue());
            TransactionalState txState = new TransactionalState();
            txState.setTxnId(txnId);
            txState.setOutcome(new Accepted());

            testPeer.expectTransfer(messageMatcher, stateMatcher, txState, true);
            testPeer.expectDischarge(txnId, true);
            testPeer.expectClose();

            final AtomicReference<JMSException> rollback = new AtomicReference<JMSException>(null);
            TextMessage message = session.createTextMessage(text);
            TestJmsCompletionListener listener = new TestJmsCompletionListener() {

                @Override
                public void onCompletion(Message message) {

                    try {
                        session.rollback();
                    } catch (JMSException jmsEx) {
                        rollback.set(jmsEx);
                    }

                    super.onCompletion(message);
                };
            };

            producer.send(message, listener);

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNull(listener.exception);
            assertNotNull(listener.message);
            assertNotNull(rollback.get());
            assertTrue(rollback.get() instanceof IllegalStateException);

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAnonymousProducerSendFailureHandledWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher, nullValue(), new Rejected(), true);
            testPeer.expectDetach(true, true, true);

            Message message = session.createMessage();
            try {
                producer.send(dest, message);
                fail("Send should fail");
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
            }

            //Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAnonymousProducerAsyncSendFailureHandledWhenAnonymousRelayNodeIsNotSupported() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {

            // DO NOT add capability to indicate server support for ANONYMOUS-RELAY

            Connection connection = testFixture.establishConnecton(testPeer, "?jms.forceAsyncSend=true");

            connection.start();

            testPeer.expectBegin();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String topicName = "myTopic";
            Topic dest = session.createTopic(topicName);

            // Expect no AMQP traffic when we create the anonymous producer, as it will wait
            // for an actual send to occur on the producer before anything occurs on the wire

            //Create an anonymous producer
            MessageProducer producer = session.createProducer(null);
            assertNotNull("Producer object was null", producer);

            // Expect a new message sent by the above producer to cause creation of a new
            // sender link to the given destination, then closing the link after the message is sent.
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(equalTo(topicName));
            targetMatcher.withDynamic(equalTo(false));
            targetMatcher.withDurable(equalTo(TerminusDurability.NONE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true);
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);

            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher, nullValue(), new Rejected(), true);
            testPeer.expectDetach(true, true, true);

            // Producer should act as synchronous regardless of asynchronous send setting.
            Message message = session.createMessage();
            try {
                producer.send(dest, message);
                fail("Send should fail");
            } catch (JMSException jmsEx) {
                LOG.debug("Caught expected error from failed send.");
            }

            //Repeat the send and observe another attach->transfer->detach.
            testPeer.expectSenderAttach(targetMatcher, false, false);
            testPeer.expectTransfer(messageMatcher);
            testPeer.expectDetach(true, true, true);

            producer.send(dest, message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSDeliveryTimeWithDelay() throws Exception {
        doSendingMessageSetsJMSDeliveryTimeTestImpl(true);
    }

    @Test(timeout = 20000)
    public void testSendingMessageSetsJMSDeliveryTimeWithoutDelay() throws Exception {
        doSendingMessageSetsJMSDeliveryTimeTestImpl(false);
    }

    private void doSendingMessageSetsJMSDeliveryTimeTestImpl(boolean deliveryDelay) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            // add connection capability to indicate server support for DELAYED-DELIVERY
            Connection connection = testFixture.establishConnecton(testPeer, new Symbol[]{ DELAYED_DELIVERY });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            int delay = 0;
            if (deliveryDelay) {
                delay = 123456;
                producer.setDeliveryDelay(delay);
            }

            // Create matcher to expect the DeliveryTime to be set to a value
            // representing 'now' [+ delivery-delay], within a upper delta for execution time.
            long deliveryTimeLower = System.currentTimeMillis();
            long deliveryTimeUpper = deliveryTimeLower + delay + 3000;
            Matcher<Long> inRange = both(greaterThanOrEqualTo(deliveryTimeLower)).and(lessThanOrEqualTo(deliveryTimeUpper));
            Symbol DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");

            String text = "myMessage";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            if (deliveryDelay) {
                msgAnnotationsMatcher.withEntry(DELIVERY_TIME, inRange);
            }
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(text));
            testPeer.expectTransfer(messageMatcher);

            Message message = session.createTextMessage(text);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);

            if (!deliveryDelay) {
                assertFalse("Message should not have delivery time annotation",
                        msgAnnotationsMatcher.keyExistsInReceivedAnnotations(DELIVERY_TIME));
            }

            assertThat(message.getJMSDeliveryTime(), inRange);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseOneProducerDoesNotCompleteAsyncSendFromAnotherProducer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.expectSenderAttach();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            MessageProducer producer = session.createProducer(queue);

            // Create a second producer which allows for a safe wait for credit for the
            // first producer without the need for a sleep.  Otherwise the first producer
            // might not do an actual async send due to not having received credit yet.
            session.createProducer(queue);

            Message message = session.createTextMessage("content");
            message.setIntProperty("test", 1);

            testPeer.expectTransferButDoNotRespond(new TransferPayloadCompositeMatcher());

            // This closes link for the second producer we created, not the one that we
            // will use to send a message.
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
            } catch (JMSException e) {
                LOG.warn("Caught unexpected error: {}", e.getMessage());
                fail("No expected exception for this send.");
            }

            testPeer.waitForAllHandlersToComplete(2000);

            assertFalse("Should not get async callback", listener.awaitCompletion(10, TimeUnit.MILLISECONDS));

            // Closing the session should complete the send with an exception
            testPeer.expectEnd();
            session.close();

            assertTrue("Did not get async callback", listener.awaitCompletion(5, TimeUnit.SECONDS));
            assertNotNull(listener.exception);
            assertNotNull(listener.message);
            assertTrue(listener.message instanceof TextMessage);

            // Message should be readable
            assertNotNull("Should have a readable JMSDestination", message.getJMSDestination());
            assertEquals("Message body not as expected", "content", ((TextMessage) message).getText());
            assertEquals("Message property not as expected", 1, message.getIntProperty("test"));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseProducerAndAttemptAsyncCompletionSendThrowsAndLeavesMessageReadable() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            final CountDownLatch producerClosed = new CountDownLatch(1);

            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onProducerClosed(MessageProducer producer, Throwable cause) {
                    producerClosed.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.remotelyDetachLastOpenedLinkOnLastOpenedSession(true, true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Message message = session.createTextMessage("content");
            message.setIntProperty("test", 1);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            MessageProducer producer = session.createProducer(queue);
            testPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Producer should have been closed", producerClosed.await(2, TimeUnit.SECONDS));

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
                fail("Expected exception to be thrown for this send.");
            } catch (JMSException e) {
                LOG.trace("Caught expected exception: {}", e.getMessage());
            }

            assertFalse("Should not get async callback", listener.awaitCompletion(5, TimeUnit.MILLISECONDS));

            // Message should be readable but not carry a destination as it wasn't actually sent anywhere
            assertNull("Should not have a readable JMSDestination", message.getJMSDestination());
            assertEquals("Message body not as expected", "content", ((TextMessage) message).getText());
            assertEquals("Message property not as expected", 1, message.getIntProperty("test"));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseSessionAndAttemptAsyncCompletionSendThrowsAndLeavesMessageReadable() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            final CountDownLatch sessionClosed = new CountDownLatch(1);

            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onSessionClosed(Session session, Throwable cause) {
                    sessionClosed.countDown();
                }
            });

            testPeer.expectBegin();
            testPeer.expectSenderAttach();
            testPeer.remotelyEndLastOpenedSession(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            Message message = session.createTextMessage("content");
            message.setIntProperty("test", 1);

            assertNull("Should not yet have a JMSDestination", message.getJMSDestination());

            MessageProducer producer = session.createProducer(queue);
            testPeer.waitForAllHandlersToComplete(2000);

            assertTrue("Session should have been closed", sessionClosed.await(2, TimeUnit.SECONDS));

            TestJmsCompletionListener listener = new TestJmsCompletionListener();
            try {
                producer.send(message, listener);
                fail("Expected exception to be thrown for this send.");
            } catch (JMSException e) {
                LOG.trace("Caught expected exception: {}", e.getMessage());
            }

            assertFalse("Should not get async callback", listener.awaitCompletion(5, TimeUnit.MILLISECONDS));

            // Message should be readable but not carry a destination as it wasn't actually sent anywhere
            assertNull("Should not have a readable JMSDestination", message.getJMSDestination());
            assertEquals("Message body not as expected", "content", ((TextMessage) message).getText());
            assertEquals("Message property not as expected", 1, message.getIntProperty("test"));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
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

    @Test(timeout = 20000)
    public void testFailedSendToOfflineConnectionMessageCanBeResentToNewConnection() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();) {

            final CountDownLatch connectionFailed = new CountDownLatch(1);
            final String text = "my-message-body-text";

            //----- Initial connection expectations and failure instructions
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(originalPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {

                @Override
                public void onConnectionFailure(Throwable error) {
                    connectionFailed.countDown();
                }
            });

            originalPeer.expectBegin();
            originalPeer.expectSenderAttach();
            originalPeer.dropAfterLastHandler();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TextMessage message = session.createTextMessage(text);
            Queue queue = session.createQueue("myQueue");

            // initial producer which will be sent to after connection fails
            MessageProducer producer = session.createProducer(queue);

            assertTrue("Connection should have been remotely closed", connectionFailed.await(10, TimeUnit.SECONDS));

            try {
                producer.send(message);
                fail("Should have failed to send to failed connection.");
            } catch (JMSException jmsEx) {
            }

            // --- Post Reconnection Expectations of this test

            // Reconnect to another peer and send the message object again which should work
            // without need to reset the message or otherwise account for past send failure.
            connection = (JmsConnection) testFixture.establishConnecton(finalPeer);

            finalPeer.expectBegin();
            finalPeer.expectSenderAttach();
            finalPeer.expectTransfer(new TransferPayloadCompositeMatcher(), nullValue(), new Accepted(), true);
            finalPeer.expectClose();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue("myQueue");
            producer = session.createProducer(queue);
            producer.send(message);
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
