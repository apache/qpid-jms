/*
 *
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
 *
 */
package org.apache.qpid.jms.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Queue;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpResourceRequest;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSentMessageToken;
import org.apache.qpid.jms.engine.TestAmqpMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SenderImplTest extends QpidJmsTestCase
{
    private static final String JMS_AMQP_TTL = "JMS_AMQP_TTL";
    private ConnectionImpl _mockConnection;
    private AmqpConnection _mockAmqpConnection;
    private AmqpSender _mockAmqpSender;
    private SessionImpl _mockSession;
    private Queue _mockQueue;
    private String _mockQueueName = "mockQueueName";

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockConnection = Mockito.mock(ConnectionImpl.class);
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        Mockito.when(_mockConnection.getAmqpConnection()).thenReturn(_mockAmqpConnection);
        _mockAmqpSender = Mockito.mock(AmqpSender.class);
        _mockSession = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSession.getDestinationHelper()).thenReturn(new DestinationHelper());
        Mockito.when(_mockSession.getMessageIdHelper()).thenReturn(new MessageIdHelper());

        _mockQueueName = "mockQueueName";
        _mockQueue = Mockito.mock(Queue.class);
        Mockito.when(_mockQueue.getQueueName()).thenReturn(_mockQueueName);
    }

    @Test
    public void testSenderOverriddesMessageDeliveryMode() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        //explicitly flip the DeliveryMode to NON_PERSISTENT, later verifying it gets changed back to PERSISTENT
        testMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, testMessage.getJMSDeliveryMode());

        senderImpl.send(testMessage);

        assertEquals(DeliveryMode.PERSISTENT, testMessage.getJMSDeliveryMode());
    }

    @Test
    public void testSenderSetsJMSDestinationOnMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertNull(testMessage.getJMSDestination());

        senderImpl.send(testMessage);

        assertSame(_mockQueue, testMessage.getJMSDestination());
    }

    @Test
    public void testSenderSetsJMSTimestampOnMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertEquals(0, testMessage.getJMSTimestamp());
        long timestamp = System.currentTimeMillis();

        senderImpl.send(testMessage);

        //verify the timestamp was set, allowing for a 3second delta
        assertEquals(timestamp, testMessage.getJMSTimestamp(), 3000);
    }

    @Test
    public void testSenderSetsJMSMessageIDOnMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertNull("JMSMessageID should not be set yet", testMessage.getJMSMessageID());

        senderImpl.send(testMessage);

        String msgId = testMessage.getJMSMessageID();
        assertNotNull("JMSMessageID should be set", msgId);
        assertTrue("MessageId does not have the expected prefix", msgId.startsWith(MessageIdHelper.JMS_ID_PREFIX));
    }

    @Test
    public void testSenderSetsJMSPriorityOnMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertEquals(Message.DEFAULT_PRIORITY, testMessage.getJMSPriority());

        int priority = 9;
        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, priority, Message.DEFAULT_TIME_TO_LIVE);

        //verify the priority used to send the message was set
        assertEquals("sender failed to set JMSPriority", priority, testMessage.getJMSPriority());
    }

    @Test
    public void testSenderSetsAbsoluteExpiryAndTtlFieldsOnUnderlyingMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertEquals(0, testMessage.getJMSTimestamp());
        long timestamp = System.currentTimeMillis();
        long ttl = 100_000;

        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);

        //verify the JMSExpiration is now set, allowing for a 3second delta
        assertEquals(timestamp + ttl, testMessage.getJMSExpiration(), 3000);

        //more specifically, check the creation-time, ttl, and absolute-expiry fields on the message are set
        Long creationTime = testMessage.getUnderlyingAmqpMessage(false).getCreationTime();
        assertNotNull(creationTime);
        assertEquals(timestamp, creationTime, 3000);

        Long underlyingTtl = testMessage.getUnderlyingAmqpMessage(false).getTtl();
        assertNotNull(underlyingTtl);
        assertEquals(ttl, underlyingTtl.longValue());

        Long absoluteExpiryTime = testMessage.getUnderlyingAmqpMessage(false).getAbsoluteExpiryTime();
        assertNotNull(absoluteExpiryTime);
        assertEquals(ttl + creationTime, absoluteExpiryTime.longValue());
    }

    @Test
    public void testSenderSetsTtlOnUnderlyingAmqpMessage() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertEquals(0, testMessage.getJMSTimestamp());
        long timestamp = System.currentTimeMillis();
        long ttl = 100_000;

        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, ttl);

        //verify the expiration was set, allowing for a 3second delta
        assertEquals(timestamp + ttl, testMessage.getJMSExpiration(), 3000);
    }

    @Test
    public void testSenderClearsExistingJMSExpirationAndTtlFieldOnUnderlyingAmqpMessageWhenNotUsingTtl() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Long oldTtl = 456L;
        testAmqpMessage.setTtl(oldTtl);

        long expiration = System.currentTimeMillis();
        testAmqpMessage.setAbsoluteExpiryTime(expiration);
        MessageImpl<?> testMessage = TestMessageImpl.createReceivedMessage(testAmqpMessage, _mockSession, _mockConnection, null);

        //verify JMSExpiration is non-zero
        assertEquals(expiration, testMessage.getJMSExpiration());

        //send the message without any TTL
        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        //verify the expiration was cleared, along with the underlying amqp message fields
        assertEquals(0, testMessage.getJMSExpiration());
        assertNull(testMessage.getUnderlyingAmqpMessage(false).getTtl());
        assertNull(testMessage.getUnderlyingAmqpMessage(false).getAbsoluteExpiryTime());
    }

    @Test
    public void testSenderUsesJMS_AMQP_TTLPropertyToSetUnderlyingTtlFieldWhenNoProducerTTLInEffect() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        Long ttlPropValue = 789L;
        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, _mockConnection);
        testMessage.setLongProperty(JMS_AMQP_TTL, ttlPropValue);

        //send the message without any TTL
        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

        //verify the expiration was clear
        assertEquals(0, testMessage.getJMSExpiration());
        assertNull(testMessage.getUnderlyingAmqpMessage(false).getAbsoluteExpiryTime());

        //Verify that the underlying amqp message ttl field was set to the value from the property
        assertEquals(ttlPropValue, testMessage.getUnderlyingAmqpMessage(false).getTtl());
    }

    @Test
    public void testSenderUsesJMS_AMQP_TTLPropertyToSetUnderlyingTtlFieldWhenProducerTTLInEffect() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        long timestamp = System.currentTimeMillis();
        Long ttlPropValue = 789L;
        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, _mockConnection);
        testMessage.setLongProperty(JMS_AMQP_TTL, ttlPropValue);

        //send the message with a different TTL
        long timeToLive = 999999L;
        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, timeToLive);

        //verify the expiration was set, allowing a delta
        Long jmsExpiration = testMessage.getJMSExpiration();
        assertEquals(timestamp + timeToLive, jmsExpiration, 3000);
        assertEquals(jmsExpiration, testMessage.getUnderlyingAmqpMessage(false).getAbsoluteExpiryTime());

        //Verify that the underlying amqp message ttl field was set to the value from the property, not the producer
        assertEquals(ttlPropValue, testMessage.getUnderlyingAmqpMessage(false).getTtl());
    }

    @Test
    public void testSenderUsesJMS_AMQP_TTLPropertyValueZeroToClearUnderlyingTtlField() throws Exception
    {
        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        long timestamp = System.currentTimeMillis();
        Long ttlPropValue = 0L;
        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, _mockConnection);
        testMessage.setLongProperty(JMS_AMQP_TTL, ttlPropValue);

        //send the message with a different TTL
        long timeToLive = 999999L;
        senderImpl.send(testMessage, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, timeToLive);

        //verify the expiration was set, allowing a delta
        Long jmsExpiration = testMessage.getJMSExpiration();
        assertEquals(timestamp + timeToLive, jmsExpiration, 3000);
        assertEquals(jmsExpiration, testMessage.getUnderlyingAmqpMessage(false).getAbsoluteExpiryTime());

        //Verify that the underlying amqp message ttl field was NOT set, as requested by the property value being 0
        assertNull(testMessage.getUnderlyingAmqpMessage(false).getTtl());
    }

    /**
     * Test that the producer sets the JMSXUserID property with the
     * user name for the connection the message is being sent on.
     */
    @Test
    public void testSenderSetsJMSUserIDOnMessage() throws Exception
    {
        senderSetsJMSXUserIDOnMessageTestImpl(true);
    }

    /**
     * Test that the producer does not set the JMSXUserID property on the message
     * when the {@link ClientProperties#QPID_SET_JMSXUSERID_ON_SEND} system property
     * is set false.
     */
    @Test
    public void testDisableSenderSettingJMSXUserIDOnMessageUsingSystemProperty() throws Exception
    {
        senderSetsJMSXUserIDOnMessageTestImpl(false);
    }

    private void senderSetsJMSXUserIDOnMessageTestImpl(boolean enabled) throws Exception
    {
        if(!enabled)
        {
            setTestSystemProperty(ClientProperties.QPID_SET_JMSXUSERID_ON_SEND, "false");
        }

        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        String myUserId = "testUser";
        Mockito.when(_mockConnection.getUserName()).thenReturn(myUserId);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        assertNull("expected JMSUserID to be null", testMessage.getStringProperty(ClientProperties.JMSXUSERID));

        senderImpl.send(testMessage);

        if(enabled)
        {
            //verify the sender stamped the message with the user
            assertTrue("JMSXUserID property should exist", testMessage.propertyExists(ClientProperties.JMSXUSERID));
            assertEquals("sender failed to set JMSXUserID", myUserId, testMessage.getStringProperty(ClientProperties.JMSXUSERID));
        }
        else
        {
            //verify the sender didnt stamp the message with the user
            assertNull("expected JMSXUserID to still be null", testMessage.getStringProperty(ClientProperties.JMSXUSERID));
            assertFalse("JMSXUserID property should not exist", testMessage.propertyExists(ClientProperties.JMSXUSERID));
        }
    }

    /**
     * Test that the producer clears any existing JMSXUserID value on a message
     * when the {@link ClientProperties#QPID_SET_JMSXUSERID_ON_SEND} system property
     * is set false.
     */
    @Test
    public void testSenderClearsExistingJMSXUserIDValueOnSendWhenSetOnSendIsDisabled() throws Exception
    {
        //disable setting the user-id on send
        setTestSystemProperty(ClientProperties.QPID_SET_JMSXUSERID_ON_SEND, "false");

        basePreparationForMockSending();

        SenderImpl senderImpl = new SenderImpl(_mockSession, _mockConnection, _mockAmqpSender, _mockQueue);

        String myUserId = "testUser";
        Mockito.when(_mockConnection.getUserName()).thenReturn(myUserId);

        MessageImpl<?> testMessage = TestMessageImpl.createNewMessage(_mockSession, null);

        testMessage.setStringProperty(ClientProperties.JMSXUSERID, "value");
        assertNotNull("expected JMSUserID to be null", testMessage.getStringProperty(ClientProperties.JMSXUSERID));

        senderImpl.send(testMessage);

        //verify the sender cleared the existing value
        String value = testMessage.getStringProperty(ClientProperties.JMSXUSERID);
        assertNull("expected JMSXUserID be null, but was: " + value, value);
        assertFalse("JMSXUserID property should not exist", testMessage.propertyExists(ClientProperties.JMSXUSERID));
    }

    private void basePreparationForMockSending() throws JmsTimeoutException, JmsInterruptedException, IOException
    {
        //Create mock request, ensuring that it immediately returns as completed
        AmqpResourceRequest<?> mockRequest = Mockito.mock(AmqpResourceRequest.class);
        AmqpSentMessageToken _mockToken = Mockito.mock(AmqpSentMessageToken.class);
        Mockito.when(_mockAmqpSender.sendMessage(Mockito.any(AmqpMessage.class), Mockito.any(AmqpResourceRequest.class))).thenReturn(_mockToken);
        Mockito.when(mockRequest.getResult()).thenReturn(null);
        ImmediateWaitUntil.mockWaitUntil(_mockConnection);
    }

}
