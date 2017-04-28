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
package org.apache.qpid.jms.provider.amqp.message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsMessageFacadeTest extends AmqpJmsMessageTypesTestCase  {

    private static final String TEST_PROP_A = "TEST_PROP_A";
    private static final String TEST_PROP_B = "TEST_PROP_B";
    private static final String TEST_VALUE_STRING_A = "TEST_VALUE_STRING_A";
    private static final String TEST_VALUE_STRING_B = "TEST_VALUE_STRING_B";
    private static final long MAX_UINT = 0xFFFFFFFFL;

    // ====== AMQP Header Section =======
    // ==================================

    /**
     * To satisfy the JMS requirement that messages are durable by default, the
     * {@link AmqpJmsMessageFacade} objects created for sending new messages are
     * populated with a header section with durable set to true.
     */
    @Test
    public void testNewMessageHasUnderlyingHeaderSectionWithDurableTrue() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNotNull("Expected message to have Header section", amqpMessageFacade.getHeader());
        assertTrue("Durable not as expected", amqpMessageFacade.getAmqpHeader().isDurable());
    }

    // --- ttl field  ---

    @Test(expected = MessageFormatException.class)
    public void testSetAmqpTimeToLiveRejectsNegatives() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setAmqpTimeToLiveOverride(-1L);
    }

    @Test(expected = MessageFormatException.class)
    public void testSetAmqpTimeToLiveRejectsValuesFromTwoToThirtyTwo() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        // check values over 2^32 - 1 are rejected
        amqpMessageFacade.setAmqpTimeToLiveOverride(0X100000000L);
    }

    @Test
    public void testNewMessageHasUnderlyingHeaderSectionWithNoTtlSet() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNotNull("Expected message to have Header section", amqpMessageFacade.getHeader());
        assertNull("Ttl field should not be set", amqpMessageFacade.getHeader().getTtl());
    }

    @Test
    public void testGetTtlSynthesizedExpirationOnReceivedMessageWithTtlButNoAbsoluteExpiration() throws JMSException {
        Long ttl = 123L;

        Message message = Proton.message();
        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        message.setHeader(header);

        long start = System.currentTimeMillis();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        long end = System.currentTimeMillis();

        long expiration = amqpMessageFacade.getExpiration();

        assertTrue("Should have sythesized expiration based on current time + ttl", start + ttl <= expiration);
        assertTrue("Should have sythesized expiration based on current time + ttl", expiration <= end + ttl);

        long expiration2 = amqpMessageFacade.getExpiration();
        assertEquals("Second retrieval should return same result", expiration, expiration2);

        amqpMessageFacade = amqpMessageFacade.copy();
        long expiration3 = amqpMessageFacade.getExpiration();
        assertEquals("Thrid retrieval from copy should return same result", expiration, expiration3);
    }

    @Test
    public void testSetGetTtlOverrideOnNewMessage() throws Exception {
        long ttl = 123L;

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertFalse("Should not have a ttl override", amqpMessageFacade.hasAmqpTimeToLiveOverride());
        assertEquals(0, amqpMessageFacade.getAmqpTimeToLiveOverride());

        amqpMessageFacade.setAmqpTimeToLiveOverride(ttl);

        assertTrue("Should have a ttl override", amqpMessageFacade.hasAmqpTimeToLiveOverride());
        assertEquals(ttl, amqpMessageFacade.getAmqpTimeToLiveOverride());
        // check value on underlying TTL field is NOT set
        assertNull("TTL field on underlying message should NOT be set", amqpMessageFacade.getHeader().getTtl());
    }

    @Test
    public void testOnSendClearsTtlOnMessageReceivedWithTtl() throws Exception {
        Message message = Proton.message();
        int origTtl = 5;
        message.setTtl(origTtl);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("TTL has been unset already", origTtl, message.getTtl());

        amqpMessageFacade.onSend(0);

        // check value on underlying TTL field is NOT set
        assertEquals("TTL has not been cleared", 0, amqpMessageFacade.getAmqpHeader().getTimeToLive());
        assertNull("Underlying Header should be null, no values set to non-defaults", amqpMessageFacade.getHeader());
    }

    @Test
    public void testOnSendOverridesTtlOnMessageReceivedWithTtl() throws Exception {
        Message message = Proton.message();
        int origTtl = 5;
        int newTtl = 10;
        message.setTtl(origTtl);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("TTL has been unset already", origTtl, message.getTtl());

        amqpMessageFacade.onSend(newTtl);

        // check value on underlying TTL field is NOT set
        assertEquals("TTL has not been overriden", newTtl, amqpMessageFacade.getAmqpHeader().getTimeToLive());
        assertEquals("TTL field on underlying message should be set", UnsignedInteger.valueOf(newTtl), amqpMessageFacade.getHeader().getTtl());
    }

    @Test
    public void testOnSendOverridesProviderTtlWithSpecifiedOverrideTtl() throws Exception {
        Message message = Proton.message();
        int overrideTtl = 5;
        int producerTtl = 10;

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        amqpMessageFacade.setAmqpTimeToLiveOverride((long) overrideTtl);

        amqpMessageFacade.onSend(producerTtl);

        // check value on underlying TTL field is set to the override
        assertEquals("TTL has not been overriden", overrideTtl, amqpMessageFacade.getAmqpHeader().getTimeToLive());
    }

    // --- delivery count  ---

    @Test
    public void testGetDeliveryCountIs1ForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        // JMS delivery count starts at one.
        assertEquals("expected delivery count value not found", 1, amqpMessageFacade.getDeliveryCount());

        // Redelivered state inferred from delivery count
        assertFalse(amqpMessageFacade.isRedelivered());
        assertEquals(0, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testGetDeliveryCountForReceivedMessageWithNoHeader() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull("expected no header section to exist", message.getHeader());
        // JMS delivery count starts at one.
        assertEquals("expected delivery count value not found", 1, amqpMessageFacade.getDeliveryCount());

        // Redelivered state inferred from delivery count
        assertFalse(amqpMessageFacade.isRedelivered());
        assertEquals(0, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testGetDeliveryCountForReceivedMessageWithHeaderButNoDeliveryCount() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        Header header = new Header();
        message.setHeader(header);

        // JMS delivery count starts at one.
        assertEquals("expected delivery count value not found", 1, amqpMessageFacade.getDeliveryCount());

        // Redelivered state inferred from delivery count
        assertFalse(amqpMessageFacade.isRedelivered());
        assertEquals(0, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testGetDeliveryCountForReceivedMessageWithHeaderWithDeliveryCount() {
        Message message = Proton.message();
        Header header = new Header();
        header.setDeliveryCount(new UnsignedInteger(1));
        message.setHeader(header);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // JMS delivery count starts at one.
        assertEquals("expected delivery count value not found", 2, amqpMessageFacade.getDeliveryCount());

        // Redelivered state inferred from delivery count
        assertTrue(amqpMessageFacade.isRedelivered());
        assertEquals(1, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testSetRedeliveredAltersDeliveryCount() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // Redelivered state inferred from delivery count
        assertFalse(amqpMessageFacade.isRedelivered());
        assertEquals(0, amqpMessageFacade.getRedeliveryCount());;

        amqpMessageFacade.setRedelivered(true);
        assertTrue(amqpMessageFacade.isRedelivered());
        assertEquals(1, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testSetRedeliveredWhenAlreadyRedeliveredDoesNotChangeDeliveryCount() {
        Message message = Proton.message();
        Header header = new Header();
        header.setDeliveryCount(new UnsignedInteger(1));
        message.setHeader(header);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // Redelivered state inferred from delivery count
        assertTrue(amqpMessageFacade.isRedelivered());
        assertEquals(1, amqpMessageFacade.getRedeliveryCount());;

        amqpMessageFacade.setRedelivered(true);
        assertTrue(amqpMessageFacade.isRedelivered());
        assertEquals(1, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testSetRedeliveredFalseClearsDeliveryCount() {
        Message message = Proton.message();
        Header header = new Header();
        header.setDeliveryCount(new UnsignedInteger(1));
        message.setHeader(header);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // Redelivered state inferred from delivery count
        assertTrue(amqpMessageFacade.isRedelivered());
        assertEquals(1, amqpMessageFacade.getRedeliveryCount());;

        amqpMessageFacade.setRedelivered(false);
        assertFalse(amqpMessageFacade.isRedelivered());
        assertEquals(0, amqpMessageFacade.getRedeliveryCount());;
    }

    @Test
    public void testSetRedeliveryCountToZeroWhenNoHeadersNoNPE() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        assertNull("expected no header section to exist", message.getHeader());
        amqpMessageFacade.setRedeliveryCount(0);
    }

    // --- priority field  ---

    @Test
    public void testGetPriorityIs4ForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertEquals("expected priority value not found", 4, amqpMessageFacade.getPriority());
    }

    /**
     * When messages have no header section, the AMQP spec says the priority has default value of 4.
     */
    @Test
    public void testGetPriorityIs4ForReceivedMessageWithNoHeader() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull("expected no header section to exist", message.getHeader());
        assertEquals("expected priority value not found", 4, amqpMessageFacade.getPriority());
    }

    /**
     * When messages have a header section, but lack the priority field,
     * the AMQP spec says the priority has default value of 4.
     */
    @Test
    public void testGetPriorityIs4ForReceivedMessageWithHeaderButWithoutPriority() {
        Message message = Proton.message();

        Header header = new Header();
        message.setHeader(header);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected priority value not found", 4, amqpMessageFacade.getPriority());
    }

    /**
     * When messages have a header section, which have a priority value, ensure it is returned.
     */
    @Test
    public void testGetPriorityForReceivedMessageWithHeaderWithPriority() {
        // value over 10 deliberately
        byte priority = 7;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected priority value not found", priority, amqpMessageFacade.getPriority());
    }

    /**
     * When messages have a header section, which has a priority value just above the
     * JMS range of 0-9, ensure it is constrained to 9.
     */
    @Test
    public void testGetPriorityForReceivedMessageWithPriorityJustAboveJmsRange() {
        // value just over 9 deliberately
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected priority value not found", 9, amqpMessageFacade.getPriority());
    }

    /**
     * When messages have a header section, which has a priority value above the
     * JMS range of 0-9 and also outside the signed byte range (given AMQP
     * allowing ubyte priority), ensure it is constrained to 9.
     */
    @Test
    public void testGetPriorityForReceivedMessageWithPriorityAboveSignedByteRange() {
        String priorityString = String.valueOf(255);

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priorityString));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("expected priority value not found", 9, amqpMessageFacade.getPriority());
    }

    /**
     * Test that setting the Priority to a non-default value results in the underlying
     * message field being populated appropriately, and the value being returned from the Getter.
     */
    @Test
    public void testSetGetNonDefaultPriorityForNewMessage() {
        byte priority = 6;

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setPriority(priority);

        assertEquals("expected priority value not found", priority, amqpMessageFacade.getPriority());
        assertEquals("expected priority value not found", UnsignedByte.valueOf(priority), amqpMessageFacade.getHeader().getPriority());
    }

    /**
     * Test that setting the Priority below the JMS range of 0-9 resuls in the underlying
     * message field being populated with the value 0.
     */
    @Test
    public void testSetPriorityBelowJmsRangeForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setPriority(-1);

        assertEquals("expected priority value not found", 0, amqpMessageFacade.getPriority());
        assertEquals("expected priority value not found", UnsignedByte.valueOf((byte) 0), amqpMessageFacade.getHeader().getPriority());
    }

    /**
     * Test that setting the Priority above the JMS range of 0-9 resuls in the underlying
     * message field being populated with the value 9.
     */
    @Test
    public void testSetPriorityAboveJmsRangeForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setPriority(11);

        assertEquals("expected priority value not found", 9, amqpMessageFacade.getPriority());
        assertEquals("expected priority value not found", UnsignedByte.valueOf((byte) 9), amqpMessageFacade.getHeader().getPriority());
    }

    /**
     * Test that setting the Priority to the default value on a message with no
     * header section does not result in creating the header section.
     */
    @Test
    public void testSetDefaultPriorityForMessageWithoutHeaderSection() {
        // Using a received message as new messages to send are set durable by default, which creates the header
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull("expected no header section to exist", message.getHeader());

        amqpMessageFacade.setPriority(Message.DEFAULT_PRIORITY);

        assertNull("expected no header section to exist", message.getHeader());
        assertEquals("expected priority to be default", Message.DEFAULT_PRIORITY, amqpMessageFacade.getPriority());
    }

    /**
     * Receive message which has a header section with a priority value. Ensure the headers
     * underlying field value is cleared when the priority is set to the default priority of 4.
     */
    @Test
    public void testSetPriorityToDefaultOnReceivedMessageWithPriorityClearsPriorityField() {
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        amqpMessageFacade.setPriority(Message.DEFAULT_PRIORITY);

        //check the expected value is still returned
        assertEquals("expected priority value not returned", Message.DEFAULT_PRIORITY, amqpMessageFacade.getPriority());

        //check the underlying header field was actually cleared rather than set to Message.DEFAULT_PRIORITY
        assertNull("underlying header priority field was not cleared", amqpMessageFacade.getHeader());
    }

    // ====== AMQP Properties Section =======
    // ======================================

    @Test
    public void testNewMessageHasNoUnderlyingPropertiesSection() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        assertNull(amqpMessageFacade.getProperties());
    }

    // --- group-id field ---

    @Test
    public void testGetGroupIdIsNullForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("expected GroupId to be null on new message", amqpMessageFacade.getGroupId());
    }

    /**
     * Check that setting GroupId null on a new message does not cause creation of the underlying properties
     * section. New messages lack the properties section section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetGroupIdNullOnNewMessageDoesNotCreatePropertiesSection() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setGroupId(null);

        assertNull("properties section was created", amqpMessageFacade.getProperties());
    }

    /**
     * Check that setting GroupId on the message causes creation of the underlying properties
     * section with the expected value. New messages lack the properties section section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetGroupIdOnNewMessage() throws Exception {
        String groupId = "testValue";
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setGroupId(groupId);

        assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
        assertEquals("value was not set for GroupId as expected", groupId, amqpMessageFacade.getProperties().getGroupId());

        assertEquals("value was not set for GroupId as expected", groupId, amqpMessageFacade.getGroupId());
    }

    /**
     * Check that setting GroupId null on the message causes any existing value to be cleared
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetGroupIdNullOnMessageWithExistingGroupId() throws Exception {
        String groupId = "testValue";
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setGroupId(groupId);
        amqpMessageFacade.setGroupId(null);

        assertNull("value was not cleared for GroupId as expected", amqpMessageFacade.getProperties().getGroupId());
        assertNull("value was not cleared for GroupId as expected", amqpMessageFacade.getGroupId());
    }

    // --- reply-to-group-id field ---

    /**
     * Check that setting the ReplyToGroupId works on new messages without a properties
     * properties section. New messages lack the properties section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     */
    @Test
    public void testGetReplyToGroupIdIsNullForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("expected ReplyToGroupId to be null on new message", amqpMessageFacade.getReplyToGroupId());
    }

    /**
     * Check that getting the ReplyToGroupId works on received messages without a properties section
     */
    @Test
    public void testGetReplyToGroupIdWithReceivedMessageWithNoProperties() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        String replyToGroupId = amqpMessageFacade.getReplyToGroupId();
        assertNull("expected ReplyToGroupId to be null on message without properties section", replyToGroupId);
    }

    /**
     * Check that setting ReplyToGroupId null on a new message does not cause creation of the
     * underlying properties section. New messages lack the properties section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetReplyToGroupIdNullOnNewMessageDoesNotCreatePropertiesSection() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setReplyToGroupId(null);

        assertNull("properties section was created", amqpMessageFacade.getProperties());
    }

    /**
     * Check that getting the ReplyToGroupId works on received messages with a
     * properties section, but no reply-to-group-id
     */
    @Test
    public void testGetReplyToGroupIdWithReceivedMessageWithPropertiesButNoReplyToGroupId() {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setContentType(Symbol.valueOf("content-type"));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        String replyToGroupId = amqpMessageFacade.getReplyToGroupId();
        assertNull("expected ReplyToGroupId to be null on message with properties section but no reply-to-group-id", replyToGroupId);
    }

    /**
     * Check that getting the ReplyToGroupId returns the expected value from a
     * received messages with a reply-to-group-id.
     */
    @Test
    public void testGetReplyToGroupIdWithReceivedMessage() {
        String replyToGroupId = "myReplyGroup";

        Message message = Proton.message();

        Properties props = new Properties();
        props.setReplyToGroupId(replyToGroupId);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        String actual = amqpMessageFacade.getReplyToGroupId();
        assertNotNull("expected ReplyToGroupId on message was not found", actual);
        assertEquals("expected ReplyToGroupId on message was not found", replyToGroupId, actual);
    }

    /**
     * Test that setting the ReplyToGroupId sets the expected value into the
     * reply-to-group-id of the underlying proton message.
     */
    @Test
    public void testSetReplyToGroupId() {
        String replyToGroupId = "myReplyGroup";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setReplyToGroupId(replyToGroupId);

        assertNotNull("expected ReplyToGroupId on message was not found", amqpMessageFacade.getProperties().getReplyToGroupId());
        assertEquals("expected ReplyToGroupId on message was not found", replyToGroupId, amqpMessageFacade.getProperties().getReplyToGroupId());
    }

    @Test
    public void testSetReplyToGroupIdNullClearsProperty() {
        String replyToGroupId = "myReplyGroup";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setReplyToGroupId(replyToGroupId);

        assertNotNull("expected ReplyToGroupId on message was not found", amqpMessageFacade.getProperties().getReplyToGroupId());
        assertEquals("expected ReplyToGroupId on message was not found", replyToGroupId, amqpMessageFacade.getProperties().getReplyToGroupId());

        amqpMessageFacade.setReplyToGroupId(null);
        assertNull("expected ReplyToGroupId on message to be null", amqpMessageFacade.getProperties().getReplyToGroupId());
    }

    /**
     * Test that setting and getting the ReplyToGroupId yields the expected result
     */
    @Test
    public void testSetGetReplyToGroupId() {
        String replyToGroupId = "myReplyGroup";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getReplyToGroupId());

        amqpMessageFacade.setReplyToGroupId(replyToGroupId);

        assertNotNull("expected ReplyToGroupId on message was not found", amqpMessageFacade.getReplyToGroupId());
        assertEquals("expected ReplyToGroupId on message was not found", replyToGroupId, amqpMessageFacade.getReplyToGroupId());
    }

    // --- group-sequence field ---

    @Test
    public void testSetGetGroupSequenceOnNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        int groupSequence = 5;
        amqpMessageFacade.setGroupSequence(groupSequence);

        assertEquals("underlying message should have groupSequence field value", groupSequence, amqpMessageFacade.getProperties().getGroupSequence().longValue());
        assertEquals("GroupSequence not as expected", groupSequence, amqpMessageFacade.getGroupSequence());
    }

    /**
     * Tests handling of negative values set for group sequence. Negative values are used to map
     * a value into the upper half of the unsigned int range supported by AMQP group-sequence
     * field by utilising all of the bits of the signed int value. That is, Integer.MIN_VALUE maps
     * to the uint value 2^31 and -1 maps to the maximum uint value 2^32-1.
     */
    @Test
    public void testSetGroupSequenceNegativeMapsToUnsignedIntValueOnUnderlyingMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        int delta = 9;
        UnsignedInteger mapped = UnsignedInteger.valueOf(MAX_UINT - delta);
        amqpMessageFacade.setGroupSequence(-1 - delta);

        assertEquals("underlying message should have no groupSequence field value",mapped, amqpMessageFacade.getProperties().getGroupSequence());
        assertEquals("GroupSequence not as expected", -1 - delta, amqpMessageFacade.getGroupSequence());
    }

    @Test
    public void testGetGroupSequenceOnReceivedMessageWithGroupSequenceJustAboveSignedIntRange() {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setGroupSequence(UnsignedInteger.valueOf(1L + Integer.MAX_VALUE));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // The unsigned int value >= 2^31 will be represented as a negative, and so should begin from minimum signed int value
        assertEquals("GroupSequence not as expected", Integer.MIN_VALUE, amqpMessageFacade.getGroupSequence());
    }

    @Test
    public void testGetGroupSequenceOnReceivedMessageWithGroupSequenceMaxUnsignedIntValue() {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setGroupSequence(UnsignedInteger.valueOf(MAX_UINT));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        // The unsigned int value 2^32-1 will be represented as a negative, and should be the largest such value, -1
        assertEquals("GroupSequence not as expected", -1, amqpMessageFacade.getGroupSequence());
    }

    @Test
    public void testClearGroupSequenceOnMessageWithExistingGroupSequence() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setGroupSequence(5);
        amqpMessageFacade.setGroupSequence(0);

        // assertNull("underlying message should have no groupSequence field value", amqpMessageFacade.getAmqpMessage().getProperties().getGroupSequence());
        assertEquals("GroupSequence should be 0", 0, amqpMessageFacade.getGroupSequence());
    }

    @Test
    public void testClearGroupSequenceOnMessageWithoutExistingGroupSequence() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setGroupSequence(0);

        assertNull("underlying message should still have no properties setion", amqpMessageFacade.getProperties());
        assertEquals("GroupSequence should be 0", 0, amqpMessageFacade.getGroupSequence());
    }

    // --- to field ---

    // Basic test to see things are wired up at all. See {@link AmqpDestinationHelperTest}
    // for more comprehensive testing of the underlying bits.

    @Test
    public void testSetDestinationWithNullClearsProperty() {
        String testToAddress = "myTestAddress";
        JmsTopic dest = new JmsTopic(testToAddress);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setDestination(null);
        assertNull(amqpMessageFacade.getProperties());
        amqpMessageFacade.setDestination(dest);
        assertNotNull(amqpMessageFacade.getProperties().getTo());

        amqpMessageFacade.setDestination(null);
        assertNull(amqpMessageFacade.getProperties().getTo());
    }

    @Test
    public void testSetGetDestination() {
        String testToAddress = "myTestAddress";
        JmsTopic dest = new JmsTopic(testToAddress);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getProperties());

        amqpMessageFacade.setDestination(dest);

        assertNotNull(amqpMessageFacade.getProperties().getTo());
        assertEquals(testToAddress, amqpMessageFacade.getProperties().getTo());
        assertEquals(dest, amqpMessageFacade.getDestination());
    }

    @Test
    public void testGetDestinationWithReceivedMessage() throws JMSException {
        String testToAddress = "myTestAddress";

        Message message = Proton.message();

        Properties props = new Properties();
        props.setTo(testToAddress);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        JmsDestination dest = amqpMessageFacade.getDestination();
        //We didn't set any destination type annotations, so the consumer destination type will be used: a topic.
        assertTrue(dest instanceof Topic);
        assertEquals(testToAddress, ((Topic) dest).getTopicName());
    }

    @Test
    public void testGetDestinationWithReceivedMessageWithoutPropertiesUsesConsumerDestination() throws JMSException {
        AmqpConsumer consumer = createMockAmqpConsumer();
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(consumer, message);
        assertNotNull(amqpMessageFacade.getDestination());
        assertEquals(consumer.getDestination(), amqpMessageFacade.getDestination());
    }

    // --- reply-to field ---

    // Basic test to see things are wired up at all. See {@link AmqpDestinationHelperTest}
    // for more comprehensive testing of the underlying bits.

    @Test
    public void testSetGetReplyToWithNullClearsProperty() {
        String testReplyToAddress = "myTestReplyTo";
        JmsTopic dest = new JmsTopic(testReplyToAddress);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getProperties());
        amqpMessageFacade.setReplyTo(dest);
        assertNotNull(amqpMessageFacade.getProperties().getReplyTo());

        amqpMessageFacade.setReplyTo(null);
        assertNull(amqpMessageFacade.getProperties().getReplyTo());
    }

    @Test
    public void testSetGetReplyTo() {
        String testReplyToAddress = "myTestReplyTo";
        JmsTopic dest = new JmsTopic(testReplyToAddress);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getProperties());

        amqpMessageFacade.setReplyTo(dest);

        assertNotNull(amqpMessageFacade.getProperties().getReplyTo());
        assertEquals(testReplyToAddress, amqpMessageFacade.getProperties().getReplyTo());
        assertEquals(dest, amqpMessageFacade.getReplyTo());
    }

    @Test
    public void testGetReplyToWithReceivedMessage() throws JMSException {
        String testReplyToAddress = "myTestReplyTo";

        Message message = Proton.message();

        Properties props = new Properties();
        props.setReplyTo(testReplyToAddress);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        JmsDestination dest = amqpMessageFacade.getReplyTo();
        //We didn't set any destination type annotations, so the consumer destination type will be used: a topic.
        assertTrue(dest instanceof Topic);
        assertEquals(testReplyToAddress, ((Topic) dest).getTopicName());
    }

    @Test
    public void testGetReplyToWithReceivedMessageWithoutProperties() throws JMSException {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        assertNull(amqpMessageFacade.getReplyTo());
    }

    // --- message-id and correlation-id ---

    @Test
    public void testGetCorrelationIdIsNullOnNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected correlationId to be null on new message", amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that setting then getting an application-specific String as the CorrelationId returns
     * the expected value and sets the expected value on the underlying AMQP message.
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithStringAppSpecific() throws Exception {
        String testCorrelationId = "myAppSpecificStringCorrelationId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(testCorrelationId);

        assertEquals("correlationId value on underlying AMQP message not as expected", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertEquals("Expected correlationId not returned", testCorrelationId, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that setting then getting an JMSMessageID String as the CorrelationId returns
     * the expected value and sets the expected value on the underlying AMQP message
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithStringJMSMessageID() throws Exception {
        String testCorrelationId = "ID:myJMSMessageIDStringCorrelationId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(testCorrelationId);

        assertEquals("correlationId value on underlying AMQP message not as expected", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertEquals("Expected correlationId not returned from facade", testCorrelationId, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that setting the correlationId null, clears an existing value in the
     * underlying AMQP message correlation-id field
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetCorrelationIdNullClearsExistingValue() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setCorrelationId("cid");
        amqpMessageFacade.setCorrelationId(null);

        assertNull("Unexpected correlationId value on underlying AMQP message", amqpMessageFacade.getCorrelationId());
        assertNull("Expected correlationId bytes to be null", amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with
     * an application-specific (no 'ID:' prefix) String correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithStringAppSpecific() {
        String testCorrelationId = "myCorrelationIdString";
        correlationIdOnReceivedMessageTestImpl(testCorrelationId, testCorrelationId, true);
    }

    /**
     * Test that getting the correlationId when using an underlying received message with
     * a String correlation id representing a JMSMessageID (i.e there is an ID: prefix)
     * returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithStringJMSMessageId() {
        String testCorrelationId = "ID:JMSMessageIDasCorrelationIdString";
        correlationIdOnReceivedMessageTestImpl(testCorrelationId, testCorrelationId, false);
    }

    /**
     * Test that setting then getting a UUID as the correlationId returns the expected value,
     * and sets the expected value on the underlying AMQP message.
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithUUID() throws Exception {
        UUID testCorrelationId = UUID.randomUUID();
        String converted = "ID:AMQP_UUID:" + testCorrelationId;

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertEquals("Expected correlationId not returned", converted, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * UUID correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithUUID() {
        UUID testCorrelationId = UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + testCorrelationId.toString();
        correlationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
    }

    /**
     * Test that setting then getting a ulong correlationId (using BigInteger) returns the expected value
     * and sets the expected value on the underlying AMQP message
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithUnsignedLong() throws Exception {
        Object testCorrelationId = UnsignedLong.valueOf(123456789L);
        String converted = "ID:AMQP_ULONG:" + testCorrelationId;

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertEquals("Expected correlationId not returned", converted, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * ulong correlation id (using BigInteger) returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithUnsignedLong() {
        UnsignedLong testCorrelationId = UnsignedLong.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + testCorrelationId.toString();
        correlationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
    }

    /**
     * Test that setting then getting binary as the correlationId returns the expected value
     * and sets the correlation id field as expected on the underlying AMQP message
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithBinary() throws Exception {
        Binary testCorrelationId = createBinaryId();
        ByteBuffer buf = testCorrelationId.asByteBuffer();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);

        String converted = "ID:AMQP_BINARY:" + new AmqpMessageIdHelper().convertBinaryToHexString(bytes);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertEquals("Expected correlationId not returned", converted, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that setting then getting bytes as the correlationId returns the expected value
     * and sets the correlation id field as expected on the underlying AMQP message
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetGetCorrelationIdBytesOnNewMessage() throws Exception {
        Binary testCorrelationId = createBinaryId();
        byte[] bytes = testCorrelationId.getArray();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationIdBytes(bytes);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertArrayEquals("Expected correlationId bytes not returned", bytes, amqpMessageFacade.getCorrelationIdBytes());
    }

    /**
     * Test that setting the correlationId null, clears an existing value in the
     * underlying AMQP message correlation-id field
     * @throws Exception if unexpected error
     */
    @Test
    public void testSetCorrelationIdBytesNullClearsExistingValue() throws Exception {
        Binary testCorrelationId = createBinaryId();
        byte[] bytes = testCorrelationId.getArray();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationIdBytes(bytes);
        amqpMessageFacade.setCorrelationIdBytes(null);

        assertNull("Unexpected correlationId value on underlying AMQP message", amqpMessageFacade.getCorrelationId());
        assertNull("Expected correlationId bytes to be null", amqpMessageFacade.getCorrelationIdBytes());
    }

    @Test
    public void testSetCorrelationIdBytesNullDoesNotCreateProperties() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationIdBytes(null);
        assertNull("Unexpected Properties Object in AMQP message", amqpMessageFacade.getProperties());
    }

    @Test
    public void testGetCorrelationIdBytesOnNewMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected correlationId bytes to be null", amqpMessageFacade.getCorrelationIdBytes());
    }

    @Test
    public void testGetCorrelationIdBytesOnMessageWithNonBinaryContent() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Properties properties = new Properties();
        properties.setCorrelationId(UUID.randomUUID());

        amqpMessageFacade.setProperties(properties);

        try {
            amqpMessageFacade.getCorrelationIdBytes();
            fail("Should have thrown JMSException");
        } catch (JMSException ex) {}
    }

    @Test
    public void testGetCorrelationIdBytesOnReceievedMessageWithBinaryId() throws Exception {
        Binary testCorrelationId = createBinaryId();
        byte[] bytes = testCorrelationId.getArray();

        Data payloadData = Data.Factory.create();
        PropertiesDescribedType props = new PropertiesDescribedType();
        props.setCorrelationId(new Binary(bytes));
        payloadData.putDescribedType(props);
        Binary b = payloadData.encode();

        System.out.println("Using encoded AMQP message payload: " + b);

        Message message = Proton.message();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getProperties().getCorrelationId());
        assertArrayEquals("Expected correlationId bytes not returned", bytes, amqpMessageFacade.getCorrelationIdBytes());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * Binary message id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithBinary() {
        Binary testCorrelationId = createBinaryId();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +
                            AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(testCorrelationId.getArray());
        correlationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
    }

    private void correlationIdOnReceivedMessageTestImpl(final Object testCorrelationId, final String expected, boolean appSpecificCorrelationId) {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setCorrelationId(testCorrelationId);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        String result = amqpMessageFacade.getCorrelationId();
        assertNotNull("Expected a correlationId on received message", result);
        assertEquals("Incorrect correlationId value received", expected, result);
        if(!appSpecificCorrelationId) {
            assertTrue("Should have have 'ID:' prefix", result.startsWith(AmqpMessageIdHelper.JMS_ID_PREFIX));
        }
    }

    //--- Message Id field ---

    /**
     * Test that setting then getting a String value as the messageId returns the expected value
     *
     * @throws Exception if the test encounters an unexpected error
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithString() throws Exception {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, amqpMessageFacade.getMessageId());
        assertEquals("ID strings were not equal", testMessageId, amqpMessageFacade.getMessageId());
    }

    /**
     * Test that setting an ID: prefixed JMSMessageId results in the underlying AMQP
     * message holding the value with the ID: prefix retained.
     *
     * @throws Exception if the test encounters an unexpected error
     */
    @Test
    public void testSetMessageIdRetainsIdPrefixInUnderlyingMessage() throws Exception {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setMessageId(testMessageId);

        assertEquals("underlying messageId value not as expected", testMessageId, amqpMessageFacade.getMessageId());
    }

    /**
     * Test that setting the messageId null clears a previous value in the
     * underlying amqp message-id field
     *
     * @throws Exception if the test encounters an unexpected error
     */
    @Test
    public void testSetMessageIdNullClearsExistingValue() throws Exception  {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setMessageId(testMessageId);

        assertNotNull("messageId should not be null", amqpMessageFacade.getMessageId());

        amqpMessageFacade.setMessageId(null);

        assertNull("Expected messageId to be null", amqpMessageFacade.getMessageId());
        assertNull("ID was not null", amqpMessageFacade.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * String message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithString() {
        String testMessageId = AmqpMessageIdHelper.JMS_ID_PREFIX + "myMessageIdString";
        messageIdOnReceivedMessageTestImpl(testMessageId, testMessageId);
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * String message id without "ID:" prefix returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithStringNoIdPrefix() {
        //Deliberately omit the "ID:", as if it was sent from a non-JMS client
        Object testMessageId = "myMessageIdString";
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_NO_PREFIX + testMessageId;
        messageIdOnReceivedMessageTestImpl(testMessageId, expected);
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * UUID message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithUUID() {
        Object testMessageId = UUID.randomUUID();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + testMessageId;
        messageIdOnReceivedMessageTestImpl(testMessageId, expected);
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * ulong message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithUnsignedLong() {
        UnsignedLong testMessageId = UnsignedLong.valueOf(123456789L);
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + testMessageId;
        messageIdOnReceivedMessageTestImpl(testMessageId, expected);
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * Binary message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithBinary() {
        Binary testMessageId = createBinaryId();
        String expected = AmqpMessageIdHelper.JMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +
                            AmqpMessageIdHelper.INSTANCE.convertBinaryToHexString(testMessageId.getArray());
        messageIdOnReceivedMessageTestImpl(testMessageId, expected);
    }

    private void messageIdOnReceivedMessageTestImpl(Object underlyingMessageId, String expected) {
        if (!(underlyingMessageId == null || underlyingMessageId instanceof Binary
                || underlyingMessageId instanceof UnsignedLong || underlyingMessageId instanceof String || underlyingMessageId instanceof UUID)) {
            throw new IllegalArgumentException("invalid id type");
        }

        Message message = Proton.message();

        Properties props = new Properties();
        props.setMessageId(underlyingMessageId);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNotNull("Expected a messageId on received message", amqpMessageFacade.getMessageId());

        assertEquals("Incorrect messageId value received", expected, amqpMessageFacade.getMessageId());
    }

    private Binary createBinaryId() {
        byte length = 10;
        byte[] idBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            idBytes[i] = (byte) (length - i);
        }

        return new Binary(idBytes);
    }

    // --- Provider Message Id field ---

    @Test
    public void testGetProviderMessageIdObjectOnNewMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        assertNull("Expected messageId not returned", amqpMessageFacade.getProviderMessageIdObject());
    }

    /**
     * Test that setting then getting a String value as the provider messageId returns the expected value
     *
     * @throws Exception if the test encounters an unexpected error
     */
    @Test
    public void testSetGetProviderMessageIdObjectOnNewMessageWithString() throws Exception {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setProviderMessageIdObject(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, amqpMessageFacade.getProviderMessageIdObject());
        assertEquals("ID strings were not equal", testMessageId, amqpMessageFacade.getProviderMessageIdObject());
    }

    @Test
    public void testSetProviderMessageIdObjectNullClearsProperty() throws Exception {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setProviderMessageIdObject(testMessageId);
        assertEquals("Expected messageId not returned", testMessageId, amqpMessageFacade.getProviderMessageIdObject());

        amqpMessageFacade.setProviderMessageIdObject(null);
        assertNull("Expected messageId not returned", amqpMessageFacade.getProviderMessageIdObject());
    }

    @Test
    public void testSetProviderMessageIdObjectNullDoesNotCreateProperties() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected null value not returned", amqpMessageFacade.getProperties());

        amqpMessageFacade.setProviderMessageIdObject(null);

        assertNull("Expected null value not returned", amqpMessageFacade.getProviderMessageIdObject());
        assertNull("Expected null value not returned", amqpMessageFacade.getProperties());
    }

    // --- creation-time field  ---

    @Test
    public void testSetCreationTimeOnNewNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected null Properties section", amqpMessageFacade.getProperties());

        long expected = 1;
        amqpMessageFacade.setTimestamp(expected);

        assertEquals("Unexpected timestamp value", expected, amqpMessageFacade.getTimestamp());
        assertEquals("Expected creation-time field to be set on new Properties section", new Date(expected), amqpMessageFacade.getProperties().getCreationTime());
    }

    @Test
    public void testGetTimestampIsZeroForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertEquals("Expected no timestamp", 0, amqpMessageFacade.getTimestamp());
    }

    @Test
    public void testSetTimestampOnNewMessage() {
        Long timestamp = System.currentTimeMillis();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setTimestamp(timestamp);

        assertEquals("Expected creation-time field to be set", timestamp.longValue(), amqpMessageFacade.getProperties().getCreationTime().getTime());
        assertEquals("Expected timestamp", timestamp.longValue(), amqpMessageFacade.getTimestamp());
    }

    @Test
    public void testSetTimestampZeroOnNewMessageDoesNotCreatePropertiesSection() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setTimestamp(0);

        assertNull("underlying message should have no properties section", amqpMessageFacade.getProperties());
        assertEquals("Timestamp should not be set", 0, amqpMessageFacade.getTimestamp());
    }

    @Test
    public void testSetTimestampZeroOnMessageWithExistingTimestampClearsCreationTimeField() {
        Long timestamp = System.currentTimeMillis();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setTimestamp(timestamp);

        amqpMessageFacade.setTimestamp(0);

        assertNull("Expected creation-time to be null", amqpMessageFacade.getProperties().getCreationTime());
        assertEquals("Expected no timestamp", 0, amqpMessageFacade.getTimestamp());
    }

    // --- absolute-expiry-time field  ---

    @Test
    public void testGetExpirationIsZeroForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertEquals("Expected no expiration", 0, amqpMessageFacade.getExpiration());
    }

    @Test
    public void testSetGetExpirationOnNewMessage() {
        Long timestamp = System.currentTimeMillis();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setExpiration(timestamp);

        assertEquals("Expected absolute-expiry-time to be set", timestamp.longValue(), amqpMessageFacade.getProperties().getAbsoluteExpiryTime().getTime());
        assertEquals("Expected expiration to be set", timestamp.longValue(), amqpMessageFacade.getExpiration());
    }

    @Test
    public void testSetExpirationZeroOnNewMessageDoesNotCreatePropertiesSection() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected properties section not to exist", amqpMessageFacade.getProperties());

        amqpMessageFacade.setExpiration(0);

        assertNull("Expected properties section still not to exist", amqpMessageFacade.getProperties());
    }

    @Test
    public void testSetExpirationZeroOnMessageWithExistingExpiryTime() {
        Long timestamp = System.currentTimeMillis();

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setExpiration(timestamp);

        amqpMessageFacade.setExpiration(0);

        assertNull("Expected absolute-expiry-time to be null", amqpMessageFacade.getProperties().getAbsoluteExpiryTime());
        assertEquals("Expected no expiration", 0, amqpMessageFacade.getExpiration());
    }

    // --- user-id field  ---

    @Test
    public void testGetUserIdIsNullForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("expected userid to be null on new message", amqpMessageFacade.getUserId());
    }

    @Test
    public void testGetUserIdOnReceievedMessage() throws Exception {
        String userIdString = "testValue";
        byte[] bytes = userIdString.getBytes("UTF-8");

        Message message = Proton.message();

        Properties props = new Properties();
        props.setUserId(new Binary(bytes));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNotNull("Expected a userid on received message", amqpMessageFacade.getUserId());
        assertEquals("Incorrect messageId value received", userIdString, amqpMessageFacade.getUserId());
    }

    @Test
    public void testGetUserIdOnReceievedMessageWithEmptyBinaryValue() throws Exception {
        byte[] bytes = new byte[0];

        Message message = Proton.message();

        Properties props = new Properties();
        props.setUserId(new Binary(bytes));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull("Expected a userid on received message", amqpMessageFacade.getUserId());
    }

    /**
     * Check that setting UserId on the message causes creation of the underlying properties
     * section with the expected value. New messages lack the properties section section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetUserIdOnNewMessage() throws Exception {
        String userIdString = "testValue";
        byte[] bytes = userIdString.getBytes("UTF-8");
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserId(userIdString);

        assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
        assertTrue("bytes were not set as expected for userid", Arrays.equals(bytes, amqpMessageFacade.getProperties().getUserId().getArray()));
        assertEquals("userid not as expected", userIdString, amqpMessageFacade.getUserId());
    }

    /**
     * Check that setting UserId null on the message causes any existing value to be cleared
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetUserIdNullOnMessageWithExistingUserId() throws Exception {
        String userIdString = "testValue";
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserId(userIdString);
        amqpMessageFacade.setUserId(null);

        assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
        assertNull("bytes were not cleared as expected for userid", amqpMessageFacade.getProperties().getUserId());
        assertNull("userid not as expected", amqpMessageFacade.getUserId());
    }

    @Test
    public void testClearUserIdWithNoExistingProperties() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserId(null);

        assertNull("underlying message should still have no properties setion", amqpMessageFacade.getProperties());
        assertEquals("UserId should be null", null, amqpMessageFacade.getUserId());
    }

    // --- user-id-bytes field  ---

    @Test
    public void testGetUserIdBytesIsNullForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("expected userid bytes to be null on new message", amqpMessageFacade.getUserIdBytes());
    }

    @Test
    public void testGetUserIdBytesOnReceievedMessage() throws Exception {
        String userIdString = "testValue";
        byte[] bytes = userIdString.getBytes("UTF-8");

        Message message = Proton.message();

        message.setUserId(bytes);

        Properties props = new Properties();
        props.setUserId(new Binary(bytes));
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNotNull("Expected a userid on received message", amqpMessageFacade.getUserIdBytes());
        assertArrayEquals("Incorrect userid bytes value received", bytes, amqpMessageFacade.getUserIdBytes());
    }

    /**
     * Check that setting UserId on the message causes creation of the underlying properties
     * section with the expected value. New messages lack the properties section section,
     * as tested by {@link #testNewMessageHasNoUnderlyingPropertiesSection()}.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetUserIdBytesOnNewMessage() throws Exception {
        String userIdString = "testValue";
        byte[] bytes = userIdString.getBytes("UTF-8");
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserIdBytes(bytes);

        assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
        assertTrue("bytes were not set as expected for userid", Arrays.equals(bytes, amqpMessageFacade.getProperties().getUserId().getArray()));
        assertArrayEquals("userid bytes not as expected", bytes, amqpMessageFacade.getUserIdBytes());
    }

    /**
     * Check that setting UserId null on the message causes any existing value to be cleared
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetUserIdBytesNullOnMessageWithExistingUserId() throws Exception {
        String userIdString = "testValue";
        byte[] bytes = userIdString.getBytes("UTF-8");

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserIdBytes(bytes);
        amqpMessageFacade.setUserIdBytes(null);

        assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
        assertNull("bytes were not cleared as expected for userid", amqpMessageFacade.getProperties().getUserId());
        assertNull("userid bytes not as expected", amqpMessageFacade.getUserIdBytes());
    }

   @Test
   public void testSetUserIdBytesEmptyOnMessageWithExistingUserId() throws Exception {
       String userIdString = "testValue";
       byte[] bytes = userIdString.getBytes("UTF-8");

       AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

       amqpMessageFacade.setUserIdBytes(bytes);
       amqpMessageFacade.setUserIdBytes(new byte[0]);

       assertNotNull("properties section was not created", amqpMessageFacade.getProperties());
       assertNull("bytes were not cleared as expected for userid", amqpMessageFacade.getProperties().getUserId());
       assertNull("userid bytes not as expected", amqpMessageFacade.getUserIdBytes());
   }

    @Test
    public void testClearUserIdBytesWithNoExistingProperties() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setUserIdBytes(null);

        assertNull("underlying message should still have no properties setion", amqpMessageFacade.getProperties());
        assertEquals("UserId should be null", null, amqpMessageFacade.getUserIdBytes());
    }

    // ====== AMQP Message Annotations =======
    // =======================================

    @Test
    public void testNewMessageHasUnderlyingMessageAnnotationsSectionWithTypeAnnotation() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();;

        assertNotNull(amqpMessageFacade.getMessageAnnotations());
        Symbol annotationKey = AmqpMessageSupport.getSymbol(AmqpMessageSupport.JMS_MSG_TYPE);
        assertEquals(AmqpMessageSupport.JMS_MESSAGE, amqpMessageFacade.getMessageAnnotations().getValue().get(annotationKey));
    }

    @Test
    public void testNewMessageDoesNotHaveUnderlyingMessageAnnotationsSectionWithDeliveryTime() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();;

        assertNotNull(amqpMessageFacade.getMessageAnnotations());
        Symbol annotationKey = AmqpMessageSupport.getSymbol(AmqpMessageSupport.JMS_DELIVERY_TIME);
        assertNull(amqpMessageFacade.getMessageAnnotations().getValue().get(annotationKey));
    }

    @Test
    public void testMessageAnnotationExistsUsingReceivedMessageWithoutMessageAnnotationsSection() {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertFalse(amqpMessageFacade.messageAnnotationExists(symbolKeyName));
    }

    @Test
    public void testMessageAnnotationExistsUsingReceivedMessageWithMessageAnnotationsSection() {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Symbol, Object> annotationsMap = new HashMap<Symbol, Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpMessageFacade.messageAnnotationExists(symbolKeyName));
        assertFalse(amqpMessageFacade.messageAnnotationExists("otherName"));
    }

    @Test
    public void testGetMessageAnnotationUsingReceivedMessageWithoutMessageAnnotationsSection() {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull(amqpMessageFacade.getMessageAnnotation(symbolKeyName));
    }

    @Test
    public void testGetMessageAnnotationUsingReceivedMessage() {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Symbol, Object> annotationsMap = new HashMap<Symbol, Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(value, amqpMessageFacade.getMessageAnnotation(symbolKeyName));
        assertNull(amqpMessageFacade.getMessageAnnotation("otherName"));
    }

    @Test
    public void testSetMessageAnnotationsOnNewMessage() {
        String symbolKeyName = "myTestSymbolName";
        String symbolKeyName2 = "myTestSymbolName2";
        String value = "myTestValue";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        // check setting first annotation
        amqpMessageFacade.setMessageAnnotation(symbolKeyName, value);

        MessageAnnotations underlyingAnnotations = amqpMessageFacade.getMessageAnnotations();
        assertNotNull(underlyingAnnotations);

        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName)));
        assertEquals(value, underlyingAnnotations.getValue().get(Symbol.valueOf(symbolKeyName)));

        // set another
        amqpMessageFacade.setMessageAnnotation(symbolKeyName2, value);

        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName)));
        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName2)));
    }

    @Test
    public void testGetMessageAnnotationsOnMessageWithEmptyAnnotationsMap() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        MessageAnnotations annotations = new MessageAnnotations(new HashMap<Symbol, Object>());
        amqpMessageFacade.setMessageAnnotations(annotations);

        MessageAnnotations underlyingAnnotations = amqpMessageFacade.getMessageAnnotations();
        assertNull(underlyingAnnotations);
    }

    @Test
    public void testRemoveMessageAnnotation() {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Symbol, Object> annotationsMap = new HashMap<Symbol, Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(value, amqpMessageFacade.getMessageAnnotation(symbolKeyName));
        assertNull(amqpMessageFacade.getMessageAnnotation("otherName"));

        amqpMessageFacade.removeMessageAnnotation(symbolKeyName);
        assertNull(amqpMessageFacade.getMessageAnnotation(symbolKeyName));
    }

    @Test
    public void testRemoveMessageAnnotationOnMessageWithNoMessageAnnotationSectionDoesntFail() {
        Message message = Proton.message();

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        amqpMessageFacade.removeMessageAnnotation("keyName");
    }

    @Test
    public void testClearAllMessageAnnotationsUsingNewMessage() {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        amqpMessageFacade.clearMessageAnnotations();

        assertNull(amqpMessageFacade.getMessageAnnotations());
    }

    @Test
    public void testClearAllMessageAnnotationsUsingReceivedMessageWithMessageAnnotationsSection() {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Symbol, Object> annotationsMap = new HashMap<Symbol, Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        amqpMessageFacade.clearMessageAnnotations();

        assertNull(amqpMessageFacade.getMessageAnnotations());
    }

    // ====== Type =======

    @Test
    public void testGetJMSTypeIsNullOnNewMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        assertNull("did not expect a JMSType value to be present", amqpMessageFacade.getType());
    }

    @Test
    public void testSetJMSTypeSetsUnderlyingMessageSubject() throws Exception {
        String jmsType = "myJMSType";
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setType(jmsType);

        assertEquals("Subject should be set to the provded JMSType string", jmsType,
                        amqpMessageFacade.getProperties().getSubject());
    }

    @Test
    public void testSetTypeNullClearsExistingSubjectValue() throws Exception {
        String jmsType = "myJMSType";
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setType(jmsType);
        assertEquals("Subject should be set to the provded JMSType string", jmsType,
                        amqpMessageFacade.getProperties().getSubject());
        amqpMessageFacade.setType(null);
        assertNull("Subject should be clear", amqpMessageFacade.getProperties().getSubject());
    }

    @Test
    public void testSetTypeNullWhenNoPropertiesDoesNotCreateProperties() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Should not be any Properties object by default", amqpMessageFacade.getProperties());
        amqpMessageFacade.setType(null);
        assertNull("Subject should be clear", amqpMessageFacade.getProperties());
        assertNull("Should be no Type", amqpMessageFacade.getType());
    }

    /**
     * Test that {@link AmqpJmsMessageFacade#getType()} returns the expected value
     * for a message received with the message Subject set.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetJMSTypeWithReceivedMessage() throws Exception {
        String myJMSType = "myJMSType";

        Message message = Proton.message();
        message.setSubject(myJMSType);
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("JMSType value was not as expected", myJMSType, amqpMessageFacade.getType());
    }

    // ====== Content Type =======

    @Test
    public void testGetContentTypeIsNullOnNewMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        assertNull("did not expect a JMSType value to be present", amqpMessageFacade.getContentType());
    }

    @Test
    public void testGetContentTypeIsNullOnMessageWithEmptyPropertiesObject() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setProperties(new Properties());
        assertNull("did not expect a JMSType value to be present", amqpMessageFacade.getContentType());
    }

    // ====== AMQP Application Properties =======
    // ==========================================

    @Test
    public void testGetProperties() throws Exception {
        Map<Object, Object> applicationPropertiesMap = new HashMap<Object, Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);
        applicationPropertiesMap.put(TEST_PROP_B, TEST_VALUE_STRING_B);

        Message message2 = Proton.message();
        message2.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));

        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message2);

        Set<String> props = amqpMessageFacade.getPropertyNames();
        assertEquals(2, props.size());
        assertTrue(props.contains(TEST_PROP_A));
        assertEquals(TEST_VALUE_STRING_A, amqpMessageFacade.getProperty(TEST_PROP_A));
        assertTrue(props.contains(TEST_PROP_B));
        assertEquals(TEST_VALUE_STRING_B, amqpMessageFacade.getProperty(TEST_PROP_B));
    }

    @Test
    public void testGetPropertiesWithoutAnyApplicationPropertiesSection() throws Exception {
        Message message = Proton.message();
        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        Set<String> applicationProperties = amqpMessageFacade.getPropertyNames();
        assertNotNull(applicationProperties);
        assertTrue(applicationProperties.isEmpty());
    }

    @Test
    public void testGetPropertyNames() throws Exception {
        Map<Object, Object> applicationPropertiesMap = new HashMap<Object, Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);
        applicationPropertiesMap.put(TEST_PROP_B, TEST_VALUE_STRING_B);

        Message message2 = Proton.message();
        message2.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message2);

        Set<String> applicationPropertyNames = amqpMessageFacade.getPropertyNames();
        assertEquals(2, applicationPropertyNames.size());
        assertTrue(applicationPropertyNames.contains(TEST_PROP_A));
        assertTrue(applicationPropertyNames.contains(TEST_PROP_B));
    }

    @Test
    public void testGetPropertyNamesWithoutAnyApplicationPropertiesSection() throws Exception {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        Set<String> applicationPropertyNames = amqpMessageFacade.getPropertyNames();
        assertNotNull(applicationPropertyNames);
        assertTrue(applicationPropertyNames.isEmpty());
    }

    @Test
    public void testClearProperties() throws Exception {
        Map<Object, Object> applicationPropertiesMap = new HashMap<Object, Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));

        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        Set<String> props1 = amqpMessageFacade.getPropertyNames();
        assertEquals(1, props1.size());

        amqpMessageFacade.clearProperties();

        Set<String> props2 = amqpMessageFacade.getPropertyNames();
        assertTrue(props2.isEmpty());
    }

    @Test
    public void testPropertyExists() throws Exception {
        Map<Object, Object> applicationPropertiesMap = new HashMap<Object, Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));

        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertTrue(amqpMessageFacade.propertyExists(TEST_PROP_A));
        assertFalse(amqpMessageFacade.propertyExists(TEST_PROP_B));
    }

    @Test
    public void testPropertyExistsWithNoApplicationPropertiesSection() throws Exception {
        Message message = Proton.message();

        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertFalse(amqpMessageFacade.propertyExists(TEST_PROP_A));
    }

    @Test
    public void testGetProperty() throws Exception {
        Map<Object, Object> applicationPropertiesMap = new HashMap<Object, Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));

        JmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals(TEST_VALUE_STRING_A, amqpMessageFacade.getProperty(TEST_PROP_A));
        assertNull(amqpMessageFacade.getProperty(TEST_PROP_B));
    }

    @Test
    public void testSetProperty() throws Exception {
        Message message = Proton.message();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNull(amqpMessageFacade.getProperty(TEST_PROP_A));
        amqpMessageFacade.setProperty(TEST_PROP_A, TEST_VALUE_STRING_A);
        assertEquals(TEST_VALUE_STRING_A, amqpMessageFacade.getProperty(TEST_PROP_A));

        @SuppressWarnings("unchecked")
        Map<String, Object> underlyingApplicationProps = amqpMessageFacade.getApplicationProperties().getValue();
        assertTrue(underlyingApplicationProps.containsKey(TEST_PROP_A));
        assertEquals(TEST_VALUE_STRING_A, underlyingApplicationProps.get(TEST_PROP_A));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertyUsingNullKeyCausesIAE() throws Exception {
        JmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setProperty(null, "value");
    }

    @Test
    public void testGetPropertyUsingNullKeyReturnsNull() throws Exception {
        JmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getProperty(null));
    }

    @Test
    public void testPropertyExistsUsingNullKeyReturnsFalse() throws Exception {
        JmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertFalse(amqpMessageFacade.propertyExists(null));
    }

    @Test
    public void testNoApplicationPropertiesReturnedOnEmptyMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getApplicationProperties());
    }

    @Test
    public void testNoApplicationReturnedOnEmptyMapInMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Map<String, Object> value = new HashMap<>();
        amqpMessageFacade.setApplicationProperties(new ApplicationProperties(value));

        assertNull(amqpMessageFacade.getApplicationProperties());
    }

    // ====== AMQP Footer =======================
    // ==========================================

    @Test
    public void testNoFooterReturnedOnEmptyMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getFooter());
    }

    @Test
    public void testNoFooterReturnedOnEmptyFooterMapInMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Map<Symbol, Object> footerMap = new HashMap<>();
        amqpMessageFacade.setFooter(new Footer(footerMap));

        assertNull(amqpMessageFacade.getFooter());
    }

    @Test
    public void testDeliveryAnnotationsReturnedOnNonEmptyFooterMapInMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Map<Symbol, Object> footerMap = new HashMap<>();
        footerMap.put(Symbol.valueOf("test"), "value");
        amqpMessageFacade.setFooter(new Footer(footerMap));

        assertNotNull(amqpMessageFacade.getFooter());
    }

    // ====== AMQP Delivery Annotations =======================
    // ========================================================

    @Test
    public void testNoDeliveryAnnotationsReturnedOnEmptyMessage() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull(amqpMessageFacade.getDeliveryAnnotations());
    }

    @Test
    public void testNoDeliveryAnnotationsReturnedOnEmptyDeliveryAnnotationsMap() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Map<Symbol, Object> deliveryAnnotationsMap = new HashMap<>();
        amqpMessageFacade.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotationsMap));

        assertNull(amqpMessageFacade.getDeliveryAnnotations());
    }

    @Test
    public void testDeliveryAnnotationsReturnedOnNonEmptyDeliveryAnnotationsMap() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Map<Symbol, Object> deliveryAnnotationsMap = new HashMap<>();
        deliveryAnnotationsMap.put(Symbol.valueOf("test"), "value");
        amqpMessageFacade.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotationsMap));

        assertNotNull(amqpMessageFacade.getDeliveryAnnotations());
    }

    // ====== AMQP Message No Header tests ===========
    // ===============================================

    @Test
    public void testMessageWithDefaultHeaderValuesCreateNoHeaderForEncode() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setPersistent(false);
        amqpMessageFacade.setPriority(Message.DEFAULT_PRIORITY);
        amqpMessageFacade.setRedelivered(false);

        amqpMessageFacade.onSend(0);

        assertNull(amqpMessageFacade.getHeader());
    }

    @Test
    public void testMessageWithNonDefaultHeaderValuesCreatesHeaderForEncode() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setPersistent(true);
        amqpMessageFacade.setPriority(Message.DEFAULT_PRIORITY + 1);
        amqpMessageFacade.setRedelivered(true);

        amqpMessageFacade.onSend(100);

        assertNotNull(amqpMessageFacade.getHeader());
    }

    // ====== AMQP Message Facade copy() tests =======
    // ===============================================

    @Test
    public void testCopyOfEmptyMessageSucceeds() throws JMSException {
        AmqpJmsMessageFacade empty = createNewMessageFacade();
        AmqpJmsMessageFacade copy = empty.copy();
        assertNotNull(copy);
    }

    @Test
    public void testBasicMessageCopy() throws JMSException {
        AmqpJmsMessageFacade source = createNewMessageFacade();

        Map<Symbol, Object> deliveryAnnotationsMap = new HashMap<>();
        deliveryAnnotationsMap.put(Symbol.valueOf("test-annotation"), "value");
        source.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotationsMap));

        Map<Symbol, Object> footerMap = new HashMap<>();
        footerMap.put(Symbol.valueOf("test-footer"), "value");
        source.setFooter(new Footer(footerMap));

        JmsQueue aQueue = new JmsQueue("Test-Queue");
        JmsTemporaryQueue tempQueue = new JmsTemporaryQueue("Test-Temp-Queue");

        source.setDestination(aQueue);
        source.setReplyTo(tempQueue);

        source.setContentType("Test-ContentType");
        source.setCorrelationId("MY-APP-ID");
        source.setExpiration(42L);
        source.setGroupId("TEST-GROUP");
        source.setGroupSequence(23);
        source.setMessageId("ID:TEST-MESSAGEID");
        source.setPriority((byte) 1);
        source.setPersistent(false);
        source.setRedeliveryCount(12);
        source.setTimestamp(150L);
        source.setUserId("Cookie-Monster");
        source.setDeliveryTime(123456, false);

        source.setProperty("APP-Prop-1", "APP-Prop-1-Value");
        source.setProperty("APP-Prop-2", "APP-Prop-2-Value");

        AmqpJmsMessageFacade copy = source.copy();

        assertSame(source.getConnection(), copy.getConnection());
        assertEquals(source.hasBody(), copy.hasBody());

        assertEquals(source.getDestination(), copy.getDestination());
        assertEquals(source.getReplyTo(), copy.getReplyTo());

        assertEquals(source.getContentType(), copy.getContentType());
        assertEquals(source.getCorrelationId(), copy.getCorrelationId());
        assertEquals(source.getExpiration(), copy.getExpiration());
        assertEquals(source.getGroupId(), copy.getGroupId());
        assertEquals(source.getGroupSequence(), copy.getGroupSequence());
        assertEquals(source.getMessageId(), copy.getMessageId());
        assertEquals(source.getPriority(), copy.getPriority());
        assertEquals(source.isPersistent(), copy.isPersistent());
        assertEquals(source.getRedeliveryCount(), copy.getRedeliveryCount());
        assertEquals(source.getTimestamp(), copy.getTimestamp());
        assertEquals(source.getUserId(), copy.getUserId());
        assertEquals(source.getDeliveryTime(), copy.getDeliveryTime());


        // There should be two since none of the extended options were set
        assertEquals(2, copy.getPropertyNames().size());

        assertNotNull(copy.getProperty("APP-Prop-1"));
        assertNotNull(copy.getProperty("APP-Prop-2"));

        assertEquals("APP-Prop-1-Value", copy.getProperty("APP-Prop-1"));
        assertEquals("APP-Prop-2-Value", copy.getProperty("APP-Prop-2"));

        Footer copiedFooter = copy.getFooter();
        DeliveryAnnotations copiedDeliveryAnnotations = copy.getDeliveryAnnotations();

        assertNotNull(copiedFooter);
        assertNotNull(copiedDeliveryAnnotations);

        assertNotNull(copiedFooter.getValue());
        assertNotNull(copiedDeliveryAnnotations.getValue());

        assertFalse(copiedFooter.getValue().isEmpty());
        assertFalse(copiedDeliveryAnnotations.getValue().isEmpty());

        assertTrue(copiedFooter.getValue().containsKey(Symbol.valueOf("test-footer")));
        assertTrue(copiedDeliveryAnnotations.getValue().containsKey(Symbol.valueOf("test-annotation")));
    }

    @Test
    public void testCopyMessageWithAmqpTtlSet() throws JMSException {
        AmqpJmsMessageFacade source = createNewMessageFacade();

        long amqpTtl = 17L;
        source.setAmqpTimeToLiveOverride(amqpTtl);

        AmqpJmsMessageFacade copy = source.copy();

        // There should be one since AmqpTtl is used for an extended option
        assertEquals(1, copy.getPropertyNames().size());
        assertEquals(amqpTtl, copy.getProperty(AmqpMessageSupport.JMS_AMQP_TTL));
    }

    // ====== AMQP Message Facade misc tests =========
    // ===============================================

    @Test
    public void testMessageHasBodyDetectsPayload() throws Exception {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        assertFalse(amqpMessageFacade.hasBody());
        amqpMessageFacade.setBody(new AmqpValue("test"));
        assertTrue(amqpMessageFacade.hasBody());
    }

    @Test
    public void testClearBodyRemoveMessageBody() {
        Message message = Message.Factory.create();
        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        amqpMessageFacade = Mockito.spy(amqpMessageFacade);
        amqpMessageFacade.clearBody();
        Mockito.verify(amqpMessageFacade).setBody(null);
    }
}
