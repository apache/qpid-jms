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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.impl.DataImpl;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsMessageFacadeTest {

    private final JmsDestination consumerDestination = new JmsTopic("TestTopic");

    private AmqpJmsMessageFacade createNewMessageFacade() {
        return new AmqpJmsMessageFacade(createMockAmqpConnection());
    }

    private AmqpJmsMessageFacade createReceivedMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsMessageFacade(amqpConsumer, message);
    }

    private AmqpConsumer createMockAmqpConsumer() {
        AmqpConsumer consumer = Mockito.mock(AmqpConsumer.class);
        Mockito.when(consumer.getConnection()).thenReturn(createMockAmqpConnection());
        Mockito.when(consumer.getDestination()).thenReturn(consumerDestination);
        return consumer;
    }

    private AmqpConnection createMockAmqpConnection() {
        return Mockito.mock(AmqpConnection.class);
    }

    // ====== AMQP Header Section =======
    // ==================================

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

    /**
     * To satisfy the JMS requirement that messages are durable by default, the
     * {@link AmqpJmsMessageFacade} objects created for sending new messages are
     * populated with a header section with durable set to true.
     */
    @Test
    public void testNewMessageHasUnderlyingHeaderSectionWithDurableTrue() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertNotNull("Expected message to have Header section", underlying.getHeader());
        assertTrue("Durable not as expected", underlying.getHeader().getDurable());
    }

    @Test
    public void testNewMessageHasUnderlyingHeaderSectionWithNoTtlSet() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertNotNull("Expected message to have Header section", underlying.getHeader());
        assertNull("Ttl field should not be set", underlying.getHeader().getTtl());
    }

    @Test
    public void testGetTtlSynthesizedExpirationOnReceivedMessageWithTtlButNoAbsoluteExpiration() {
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
    }

    @Test
    public void testSetGetTtlOverrideOnNewMessage() throws Exception {
        long ttl = 123L;

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertFalse("Should not have a ttl override", amqpMessageFacade.hasAmqpTimeToLiveOverride());

        amqpMessageFacade.setAmqpTimeToLiveOverride(ttl);

        assertTrue("Should have a ttl override", amqpMessageFacade.hasAmqpTimeToLiveOverride());
        assertEquals(ttl, amqpMessageFacade.getAmqpTimeToLiveOverride());
        // check value on underlying TTL field is NOT set
        assertNull("TTL field on underlying message should NOT be set", amqpMessageFacade.getAmqpMessage().getHeader().getTtl());
    }

    @Test
    public void testOnSendClearsTtlOnMessageReceivedWithTtl() throws Exception {
        Message message = Proton.message();
        int origTtl = 5;
        message.setTtl(origTtl);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("TTL has been unset already", origTtl, message.getTtl());

        amqpMessageFacade.onSend(false, false, 0);

        // check value on underlying TTL field is NOT set
        assertEquals("TTL has not been cleared", 0, message.getTtl());
        assertNull("TTL field on underlying message should NOT be set", amqpMessageFacade.getAmqpMessage().getHeader().getTtl());
    }

    @Test
    public void testOnSendOverridesTtlOnMessageReceivedWithTtl() throws Exception {
        Message message = Proton.message();
        int origTtl = 5;
        int newTtl = 10;
        message.setTtl(origTtl);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("TTL has been unset already", origTtl, message.getTtl());

        amqpMessageFacade.onSend(false, false, newTtl);

        // check value on underlying TTL field is NOT set
        assertEquals("TTL has not been overriden", newTtl, message.getTtl());
        assertEquals("TTL field on underlying message should be set", UnsignedInteger.valueOf(newTtl), amqpMessageFacade.getAmqpMessage().getHeader().getTtl());
    }

    @Test
    public void testOnSendOverridesProviderTtlWithSpecifiedOverrideTtl() throws Exception {
        Message message = Proton.message();
        int overrideTtl = 5;
        int producerTtl = 10;

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);
        amqpMessageFacade.setAmqpTimeToLiveOverride(overrideTtl);

        amqpMessageFacade.onSend(false, false, producerTtl);

        // check value on underlying TTL field is set to the override
        assertEquals("TTL has not been overriden", overrideTtl, message.getTtl());
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

        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertEquals("expected priority value not found", priority, underlying.getPriority());
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

        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertEquals("expected priority value not found", 0, underlying.getPriority());
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

        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertEquals("expected priority value not found", 9, underlying.getPriority());
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
        Message underlying = amqpMessageFacade.getAmqpMessage();
        assertNull("underlying header priority field was not cleared", underlying.getHeader().getPriority());
    }

    // ====== AMQP Properties Section =======
    // ======================================

    @Test
    public void testGetExpirationIsZeroForNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertEquals("Expected no expiration", 0, amqpMessageFacade.getExpiration());
    }

    // --- message-id and correlation-id ---

    @Test
    public void testGetCorrelationIdIsNullOnNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected correlationId to be null on new message", amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that setting then getting an application-specific String as the CorrelationId returns
     * the expected value and sets the expected value on the underlying AMQP message, additionally
     * setting the annotation to indicate an application-specific correlation-id
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithStringAppSpecific() {
        String testCorrelationId = "myAppSpecificStringCorrelationId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(testCorrelationId);

        Message amqpMessage = amqpMessageFacade.getAmqpMessage();
        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessage.getCorrelationId());
        assertEquals("Expected correlationId not returned", testCorrelationId, amqpMessageFacade.getCorrelationId());

        MessageAnnotations messageAnnotations = amqpMessage.getMessageAnnotations();
        assertNotNull("Message Annotations not present", messageAnnotations);
        Object annotation = messageAnnotations.getValue().get(Symbol.valueOf(AmqpMessageSupport.JMS_APP_CORRELATION_ID));
        assertTrue("Message annotation " + AmqpMessageSupport.JMS_APP_CORRELATION_ID + " not set as expected", Boolean.TRUE.equals(annotation));
    }

    /**
     * Test that setting then getting an JMSMessageID String as the CorrelationId returns
     * the expected value and sets the expected value on the underlying AMQP message, additionally
     * checking it does not set the annotation to indicate an application-specific correlation-id
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithStringJMSMessageID() {
        String testCorrelationId = "ID:myJMSMessageIDStringCorrelationId";
        //The underlying AMQP message should not contain the ID: prefix
        String stripped = AmqpMessageIdHelper.INSTANCE.stripMessageIdPrefix(testCorrelationId);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(testCorrelationId);

        Message amqpMessage = amqpMessageFacade.getAmqpMessage();
        assertEquals("Unexpected correlationId value on underlying AMQP message", stripped, amqpMessage.getCorrelationId());
        assertEquals("Expected correlationId not returned from facade", testCorrelationId, amqpMessageFacade.getCorrelationId());

        assertNull("Message annotation " + AmqpMessageSupport.JMS_APP_CORRELATION_ID + " not null as expected", amqpMessageFacade.getAnnotation(AmqpMessageSupport.JMS_APP_CORRELATION_ID));
    }

    /**
     * Test that getting the correlationId when using an underlying received message with
     * an application-specific (no 'ID:' prefix) String correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithStringAppSpecific() {
        correlationIdOnReceivedMessageTestImpl("myCorrelationIdString", true);
    }

    /**
     * Test that getting the correlationId when using an underlying received message with
     * a String correlation id representing a JMSMessageID (i.e there is no annotation to
     * indicate it is an application-specific correlation-id) returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithStringJMSMessageId() {
        correlationIdOnReceivedMessageTestImpl("myCorrelationIdString", false);
    }

    /**
     * Test that setting then getting a UUID as the correlationId returns the expected value,
     * and sets the expected value on the underlying AMQP message.
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithUUID() {
        UUID testCorrelationId = UUID.randomUUID();
        String converted = appendIdAndTypePrefix(testCorrelationId);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getAmqpMessage().getCorrelationId());
        assertEquals("Expected correlationId not returned", converted, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * UUID correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithUUID() {
        correlationIdOnReceivedMessageTestImpl(UUID.randomUUID(), true);
    }

    /**
     * Test that setting then getting a ulong correlationId (using BigInteger) returns the expected value
     * and sets the expected value on the underlying AMQP message
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithUnsignedLong() {
        Object testCorrelationId = UnsignedLong.valueOf(123456789L);
        String converted = appendIdAndTypePrefix(testCorrelationId);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getAmqpMessage().getCorrelationId());
        assertEquals("Expected correlationId not returned", converted, amqpMessageFacade.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * ulong correlation id (using BigInteger) returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithUnsignedLong() {
        correlationIdOnReceivedMessageTestImpl(UnsignedLong.valueOf(123456789L), true);
    }

    /**
     * Test that setting then getting binary as the correlationId returns the expected value
     * and sets the correlation id field as expected on the underlying AMQP message
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithBinary() {
        Binary testCorrelationId = createBinaryId();
        String converted = appendIdAndTypePrefix(testCorrelationId);

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();
        amqpMessageFacade.setCorrelationId(converted);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getAmqpMessage().getCorrelationId());
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

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getAmqpMessage().getCorrelationId());
        assertArrayEquals("Expected correlationId bytes not returned", bytes, amqpMessageFacade.getCorrelationIdBytes());
    }

    @Test
    public void testGetCorrelationIdBytesOnReceievedMessageWithBinaryId() throws Exception {
        Binary testCorrelationId = createBinaryId();
        byte[] bytes = testCorrelationId.getArray();

        org.apache.qpid.proton.codec.Data payloadData = new DataImpl();
        PropertiesDescribedType props = new PropertiesDescribedType();
        props.setCorrelationId(new Binary(bytes));
        payloadData.putDescribedType(props);
        Binary b = payloadData.encode();

        System.out.println("Using encoded AMQP message payload: " + b);

        Message message = Proton.message();
        int decoded = message.decode(b.getArray(), b.getArrayOffset(), b.getLength());
        assertEquals(decoded, b.getLength());

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertEquals("Unexpected correlationId value on underlying AMQP message", testCorrelationId, amqpMessageFacade.getAmqpMessage().getCorrelationId());
        assertArrayEquals("Expected correlationId bytes not returned", bytes, amqpMessageFacade.getCorrelationIdBytes());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * Binary message id returns the expected ByteBuffer value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithBinary() {
        Binary testCorrelationId = createBinaryId();

        correlationIdOnReceivedMessageTestImpl(testCorrelationId, true);
    }

    private void correlationIdOnReceivedMessageTestImpl(final Object testCorrelationId, boolean appSpecificCorrelationId) {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setCorrelationId(testCorrelationId);
        message.setProperties(props);

        if(appSpecificCorrelationId)
        {
            //Add the annotation instructing the client the correlation-id is not a JMS MessageID value.
            Map<Symbol, Object> annMap = new HashMap<Symbol, Object>();
            annMap.put(Symbol.valueOf(AmqpMessageSupport.JMS_APP_CORRELATION_ID), true);
            MessageAnnotations messageAnnotations = new MessageAnnotations(annMap);
            message.setMessageAnnotations(messageAnnotations);
        }

        AmqpMessageIdHelper helper = AmqpMessageIdHelper.INSTANCE;
        String expected = helper.toBaseMessageIdString(testCorrelationId);
        if(!appSpecificCorrelationId && !helper.hasMessageIdPrefix(expected))
        {
            expected = AmqpMessageIdHelper.JMS_ID_PREFIX + expected;
        }

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNotNull("Expected a correlationId on received message", amqpMessageFacade.getCorrelationId());

        assertEquals("Incorrect correlationId value received", expected, amqpMessageFacade.getCorrelationId());
    }

    @Test
    public void testGetMessageIdIsNullOnNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected messageId value to be null on new message", amqpMessageFacade.getMessageId());
    }

    /**
     * Test that setting then getting a String value as the messageId returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithString() {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        amqpMessageFacade.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, amqpMessageFacade.getMessageId());
        assertEquals("ID strings were not equal", testMessageId, amqpMessageFacade.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * String message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithString() {
        messageIdOnReceivedMessageTestImpl("myMessageIdString");
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * UUID message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithUUID() {
        messageIdOnReceivedMessageTestImpl(UUID.randomUUID());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * ulong message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithUnsignedLong() {
        messageIdOnReceivedMessageTestImpl(UnsignedLong.valueOf(123456789L));
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * Binary message id returns the expected ByteBuffer value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithBinary() {
        Binary testMessageId = createBinaryId();

        messageIdOnReceivedMessageTestImpl(testMessageId);
    }

    private void messageIdOnReceivedMessageTestImpl(Object testMessageId) {
        Object underlyingIdObject = testMessageId;
        if (!(testMessageId == null || testMessageId instanceof Binary || testMessageId instanceof UnsignedLong || testMessageId instanceof String || testMessageId instanceof UUID)) {
            throw new IllegalArgumentException("invalid id type");
        }

        Message message = Proton.message();

        Properties props = new Properties();
        props.setMessageId(underlyingIdObject);
        message.setProperties(props);

        AmqpJmsMessageFacade amqpMessageFacade = createReceivedMessageFacade(createMockAmqpConsumer(), message);

        assertNotNull("Expected a messageId on received message", amqpMessageFacade.getMessageId());

        String expectedString = appendIdAndTypePrefix(testMessageId);

        assertEquals("Incorrect messageId value received", expectedString, amqpMessageFacade.getMessageId());
    }

    private String appendIdAndTypePrefix(Object testMessageId) {
        if (testMessageId instanceof Binary) {
            ByteBuffer buf = ((Binary) testMessageId).asByteBuffer();

            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            return "ID:AMQP_BINARY:" + new AmqpMessageIdHelper().convertBinaryToHexString(bytes);
        } else if (testMessageId instanceof UnsignedLong) {
            return ("ID:AMQP_ULONG:" + testMessageId);
        } else if (testMessageId instanceof UUID) {
            return ("ID:AMQP_UUID:" + testMessageId);
        } else if (testMessageId instanceof String) {
            return "ID:" + testMessageId;
        } else if (testMessageId == null) {
            return null;
        }

        throw new IllegalArgumentException();
    }

    private Binary createBinaryId() {
        byte length = 10;
        byte[] idBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            idBytes[i] = (byte) (length - i);
        }

        return new Binary(idBytes);
    }

    // --- creation-time field  ---

    @Test
    public void testSetCreationTimeOnNewNewMessage() {
        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        assertNull("Expected null Properties section", amqpMessageFacade.getAmqpMessage().getProperties());

        long expected = 1;
        amqpMessageFacade.setTimestamp(expected);

        assertEquals("Unexpected timestamp value", expected, amqpMessageFacade.getTimestamp());
        assertEquals("Expected creation-time field to be set on new Properties section", new Date(expected), amqpMessageFacade.getAmqpMessage().getProperties().getCreationTime());
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

        source.setProperty("APP-Prop-1", "APP-Prop-1-Value");
        source.setProperty("APP-Prop-2", "APP-Prop-2-Value");

        AmqpJmsMessageFacade copy = source.copy();

        assertSame(source.getConnection(), copy.getConnection());

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

        // There should be two since none of the extended options were set
        assertEquals(2, copy.getPropertyNames().size());

        assertNotNull(copy.getProperty("APP-Prop-1"));
        assertNotNull(copy.getProperty("APP-Prop-2"));

        assertEquals("APP-Prop-1-Value", copy.getProperty("APP-Prop-1"));
        assertEquals("APP-Prop-2-Value", copy.getProperty("APP-Prop-2"));
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
}
