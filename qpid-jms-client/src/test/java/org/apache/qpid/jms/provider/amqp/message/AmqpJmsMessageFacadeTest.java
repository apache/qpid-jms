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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.jms.meta.JmsMessageId;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsMessageFacadeTest {

    private AmqpJmsMessageFacade createNewMessageFacade() {
        return new AmqpJmsMessageFacade(createMockAmqpConnection());
    }

    private AmqpJmsMessageFacade createReceivedMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsMessageFacade(amqpConsumer, message);
    }


    private AmqpConsumer createMockAmqpConsumer() {
        AmqpConsumer consumer = Mockito.mock(AmqpConsumer.class);
        Mockito.when(consumer.getConnection()).thenReturn(createMockAmqpConnection());
        return consumer;
    }

    private AmqpConnection createMockAmqpConnection() {
        return Mockito.mock(AmqpConnection.class);
    }

    // ====== AMQP Properties Section =======

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

        assertNull("Expected messageId value to be null on new message", amqpMessageFacade.getMessageId().getValue());
    }

    /**
     * Test that setting then getting a String value as the messageId returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithString() {
        String testMessageId = "ID:myStringMessageId";

        AmqpJmsMessageFacade amqpMessageFacade = createNewMessageFacade();

        JmsMessageId jmsMessageId = new JmsMessageId(testMessageId);
        amqpMessageFacade.setMessageId(jmsMessageId);

        assertEquals("Expected messageId object not returned", jmsMessageId, amqpMessageFacade.getMessageId());
        assertEquals("ID strings were not equal", testMessageId, amqpMessageFacade.getMessageId().getValue());
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

        assertEquals("Incorrect messageId value received", new JmsMessageId(expectedString), amqpMessageFacade.getMessageId());
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

}
