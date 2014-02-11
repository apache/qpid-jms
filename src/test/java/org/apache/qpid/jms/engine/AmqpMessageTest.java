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
package org.apache.qpid.jms.engine;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpMessageTest extends QpidJmsTestCase
{
    private static final String TEST_PROP_A = "TEST_PROP_A";
    private static final String TEST_PROP_B = "TEST_PROP_B";
    private static final String TEST_VALUE_STRING_A = "TEST_VALUE_STRING_A";
    private static final String TEST_VALUE_STRING_B = "TEST_VALUE_STRING_B";
    private static final byte[] TWO_TO_64_BYTES = new byte[] { (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0 };

    private AmqpConnection _mockAmqpConnection;
    private Delivery _mockDelivery;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockDelivery = Mockito.mock(Delivery.class);
    }

    // ====== Application Properties =======

    @Test
    public void testGetApplicationPropertyNames()
    {
        //Check a Proton Message without any application properties section
        Message message1 = Proton.message();
        AmqpMessage testAmqpMessage1 = TestAmqpMessage.createReceivedMessage(message1, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames = testAmqpMessage1.getApplicationPropertyNames();
        assertNotNull(applicationPropertyNames);
        assertTrue(applicationPropertyNames.isEmpty());

        //Check a Proton Message with some application properties
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);
        applicationPropertiesMap.put(TEST_PROP_B, TEST_VALUE_STRING_B);

        Message message2 = Proton.message();
        message2.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        AmqpMessage testAmqpMessage2 = TestAmqpMessage.createReceivedMessage(message2, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames2 = testAmqpMessage2.getApplicationPropertyNames();
        assertEquals(2, applicationPropertyNames2.size());
        assertTrue(applicationPropertyNames2.contains(TEST_PROP_A));
        assertTrue(applicationPropertyNames2.contains(TEST_PROP_B));
    }

    @Test
    public void testClearAllApplicationProperties()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames = testAmqpMessage.getApplicationPropertyNames();
        assertEquals(1, applicationPropertyNames.size());
        assertTrue(applicationPropertyNames.contains(TEST_PROP_A));

        //Now empty the application properties
        testAmqpMessage.clearAllApplicationProperties();
        applicationPropertyNames = testAmqpMessage.getApplicationPropertyNames();
        assertTrue(applicationPropertyNames.isEmpty());
    }

    @Test
    public void testApplicationPropertyExists()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertTrue(testAmqpMessage.applicationPropertyExists(TEST_PROP_A));
        assertFalse(testAmqpMessage.applicationPropertyExists(TEST_PROP_B));
    }

    @Test
    public void testGetApplicationProperty()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_B));
    }

    @Test
    public void testSetApplicationProperty()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        testAmqpMessage.setApplicationProperty(TEST_PROP_A, TEST_VALUE_STRING_A);
        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
    }

    @Test
    public void testSetApplicationPropertyUsingNullKeyCausesIAE()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            testAmqpMessage.setApplicationProperty(null, "value");
            fail("expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    // ====== Header =======

    /**
     * To satisfy the JMS requirement that messages are durable by default, the
     * {@link AmqpMessage} objects created for sending new messages are populated
     * with a header section with durable set to true.
     */
    @Test
    public void testNewMessageHasUnderlyingHeaderSectionWithDurableTrue()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNotNull(underlying.getHeader());
        assertTrue(underlying.getHeader().getDurable());
    }

    @Test
    public void testGetTtlIsNullForNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull(testAmqpMessage.getTtl());
    }

    @Test
    public void testGetTtlOnReceivedMessageWithTtl()
    {
        Long ttl = 123L;

        Message message = Proton.message();
        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        message.setHeader(header);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(ttl, testAmqpMessage.getTtl());
    }

    @Test
    public void testSetGetTtlOnNewMessage()
    {
        Long ttl = 123L;

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        testAmqpMessage.setTtl(ttl);

        assertEquals(ttl.longValue(), testAmqpMessage.getMessage().getHeader().getTtl().longValue());
        assertEquals(ttl, testAmqpMessage.getTtl());
    }

    @Test
    public void testSetTtlNullOnMessageWithExistingTtl()
    {
        Long ttl = 123L;

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setTtl(ttl);

        testAmqpMessage.setTtl(null);

        assertNull(testAmqpMessage.getMessage().getHeader().getTtl());
        assertNull(testAmqpMessage.getTtl());
    }

    /**
     * New messages lack the header section, as tested by {@link #testNewMessageHasNoUnderlyingHeaderSection}.
     *
     * In this case, the AMQP spec says the priority has default value of 4.
     */
    @Test
    public void testGetPriorityIs4ForNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertEquals("expected priority value not found", 4, testAmqpMessage.getPriority());
    }

    /**
     * When messages have a header section, but lack the priority field,
     * the AMQP spec says the priority has default value of 4.
     */
    @Test
    public void testGetPriorityIs4ForReceivedMessageWithHeaderButWithoutPriority()
    {
        Message message = Proton.message();

        Header header = new Header();
        message.setHeader(header);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected priority value not found", 4, testAmqpMessage.getPriority());
    }

    /**
     * When messages have a header section, which have a priority value, ensure it is returned.
     */
    @Test
    public void testGetPriorityForReceivedMessageWithHeaderWithPriority()
    {
        //value over 10
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected priority value not found", priority, testAmqpMessage.getPriority());
    }

    /**
     * Test that setting the Priority to a non-default value results in the underlying
     * message field being populated appropriately, and the value being returned from the Getter.
     */
    @Test
    public void testSetGetNonDefaultPriorityForNewMessage()
    {
        //value over 10
        byte priority = 11;

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setPriority(priority);

        Message underlying = testAmqpMessage.getMessage();
        assertEquals("expected priority value not found", priority, underlying.getPriority());

        assertEquals("expected priority value not found", priority, testAmqpMessage.getPriority());
    }

    /**
     * Receive message which has a header section with a priority value. Ensure the headers
     * underlying field value is cleared when the priority is set to the default priority of 4.
     */
    @Test
    public void testSetPriorityToDefaultOnReceivedMessageWithPriorityClearsPriorityField()
    {
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);
        testAmqpMessage.setPriority(AmqpMessage.DEFAULT_PRIORITY);

        //check the expected value is still returned
        assertEquals("expected priority value not returned", AmqpMessage.DEFAULT_PRIORITY, testAmqpMessage.getPriority());

        //check the underlying header field was actually cleared rather than set to AmqpMessage.DEFAULT_PRIORITY
        Message underlying = testAmqpMessage.getMessage();
        assertNull("underlying header priority field was not cleared", underlying.getHeader().getPriority());
    }

    // ====== Properties =======

    @Test
    public void testGetToWithReceivedMessageWithNoProperties()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String toAddress = testAmqpMessage.getTo();
        assertNull(toAddress);
    }

    @Test
    public void testGetToWithReceivedMessageWithPropertiesButNoTo()
    {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setContentType(Symbol.valueOf("content-type"));
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String toAddress = testAmqpMessage.getTo();
        assertNull(toAddress);
    }

    @Test
    public void testGetToWithReceivedMessage()
    {
        String testToAddress = "myTestAddress";

        Message message = Proton.message();

        Properties props = new Properties();
        props.setTo(testToAddress);
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String toAddress = testAmqpMessage.getTo();
        assertNotNull(toAddress);
        assertEquals(testToAddress, toAddress);
    }

    @Test
    public void testSetTo()
    {
        String testToAddress = "myTestAddress";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Message underlyingMessage = testAmqpMessage.getMessage();
        assertNull(underlyingMessage.getAddress());

        testAmqpMessage.setTo(testToAddress);

        assertNotNull(underlyingMessage.getAddress());
        assertEquals(testToAddress, underlyingMessage.getAddress());
    }

    @Test
    public void testSetGetTo()
    {
        String testToAddress = "myTestAddress";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull(testAmqpMessage.getTo());

        testAmqpMessage.setTo(testToAddress);

        assertNotNull(testAmqpMessage.getTo());
        assertEquals(testToAddress, testAmqpMessage.getTo());
    }

    @Test
    public void testGetReplyToWithReceivedMessageWithNoProperties()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String replyToAddress = testAmqpMessage.getReplyTo();
        assertNull(replyToAddress);
    }

    @Test
    public void testGetReplyToWithReceivedMessageWithPropertiesButNoReplyTo()
    {
        Message message = Proton.message();

        Properties props = new Properties();
        props.setContentType(Symbol.valueOf("content-type"));
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String replyToAddress = testAmqpMessage.getReplyTo();
        assertNull(replyToAddress);
    }

    @Test
    public void testGetReplyToWithReceivedMessage()
    {
        String testReplyToAddress = "myTestAddress";

        Message message = Proton.message();

        Properties props = new Properties();
        props.setReplyTo(testReplyToAddress);
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        String replyToAddress = testAmqpMessage.getReplyTo();
        assertNotNull(replyToAddress);
        assertEquals(testReplyToAddress, replyToAddress);
    }

    @Test
    public void testSetReplyTo()
    {
        String testReplyToAddress = "myTestAddress";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Message underlyingMessage = testAmqpMessage.getMessage();
        assertNull(underlyingMessage.getReplyTo());

        testAmqpMessage.setReplyTo(testReplyToAddress);

        assertNotNull(underlyingMessage.getReplyTo());
        assertEquals(testReplyToAddress, underlyingMessage.getReplyTo());
    }

    @Test
    public void testSetGetReplyTo()
    {
        String testReplyToAddress = "myTestAddress";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull(testAmqpMessage.getReplyTo());

        testAmqpMessage.setReplyTo(testReplyToAddress);

        assertNotNull(testAmqpMessage.getReplyTo());
        assertEquals(testReplyToAddress, testAmqpMessage.getReplyTo());
    }

    @Test
    public void testNewMessageHasNoUnderlyingPropertiesSection()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getProperties());
    }

    @Test
    public void testGetCreationTimeIsNullForNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull(testAmqpMessage.getCreationTime());
    }

    @Test
    public void testSetCreationTimeOnNewMessage()
    {
        Long timestamp = System.currentTimeMillis();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        testAmqpMessage.setCreationTime(timestamp);

        assertEquals(timestamp.longValue(), testAmqpMessage.getMessage().getProperties().getCreationTime().getTime());
        assertEquals(timestamp, testAmqpMessage.getCreationTime());
    }

    @Test
    public void testSetCreationTimeNullOnMessageWithExistingCreationTime()
    {
        Long timestamp = System.currentTimeMillis();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setCreationTime(timestamp);

        testAmqpMessage.setCreationTime(null);

        assertNull(testAmqpMessage.getMessage().getProperties().getCreationTime());
        assertNull(testAmqpMessage.getCreationTime());
    }

    @Test
    public void testGetAbsoluteExpiryTimeIsNullForNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull(testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testSetAbsoluteExpiryTimeOnNewMessage()
    {
        Long timestamp = System.currentTimeMillis();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        testAmqpMessage.setAbsoluteExpiryTime(timestamp);

        assertEquals(timestamp.longValue(), testAmqpMessage.getMessage().getProperties().getAbsoluteExpiryTime().getTime());
        assertEquals(timestamp, testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testSetAbsoluteExpiryTimeNullOnMessageWithExistingExpiryTime()
    {
        Long timestamp = System.currentTimeMillis();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setAbsoluteExpiryTime(timestamp);

        testAmqpMessage.setAbsoluteExpiryTime(null);

        assertNull(testAmqpMessage.getMessage().getProperties().getAbsoluteExpiryTime());
        assertNull(testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testGetCorrelationIdIsNullOnNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull("Expected correlationId to be null on new message", testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that setting then getting a String as the CorrelationId returns the expected value
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithString()
    {
        String testCorrelationId = "myStringCorrelationId";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setCorrelationId(testCorrelationId);

        assertEquals("Expected correlationId not returned", testCorrelationId, testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * String correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithString()
    {
        correlationIdOnReceivedMessageTestImpl("myCorrelationIdString");
    }

    /**
     * Test that setting then getting a UUID as the correlationId returns the expected value
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithUUID()
    {
        UUID testCorrelationId = UUID.randomUUID();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setCorrelationId(testCorrelationId);

        assertEquals("Expected correlationId not returned", testCorrelationId, testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * UUID correlation id returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithUUID()
    {
        correlationIdOnReceivedMessageTestImpl(UUID.randomUUID());
    }

    /**
     * Test that setting then getting a ulong correlationId (using BigInteger) returns the expected value
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithULong()
    {
        BigInteger testCorrelationId = BigInteger.valueOf(123456789);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setCorrelationId(testCorrelationId);

        assertEquals("Expected correlationId not returned", testCorrelationId, testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * ulong correlation id (using BigInteger) returns the expected value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithLong()
    {
        correlationIdOnReceivedMessageTestImpl(BigInteger.valueOf(123456789L));
    }

    /**
     * Test that attempting to set a ulong correlationId (using BigInteger) with a value
     * outwith the allowed [0 - 2^64) range results in an IAE being thrown
     */
    @Test
    public void testSetCorrelationIdOnNewMessageWithULongOurOfRangeThrowsIAE()
    {
        //negative value
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        try
        {
            testAmqpMessage.setCorrelationId(BigInteger.valueOf(-1));
            fail("expected exception was not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        //value 1 over max
        BigInteger aboveLimit = new BigInteger(TWO_TO_64_BYTES);
        try
        {
            testAmqpMessage.setCorrelationId(aboveLimit);
            fail("expected exception was not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        //confirm subtracting 1 to get the max value then allows success
        BigInteger onLimit = aboveLimit.subtract(BigInteger.ONE);
        testAmqpMessage.setCorrelationId(onLimit);
    }

    /**
     * Test that setting then getting binary as the correlationId returns the expected value
     */
    @Test
    public void testSetGetCorrelationIdOnNewMessageWithBinary()
    {
        ByteBuffer testCorrelationId = createByteBufferForBinaryId();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setCorrelationId(testCorrelationId);

        assertEquals("Expected correlationId not returned", testCorrelationId, testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that getting the correlationId when using an underlying received message with a
     * Binary message id returns the expected ByteBuffer value.
     */
    @Test
    public void testGetCorrelationIdOnReceivedMessageWithBinary()
    {
        ByteBuffer testCorrelationId = createByteBufferForBinaryId();

        correlationIdOnReceivedMessageTestImpl(testCorrelationId);
    }

    private void correlationIdOnReceivedMessageTestImpl(Object testCorrelationId)
    {
        Object underlyingIdObject = testCorrelationId;
        if(testCorrelationId instanceof ByteBuffer)
        {
            //The proton message uses a Binary wrapper for binary ids, not a ByteBuffer
            underlyingIdObject = Binary.create((ByteBuffer) testCorrelationId);
        }
        else if(testCorrelationId instanceof BigInteger)
        {
            //The proton message uses an UnsignedLong wrapper
            underlyingIdObject = UnsignedLong.valueOf((BigInteger) testCorrelationId);
        }

        Message message = Proton.message();

        Properties props = new Properties();
        props.setCorrelationId(underlyingIdObject);
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNotNull("Expected a correlationId on received message", testAmqpMessage.getCorrelationId());

        assertEquals("Incorrect correlationId value received", testCorrelationId, testAmqpMessage.getCorrelationId());
    }

    @Test
    public void testGetMessageIdIsNullOnNewMessage()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        assertNull("Expected messageId to be null on new message", testAmqpMessage.getMessageId());
    }

    /**
     * Test that setting then getting a String as the messageId returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithString()
    {
        String testMessageId = "myStringMessageId";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, testAmqpMessage.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * String message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithString()
    {
        messageIdOnReceivedMessageTestImpl("myMessageIdString");
    }

    /**
     * Test that setting then getting a UUID as the messageId returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithUUID()
    {
        UUID testMessageId = UUID.randomUUID();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, testAmqpMessage.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * UUID message id returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithUUID()
    {
        messageIdOnReceivedMessageTestImpl(UUID.randomUUID());
    }

    /**
     * Test that setting then getting a ulong messageId (using BigInteger) returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithULong()
    {
        BigInteger testMessageId = BigInteger.valueOf(123456789);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, testAmqpMessage.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * ulong message id (using BigInteger) returns the expected value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithLong()
    {
        messageIdOnReceivedMessageTestImpl(BigInteger.valueOf(123456789L));
    }

    /**
     * Test that attempting to set a ulong messageId (using BigInteger) with a value
     * outwith the allowed [0 - 2^64) range results in an IAE being thrown
     */
    @Test
    public void testSetMessageIdOnNewMessageWithULongOurOfRangeThrowsIAE()
    {
        //negative value
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        try
        {
            testAmqpMessage.setMessageId(BigInteger.valueOf(-1));
            fail("expected exception was not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        //value 1 over max
        BigInteger aboveLimit = new BigInteger(TWO_TO_64_BYTES);
        try
        {
            testAmqpMessage.setMessageId(aboveLimit);
            fail("expected exception was not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        //confirm subtracting 1 to get the max value then allows success
        BigInteger onLimit = aboveLimit.subtract(BigInteger.ONE);
        testAmqpMessage.setMessageId(onLimit);
    }

    /**
     * Test that setting then getting binary as the messageId returns the expected value
     */
    @Test
    public void testSetGetMessageIdOnNewMessageWithBinary()
    {
        ByteBuffer testMessageId = createByteBufferForBinaryId();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();
        testAmqpMessage.setMessageId(testMessageId);

        assertEquals("Expected messageId not returned", testMessageId, testAmqpMessage.getMessageId());
    }

    /**
     * Test that getting the messageId when using an underlying received message with a
     * Binary message id returns the expected ByteBuffer value.
     */
    @Test
    public void testGetMessageIdOnReceivedMessageWithBinary()
    {
        ByteBuffer testMessageId = createByteBufferForBinaryId();

        messageIdOnReceivedMessageTestImpl(testMessageId);
    }

    private void messageIdOnReceivedMessageTestImpl(Object testMessageId)
    {
        Object underlyingIdObject = testMessageId;
        if(testMessageId instanceof ByteBuffer)
        {
            //The proton message uses a Binary wrapper for binary ids, not a ByteBuffer
            underlyingIdObject = Binary.create((ByteBuffer) testMessageId);
        }
        else if(testMessageId instanceof BigInteger)
        {
            //The proton message uses an UnsignedLong wrapper
            underlyingIdObject = UnsignedLong.valueOf((BigInteger) testMessageId);
        }

        Message message = Proton.message();

        Properties props = new Properties();
        props.setMessageId(underlyingIdObject);
        message.setProperties(props);

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNotNull("Expected a messageId on received message", testAmqpMessage.getMessageId());

        assertEquals("Incorrect messageId value received", testMessageId, testAmqpMessage.getMessageId());
    }

    private ByteBuffer createByteBufferForBinaryId()
    {
        byte length = 10;
        byte[] idBytes = new byte[length];
        for(int i = 0; i < length; i++)
        {
            idBytes[i] = (byte) (length - i);
        }

        return ByteBuffer.wrap(idBytes);
    }

    // ====== Message Annotations =======

    @Test
    public void testMessageAnnotationExistsUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertFalse(testAmqpMessage.messageAnnotationExists(symbolKeyName));
    }

    @Test
    public void testMessageAnnotationExistsUsingReceivedMessageWithMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Object,Object> annotationsMap = new HashMap<Object,Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertTrue(testAmqpMessage.messageAnnotationExists(symbolKeyName));
        assertFalse(testAmqpMessage.messageAnnotationExists("otherName"));
    }

    @Test
    public void testGetMessageAnnotationUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull(testAmqpMessage.getMessageAnnotation(symbolKeyName));
    }

    @Test
    public void testGetMessageAnnotationUsingReceivedMessageWithMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Object,Object> annotationsMap = new HashMap<Object,Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(value, testAmqpMessage.getMessageAnnotation(symbolKeyName));
        assertNull(testAmqpMessage.getMessageAnnotation("otherName"));
    }

    @Test
    public void testNewMessageHasNoUnderlyingMessageAnnotationsSection()
    {
        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getMessageAnnotations());
    }

    @Test
    public void testSetMessageAnnotationOnNewMessage()
    {
        String symbolKeyName = "myTestSymbolName";
        String symbolKeyName2 = "myTestSymbolName2";
        String value = "myTestValue";

        AmqpMessage testAmqpMessage = TestAmqpMessage.createNewMessage();

        //check setting first annotation creates the annotations section
        testAmqpMessage.setMessageAnnotation(symbolKeyName, value);

        MessageAnnotations underlyingAnnotations = testAmqpMessage.getMessage().getMessageAnnotations();
        assertNotNull(underlyingAnnotations);

        assertEquals(1, underlyingAnnotations.getValue().size());
        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName)));
        assertEquals(value, underlyingAnnotations.getValue().get(Symbol.valueOf(symbolKeyName)));

        //set another
        testAmqpMessage.setMessageAnnotation(symbolKeyName2, value);

        assertEquals(2, underlyingAnnotations.getValue().size());
        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName)));
        assertTrue(underlyingAnnotations.getValue().containsKey(Symbol.valueOf(symbolKeyName2)));
    }

    @Test
    public void testClearMessageAnnotation()
    {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Object,Object> annotationsMap = new HashMap<Object,Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(value, testAmqpMessage.getMessageAnnotation(symbolKeyName));
        assertNull(testAmqpMessage.getMessageAnnotation("otherName"));

        testAmqpMessage.clearMessageAnnotation(symbolKeyName);
        assertNull(testAmqpMessage.getMessageAnnotation(symbolKeyName));
    }

    @Test
    public void testClearMessageAnnotationONMessageWithNoMessageAnnotationSectionDoesntThrowException()
    {
        Message message = Proton.message();

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        testAmqpMessage.clearMessageAnnotation("keyName");
    }

    @Test
    public void testClearAllMessageAnnotationsUsingNewMessage()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        testAmqpMessage.clearAllMessageAnnotations();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getMessageAnnotations());
    }

    @Test
    public void testClearAllMessageAnnotationsUsingReceivedMessageWithMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Object,Object> annotationsMap = new HashMap<Object,Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        testAmqpMessage.clearAllMessageAnnotations();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getMessageAnnotations());
    }

    @Test
    public void testGetMessageAnnotationsCountUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        Message message = Proton.message();
        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(0, testAmqpMessage.getMessageAnnotationsCount());
    }

    @Test
    public void testGetMessageAnnotationsCountUsingReceivedMessageWithMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";
        String symbolKeyName2 = "myTestSymbolName2";
        String value = "myTestValue";

        Message message = Proton.message();

        Map<Object,Object> annotationsMap = new HashMap<Object,Object>();
        annotationsMap.put(Symbol.valueOf(symbolKeyName), value);
        annotationsMap.put(Symbol.valueOf(symbolKeyName2), value);
        message.setMessageAnnotations(new MessageAnnotations(annotationsMap));

        AmqpMessage testAmqpMessage = TestAmqpMessage.createReceivedMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(2, testAmqpMessage.getMessageAnnotationsCount());
        testAmqpMessage.clearMessageAnnotation(symbolKeyName);
        assertEquals(1, testAmqpMessage.getMessageAnnotationsCount());
    }
}
