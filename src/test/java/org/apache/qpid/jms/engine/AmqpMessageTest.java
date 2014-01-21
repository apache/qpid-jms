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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
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
        TestAmqpMessage testAmqpMessage1 = new TestAmqpMessage(message1, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames = testAmqpMessage1.getApplicationPropertyNames();
        assertNotNull(applicationPropertyNames);
        assertTrue(applicationPropertyNames.isEmpty());

        //Check a Proton Message with some application properties
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);
        applicationPropertiesMap.put(TEST_PROP_B, TEST_VALUE_STRING_B);

        Message message2 = Proton.message();
        message2.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        TestAmqpMessage testAmqpMessage2 = new TestAmqpMessage(message2, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_B));
    }

    @Test
    public void testSetApplicationProperty()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        testAmqpMessage.setApplicationProperty(TEST_PROP_A, TEST_VALUE_STRING_A);
        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
    }

    @Test
    public void testSetApplicationPropertyUsingNullKeyCausesIAE()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

    @Test
    public void testNewMessageHasNoUnderlyingHeaderSection()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getHeader());
    }

    @Test
    public void testGetTtlIsNullForNewMessage()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertNull(testAmqpMessage.getTtl());
    }

    @Test
    public void testGetTtlOnRecievedMessageWithTtl()
    {
        Long ttl = 123L;

        Message message = Proton.message();
        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttl));
        message.setHeader(header);

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(ttl, testAmqpMessage.getTtl());
    }

    @Test
    public void testSetGetTtlOnNewMessage()
    {
        Long ttl = 123L;

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        testAmqpMessage.setTtl(ttl);

        assertEquals(ttl.longValue(), testAmqpMessage.getMessage().getHeader().getTtl().longValue());
        assertEquals(ttl, testAmqpMessage.getTtl());
    }

    @Test
    public void testSetTtlNullOnMessageWithExistingTtl()
    {
        Long ttl = 123L;

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();
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
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertEquals("expected priority value not found", 4, testAmqpMessage.getPriority());
    }

    /**
     * When messages have a header section, but lack the priority field,
     * the AMQP spec says the priority has default value of 4.
     */
    @Test
    public void testGetPriorityIs4ForRecievedMessageWithHeaderButWithoutPriority()
    {
        Message message = Proton.message();

        Header header = new Header();
        message.setHeader(header);

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("expected priority value not found", 4, testAmqpMessage.getPriority());
    }

    /**
     * When messages have a header section, which have a priority value, ensure it is returned.
     */
    @Test
    public void testGetPriorityForRecievedMessageWithHeaderWithPriority()
    {
        //value over 10
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();
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
    public void testSetPriorityToDefaultOnRecievedMessageWithPriorityClearsPriorityField()
    {
        byte priority = 11;

        Message message = Proton.message();
        Header header = new Header();
        message.setHeader(header);
        header.setPriority(UnsignedByte.valueOf(priority));

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);
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
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        String toAddress = testAmqpMessage.getTo();
        assertNotNull(toAddress);
        assertEquals(testToAddress, toAddress);
    }

    @Test
    public void testSetTo()
    {
        String testToAddress = "myTestAddress";

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertNull(testAmqpMessage.getTo());

        testAmqpMessage.setTo(testToAddress);

        assertNotNull(testAmqpMessage.getTo());
        assertEquals(testToAddress, testAmqpMessage.getTo());
    }

    @Test
    public void testGetReplyToWithReceivedMessageWithNoProperties()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        String replyToAddress = testAmqpMessage.getReplyTo();
        assertNotNull(replyToAddress);
        assertEquals(testReplyToAddress, replyToAddress);
    }

    @Test
    public void testSetReplyTo()
    {
        String testReplyToAddress = "myTestAddress";

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertNull(testAmqpMessage.getReplyTo());

        testAmqpMessage.setReplyTo(testReplyToAddress);

        assertNotNull(testAmqpMessage.getReplyTo());
        assertEquals(testReplyToAddress, testAmqpMessage.getReplyTo());
    }

    @Test
    public void testNewMessageHasNoUnderlyingPropertiesSection()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getProperties());
    }

    @Test
    public void testGetCreationTimeIsNullForNewMessage()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertNull(testAmqpMessage.getCreationTime());
    }

    @Test
    public void testSetCreationTimeOnNewMessage()
    {
        Long timestamp = System.currentTimeMillis();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        testAmqpMessage.setCreationTime(timestamp);

        assertEquals(timestamp.longValue(), testAmqpMessage.getMessage().getProperties().getCreationTime().getTime());
        assertEquals(timestamp, testAmqpMessage.getCreationTime());
    }

    @Test
    public void testSetCreationTimeNullOnMessageWithExistingCreationTime()
    {
        Long timestamp = System.currentTimeMillis();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();
        testAmqpMessage.setCreationTime(timestamp);

        testAmqpMessage.setCreationTime(null);

        assertNull(testAmqpMessage.getMessage().getProperties().getCreationTime());
        assertNull(testAmqpMessage.getCreationTime());
    }

    @Test
    public void testGetAbsoluteExpiryTimeIsNullForNewMessage()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        assertNull(testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testSetAbsoluteExpiryTimeOnNewMessage()
    {
        Long timestamp = System.currentTimeMillis();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        testAmqpMessage.setAbsoluteExpiryTime(timestamp);

        assertEquals(timestamp.longValue(), testAmqpMessage.getMessage().getProperties().getAbsoluteExpiryTime().getTime());
        assertEquals(timestamp, testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testSetAbsoluteExpiryTimeNullOnMessageWithExistingExpiryTime()
    {
        Long timestamp = System.currentTimeMillis();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();
        testAmqpMessage.setAbsoluteExpiryTime(timestamp);

        testAmqpMessage.setAbsoluteExpiryTime(null);

        assertNull(testAmqpMessage.getMessage().getProperties().getAbsoluteExpiryTime());
        assertNull(testAmqpMessage.getAbsoluteExpiryTime());
    }

    // ====== Message Annotations =======

    @Test
    public void testMessageAnnotationExistsUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertTrue(testAmqpMessage.messageAnnotationExists(symbolKeyName));
        assertFalse(testAmqpMessage.messageAnnotationExists("otherName"));
    }

    @Test
    public void testGetMessageAnnotationUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        String symbolKeyName = "myTestSymbolName";

        Message message = Proton.message();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(value, testAmqpMessage.getMessageAnnotation(symbolKeyName));
        assertNull(testAmqpMessage.getMessageAnnotation("otherName"));
    }

    @Test
    public void testNewMessageHasNoUnderlyingMessageAnnotationsSection()
    {
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getMessageAnnotations());
    }

    @Test
    public void testSetMessageAnnotationOnNewMessage()
    {
        String symbolKeyName = "myTestSymbolName";
        String symbolKeyName2 = "myTestSymbolName2";
        String value = "myTestValue";

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage();

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(value, testAmqpMessage.getMessageAnnotation(symbolKeyName));
        assertNull(testAmqpMessage.getMessageAnnotation("otherName"));

        testAmqpMessage.clearMessageAnnotation(symbolKeyName);
        assertNull(testAmqpMessage.getMessageAnnotation(symbolKeyName));
    }

    @Test
    public void testClearMessageAnnotationONMessageWithNoMessageAnnotationSectionDoesntThrowException()
    {
        Message message = Proton.message();

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        testAmqpMessage.clearMessageAnnotation("keyName");
    }

    @Test
    public void testClearAllMessageAnnotationsUsingNewMessage()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        testAmqpMessage.clearAllMessageAnnotations();

        Message underlying = testAmqpMessage.getMessage();
        assertNull(underlying.getMessageAnnotations());
    }

    @Test
    public void testGetMessageAnnotationsCountUsingReceivedMessageWithoutMessageAnnotationsSection()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

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

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(2, testAmqpMessage.getMessageAnnotationsCount());
        testAmqpMessage.clearMessageAnnotation(symbolKeyName);
        assertEquals(1, testAmqpMessage.getMessageAnnotationsCount());
    }
}
