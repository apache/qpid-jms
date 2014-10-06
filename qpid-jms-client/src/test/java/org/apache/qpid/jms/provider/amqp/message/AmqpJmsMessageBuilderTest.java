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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsMessageBuilderTest extends QpidJmsTestCase {
    private AmqpConsumer mockConsumer;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockConsumer = Mockito.mock(AmqpConsumer.class);
    }

    // =============== With The Message Type Annotation =========
    // ==========================================================

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_MESSAGE} is
     * treated as a generic {@link JmsMessage} with {@link AmqpJmsMessageFacade}
     */
    @Test(expected = IOException.class)
    public void testCreateMessageFromUnknownMessageTypeAnnotationValueThrows() throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), (byte) -1);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_MESSAGE} is
     * treated as a generic {@link JmsMessage} with {@link AmqpJmsMessageFacade}
     */
    @Test
    public void testCreateGenericMessageFromMessageTypeAnnotation() throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), AmqpMessageSupport.JMS_MESSAGE);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_BYTES_MESSAGE} is
     * treated as a {@link JmsBytesMessage} with {@link AmqpJmsBytesMessageFacade}
     */
    @Test
    public void testCreateBytesMessageFromMessageTypeAnnotation() throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), AmqpMessageSupport.JMS_BYTES_MESSAGE);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_BYTES_MESSAGE} is
     * treated as a {@link JmsTextMessage} with {@link AmqpJmsTextMessageFacade}
     */
    @Test
    public void testCreateTextMessageFromMessageTypeAnnotation() throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), AmqpMessageSupport.JMS_TEXT_MESSAGE);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsTextMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsTextMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_OBJECT_MESSAGE} and
     * content-type set to {@value AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE} is
     * treated as a {@link JmsObjectMessage} with {@link AmqpJmsObjectMessageFacade}
     * containing a {@link AmqpSerializedObjectDelegate}.
     */
    @Test
    public void testCreateObjectMessageFromMessageTypeAnnotation() throws Exception {
        createObjectMessageFromMessageTypeAnnotationTestImpl(true);
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_OBJECT_MESSAGE} and
     * content-type not set is treated as a {@link JmsObjectMessage} with
     * {@link AmqpJmsObjectMessageFacade} containing a {@link AmqpTypedObjectDelegate}.
     */
    @Test
    public void testCreateObjectMessageFromMessageTypeAnnotationAnd() throws Exception {
        createObjectMessageFromMessageTypeAnnotationTestImpl(false);
    }

    private void createObjectMessageFromMessageTypeAnnotationTestImpl(boolean setJavaSerializedContentType) throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), AmqpMessageSupport.JMS_OBJECT_MESSAGE);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        if (setJavaSerializedContentType) {
            message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
        }

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        if (setJavaSerializedContentType) {
            assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpSerializedObjectDelegate);
        } else {
            assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpTypedObjectDelegate);
        }
    }

    /**
     * Test that a message with the {@value AmqpMessageSupport#JMS_MSG_TYPE}
     * annotation set to  {@value AmqpMessageSupport#JMS_STREAM_MESSAGE} is
     * treated as a {@link JmsStreamMessage} with {@link AmqpJmsStreamMessageFacade}
     */
    @Test
    public void testCreateStreamMessageFromMessageTypeAnnotation() throws Exception {
        Message message = Proton.message();

        Map<Symbol, Object> map = new HashMap<Symbol, Object>();
        map.put(Symbol.valueOf(AmqpMessageSupport.JMS_MSG_TYPE), AmqpMessageSupport.JMS_STREAM_MESSAGE);

        MessageAnnotations messageAnnotations = new MessageAnnotations(map);
        message.setMessageAnnotations(messageAnnotations);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsStreamMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsStreamMessageFacade.class, facade.getClass());
    }

    // =============== Without The Message Type Annotation =========
    // =============================================================

    // --------- No Body Section ---------

    /**
     * Test that a message with no body section, but with the content type set to
     * {@value AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE} results in a BytesMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateBytesMessageFromNoBodySectionAndContentType() throws Exception {
        Message message = Proton.message();
        message.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with no body section, and no content-type results in a BytesMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateBytesMessageFromNoBodySectionAndNoContentType() throws Exception {
        Message message = Proton.message();

        assertNull(message.getContentType());

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpMessageSupport#SERIALIZED_JAVA_OBJECT_CONTENT_TYPE} results in an ObjectMessage
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
    @Test
    public void testCreateObjectMessageFromNoBodySectionAndContentType() throws Exception {
        Message message = Proton.message();
        message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpSerializedObjectDelegate);
    }

    /**
     * Test that a message with no body section, but with the content type set to
     * {@value AmqpMessageSupport#TEXT_PLAIN_CONTENT_TYPE} results in a TextMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateTextMessageFromNoBodySectionAndContentType() throws Exception {
        Message message = Proton.message();
        message.setContentType(AmqpMessageSupport.TEXT_PLAIN_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsTextMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsTextMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with no body section, and with the content type set to
     * an unknown value results in an exception when not otherwise annotated to
     * indicate the type of JMS message it is.
     */
    @Test(expected = IOException.class)
    public void testNoBodySectionAndUnknownContentTypeThrowsException() throws Exception {
        //TODO: decide if this should instead just be a plain Message
        Message message = Proton.message();
        message.setContentType("unknown-content-type");

        AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
    }

    // --------- Data Body Section ---------

    /**
     * Test that a data body containing nothing, but with the content type set to
     * {@value AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE} results in a BytesMessage when not
     * otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateBytesMessageFromDataWithEmptyBinaryAndContentType() throws Exception {
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new Data(binary));
        message.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a receiving a data body containing nothing and no content type being set
     * results in a BytesMessage when not otherwise annotated to indicate the type of
     * JMS message it is.
     */
    @Test
    public void testCreateBytesMessageFromDataWithEmptyBinaryAndNoContentType() throws Exception {
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new Data(binary));

        assertNull(message.getContentType());

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
     * Test that receiving a data body containing nothing, but with the content type set to
     * {@value AmqpMessageSupport#SERIALIZED_JAVA_OBJECT_CONTENT_TYPE} results in an ObjectMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateObjectMessageFromDataWithContentTypeAndEmptyBinary() throws Exception {
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new Data(binary));
        message.setContentType(AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpSerializedObjectDelegate);
    }

    /**
     * Test that receiving a data body containing nothing, but with the content type set to
     * {@value AmqpMessageSupport#TEXT_PLAIN_CONTENT_TYPE} results in a TextMessage when
     * not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateTextMessageFromDataWithContentTypeAndEmptyBinary() throws Exception {
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new Data(binary));
        message.setContentType(AmqpMessageSupport.TEXT_PLAIN_CONTENT_TYPE);

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsTextMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsTextMessageFacade.class, facade.getClass());
    }

    /**
     * Test that a message with a data body section, and with the content type set
     * to an unknown value results in an exception when not otherwise annotated to
     * indicate the type of JMS message it is.
     */
    @Test(expected = IOException.class)
    public void testDataWithUnknownContentTypeAndEmptyBinaryThrowsException() throws Exception {
        //TODO: decide if this should instead just be a plain Message or BytesMessage instead?
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new Data(binary));
        message.setContentType("unknown-content-type");

        AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
    }

    // --------- AmqpValue Body Section ---------

    /**
     * Test that an amqp-value body containing a string results in a TextMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateTextMessageFromAmqpValueWithString() throws Exception {
        Message message = Proton.message();
        message.setBody(new AmqpValue("content"));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsTextMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsTextMessageFacade.class, facade.getClass());
    }

    /**
     * Test that an amqp-value body containing a null results in an TextMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateTextMessageFromAmqpValueWithNull() throws Exception {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsTextMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsTextMessageFacade.class, facade.getClass());
    }

    /**
     * Test that an amqp-value body containing a map results in an ObjectMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateAmqpObjectMessageFromAmqpValueWithMap() throws Exception {
        Message message = Proton.message();
        Map<String, String> map = new HashMap<String,String>();
        message.setBody(new AmqpValue(map));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpTypedObjectDelegate);
    }

    /**
     * Test that an amqp-value body containing a list results in an ObjectMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateAmqpObjectMessageFromAmqpValueWithList() throws Exception {
        Message message = Proton.message();
        List<String> list = new ArrayList<String>();
        message.setBody(new AmqpValue(list));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpTypedObjectDelegate);
    }

    /**
     * Test that an amqp-value body containing a binary value results in BytesMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateAmqpBytesMessageFromAmqpValueWithBinary() throws Exception {
        Message message = Proton.message();
        Binary binary = new Binary(new byte[0]);
        message.setBody(new AmqpValue(binary));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsBytesMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsBytesMessageFacade.class, facade.getClass());
    }

    /**
     * Test that an amqp-value body containing a value which can't be categorised results in
     * an ObjectMessage when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateObjectMessageFromAmqpValueWithUncategorisedContent() throws Exception {
        Message message = Proton.message();
        message.setBody(new AmqpValue(new Object()));// This obviously shouldn't happen in practice

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpTypedObjectDelegate);
    }

    // --------- AmqpSequence Body Section ---------

    /**
     * Test that an amqp-sequence body containing a binary value results in an ObjectMessage
     * when not otherwise annotated to indicate the type of JMS message it is.
     */
    @Test
    public void testCreateObjectMessageMessageFromAmqpSequence() throws Exception
    {
        Message message = Proton.message();
        List<String> list = new ArrayList<String>();
        message.setBody(new AmqpSequence(list));

        JmsMessage jmsMessage = AmqpJmsMessageBuilder.createJmsMessage(mockConsumer, message);
        assertNotNull("Message should not be null", jmsMessage);
        assertEquals("Unexpected message class type", JmsObjectMessage.class, jmsMessage.getClass());

        JmsMessageFacade facade = jmsMessage.getFacade();
        assertNotNull("Facade should not be null", facade);
        assertEquals("Unexpected facade class type", AmqpJmsObjectMessageFacade.class, facade.getClass());

        AmqpObjectTypeDelegate delegate = ((AmqpJmsObjectMessageFacade) facade).getDelegate();
        assertTrue("Unexpected delegate type: " + delegate, delegate instanceof AmqpTypedObjectDelegate);
    }
}
