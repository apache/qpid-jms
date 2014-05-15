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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpTextMessage;
import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpMessageFactoryTest extends QpidJmsTestCase
{
   private AmqpConnection _mockAmqpConnection;
   private Delivery _mockDelivery;
   private AmqpMessageFactory _amqpMessageFactory;

   @Before
   @Override
   public void setUp() throws Exception
   {
       super.setUp();
       _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
       _mockDelivery = Mockito.mock(Delivery.class);
       _amqpMessageFactory = new AmqpMessageFactory();
   }

   // =============== With The Message Type Annotation =========
   // ==========================================================

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#GENERIC_MESSAGE_TYPE} is
    * treated as an {@link AmqpGenericMessage}
    */
   @Test
   public void testCreateAmqpGenericMessageFromMessageTypeAnnotation() throws Exception
   {
       Message message = Proton.message();

       Map<Symbol,Object> map = new HashMap<Symbol,Object>();
       map.put(Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE), ClientProperties.GENERIC_MESSAGE_TYPE);

       MessageAnnotations messageAnnotations = new MessageAnnotations(map);
       message.setMessageAnnotations(messageAnnotations);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpGenericMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#BYTES_MESSAGE_TYPE} is
    * treated as an {@link AmqpBytesMessage}
    */
   @Test
   public void testCreateAmqpBytesMessageFromMessageTypeAnnotation() throws Exception
   {
       Message message = Proton.message();

       Map<Symbol,Object> map = new HashMap<Symbol,Object>();
       map.put(Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE), ClientProperties.BYTES_MESSAGE_TYPE);

       MessageAnnotations messageAnnotations = new MessageAnnotations(map);
       message.setMessageAnnotations(messageAnnotations);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#TEXT_MESSAGE_TYPE} is
    * treated as an {@link AmqpTextMessage}
    */
   @Test
   public void testCreateAmqpTextMessageFromMessageTypeAnnotation() throws Exception
   {
       Message message = Proton.message();

       Map<Symbol,Object> map = new HashMap<Symbol,Object>();
       map.put(Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE), ClientProperties.TEXT_MESSAGE_TYPE);

       MessageAnnotations messageAnnotations = new MessageAnnotations(map);
       message.setMessageAnnotations(messageAnnotations);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#OBJECT_MESSSAGE_TYPE} is
    * treated as an {@link AmqpObjectMessage}
    */
   @Test
   public void testCreateAmqpObjectMessageFromMessageTypeAnnotation() throws Exception
   {
       createAmqpObjectMessageFromMessageTypeAnnotation(false);
   }

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#OBJECT_MESSSAGE_TYPE} and
    * content type set to {@value AmqpObjectMessageSerializedDelegate#CONTENT_TYPE}
    * is treated as an {@link AmqpObjectMessage}
    */
   @Test
   public void testCreateSerializedAmqpObjectMessageFromMessageTypeAnnotation() throws Exception
   {
       createAmqpObjectMessageFromMessageTypeAnnotation(true);
   }

   private void createAmqpObjectMessageFromMessageTypeAnnotation(boolean serialized)
   {
       Message message = Proton.message();

       Map<Symbol,Object> map = new HashMap<Symbol,Object>();
       map.put(Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE), ClientProperties.OBJECT_MESSSAGE_TYPE);

       MessageAnnotations messageAnnotations = new MessageAnnotations(map);
       message.setMessageAnnotations(messageAnnotations);

       if(serialized)
       {
           message.setContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE);
       }

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with the {@value ClientProperties#X_OPT_JMS_MSG_TYPE}
    * annotation set to  {@value ClientProperties#STREAM_MESSAGE_TYPE} is
    * treated as an {@link AmqpListMessage}
    */
   @Test
   public void testCreateAmqpListMessageFromMessageTypeAnnotation() throws Exception
   {
       Message message = Proton.message();

       Map<Symbol,Object> map = new HashMap<Symbol,Object>();
       map.put(Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE), ClientProperties.STREAM_MESSAGE_TYPE);

       MessageAnnotations messageAnnotations = new MessageAnnotations(map);
       message.setMessageAnnotations(messageAnnotations);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpListMessage.class, amqpMessage.getClass());
   }

   // =============== No Body =============
   // =====================================

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpBytesMessage#CONTENT_TYPE} results in a bytes message when not
    * otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpBytesMessageFromNoBodySectionAndContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       message.setContentType(AmqpBytesMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with no body section, and no content-type
    * results in a bytes message when not otherwise annotated
    * to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpBytesMessageFromNoBodySectionAndNoContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       assertNull(message.getContentType());

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpObjectMessageSerializedDelegate#CONTENT_TYPE} results in a object message
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpObjectMessageFromNoBodySectionAndContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       message.setContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpTextMessage#CONTENT_TYPE} results in a text message
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpTextMessageFromNoBodySectionAndContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       message.setContentType(AmqpTextMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with no body section, and with the content type set to
    * an unknown value results in a 'generic message' when not otherwise
    * annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpGenericMessageFromNoBodySectionAndUnknownContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       message.setContentType("unknown-content-type");

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpGenericMessage.class, amqpMessage.getClass());
   }

   // =============== data ================
   // =====================================

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AmqpBytesMessage#CONTENT_TYPE} results in a bytes message when not
    * otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpBytesMessageFromDataWithEmptyBinaryAndContentType() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType(AmqpBytesMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a data body containing nothing and with no content type set
    * results in a bytes message when not otherwise annotated to
    * indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpBytesMessageFromDataWithEmptyBinaryAndNoContentType() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       assertNull(message.getContentType());

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AmqpObjectMessageSerializedDelegate#CONTENT_TYPE} results in a object message
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpObjectMessageFromDataWithContentTypeAndEmptyBinary() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AmqpTextMessage#CONTENT_TYPE} results in a text message when
    * not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpTextMessageFromDataWithContentTypeAndEmptyBinary() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType(AmqpTextMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   /**
    * TODO: should this situation throw an exception, or be a bytes message?
    *
    * Test that a data body containing nothing, but with the content type set to
    * an unknown value results in a 'generic message' when not otherwise
    * annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpGenericMessageFromDataWithUnknownContentTypeAndEmptyBinary() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType("unknown-content-type");

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpGenericMessage.class, amqpMessage.getClass());
   }

   // ============= amqp-value ============
   // =====================================

   /**
    * Test that an amqp-value body containing a string results in a text message
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpTextMessageFromAmqpValueWithString() throws Exception
   {
       Message message = Proton.message();
       message.setBody(new AmqpValue("content"));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a null results in an {@link AmqpTextMessage}
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpTextMessageFromAmqpValueWithNull() throws Exception
   {
       Message message = Proton.message();
       message.setBody(new AmqpValue(null));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a map results in an {@link AmqpObjectMessage}
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpObjectMessageFromAmqpValueWithMap() throws Exception
   {
       Message message = Proton.message();
       Map<String,String> map = new HashMap<String,String>();
       message.setBody(new AmqpValue(map));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a list results in an {@link AmqpObjectMessage}
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpObjectMessageFromAmqpValueWithList() throws Exception
   {
       Message message = Proton.message();
       List<String> list = new ArrayList<String>();
       message.setBody(new AmqpValue(list));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a binary value results in {@link AmqpBytesMessage}
    * when not otherwise annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpBytesMessageFromAmqpValueWithBinary() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new AmqpValue(binary));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a value which can't
    * be categorised results in an object message when not otherwise
    * annotated to indicate the type of JMS message it is.
    */
   @Test
   public void testCreateAmqpGenericMessageFromAmqpValueWithUncategorisedContent() throws Exception
   {
       Message message = Proton.message();
       message.setBody(new AmqpValue(new Object()));//This obviously shouldn't happen in practice
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   // ============= amqp-sequence ============
   // ========================================

   /**
    * Test that an amqp-sequence body results in a 'generic message' until support is implemented.
    * TODO: change once support for amqp-sequence body sections is implemented.
    */
   @Test
   public void testCreateAmqpGenericMessageFromAmqpSequence() throws Exception
   {
       Message message = Proton.message();
       List<String> list = new ArrayList<String>();
       message.setBody(new AmqpSequence(list));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpGenericMessage.class, amqpMessage.getClass());
   }
}

