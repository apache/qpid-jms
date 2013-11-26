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
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
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

   // =============== No Body =============
   // =====================================

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpBytesMessage#CONTENT_TYPE} results in a bytes message
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
    * results in a bytes message
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
    * {@value AmqpObjectMessage#CONTENT_TYPE} results in a object message
    */
   @Test
   public void testCreateAmqpObjectMessageFromNoBodySectionAndContentType() throws Exception
   {
       //TODO: this test only required if we decide that not sending a content body is legal
       Message message = Proton.message();
       message.setContentType(AmqpObjectMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AmqpTextMessage#CONTENT_TYPE} results in a text message
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

   // =============== data ================
   // =====================================

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AmqpBytesMessage#CONTENT_TYPE} results in a bytes message
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
    * results in a text message
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
    * {@value AmqpObjectMessage#CONTENT_TYPE} results in a object message
    */
   @Test
   public void testCreateAmqpObjectMessageFromDataWithEmptyBinaryAndContentType() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType(AmqpObjectMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpObjectMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AmqpTextMessage#CONTENT_TYPE} results in a text message
    */
   @Test
   public void testCreateAmqpTextMessageFromDataWithEmptyBinaryAndContentType() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       message.setContentType(AmqpTextMessage.CONTENT_TYPE);

       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpTextMessage.class, amqpMessage.getClass());
   }

   // ============= amqp-value ============
   // =====================================

   /**
    * Test that an amqp-value body containing a string results in a text message
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
    * Test that an amqp-value body containing a null results in a text message
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
    * Test that an amqp-value body containing a map results in a map message
    */
   @Test
   public void testCreateAmqpMapMessageFromAmqpValueWithMap() throws Exception
   {
       Message message = Proton.message();
       Map<String,String> map = new HashMap<String,String>();
       message.setBody(new AmqpValue(map));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpMapMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a list results in a list message
    * (which will be used as the basis of the JMS StreamMessage)
    */
   @Test
   public void testCreateAmqpListMessageFromAmqpValueWithList() throws Exception
   {
       Message message = Proton.message();
       List<String> list = new ArrayList<String>();
       message.setBody(new AmqpValue(list));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpListMessage.class, amqpMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a binary value results in a bytes message
    */
   @Test
   public void testCreateAmqpBytesMessageFromAmqpValueWithBinary() throws Exception
   {
       Message message = Proton.message();
       Binary binary = new Binary(new byte[0]);
       message.setBody(new Data(binary));
       AmqpMessage amqpMessage = _amqpMessageFactory.createAmqpMessage(_mockDelivery, message, _mockAmqpConnection);
       assertEquals(AmqpBytesMessage.class, amqpMessage.getClass());
   }
}

