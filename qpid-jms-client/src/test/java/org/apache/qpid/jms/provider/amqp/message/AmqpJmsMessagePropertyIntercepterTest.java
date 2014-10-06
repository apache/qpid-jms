/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_TTL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_TYPED_ENCODING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.JMSException;

import org.junit.Test;
import org.mockito.Mockito;

public class AmqpJmsMessagePropertyIntercepterTest {

    //-------- JMS_AMQP_TTL --------------------------------------------------//

    @Test
    public void testJmsAmqpTtlInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_TTL));
    }

    @Test
    public void testGetJmsAmqpTtlWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
    }

    @Test
    public void testSetJmsAmqpTtl() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TTL, 65536L);
        Mockito.verify(message).setAmqpTimeToLiveOverride(65536L);
    }

    @Test
    public void testGetJmsAmqpTtlWhenSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.hasAmqpTimeToLiveOverride()).thenReturn(true);
        Mockito.when(message.getAmqpTimeToLiveOverride()).thenReturn(65536L);

        assertNotNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
        assertEquals(65536L, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlInPropertyNamesWhenSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.hasAmqpTimeToLiveOverride()).thenReturn(true);
        Mockito.when(message.getAmqpTimeToLiveOverride()).thenReturn(65536L);
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TTL));
    }

    //-------- JMS_AMQP_REPLY_TO_GROUP_ID ------------------------------------//

    @Test
    public void testJmsAmqpReplyToGroupIdInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testSetJmsAmqpReplyToGroupId() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID, testValue);
        Mockito.verify(message).setReplyToGroupId(testValue);
    }

    @Test
    public void testGetJmsAmqpReplyToGroupIdWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testGetJmsAmqpReplyToGroupIdWhenSet() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToGroupId()).thenReturn(testValue);
        assertNotNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
        assertEquals(testValue, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsJmsAmqpReplyToGroupIdNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsAmqpReplyToGroupIdInPropertyNamesWhenSet() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToGroupId()).thenReturn(testValue);
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    //-------- JMS_AMQP_TYPED_ENCODING ---------------------------------------//

    @Test
    public void testJmsAmqpTypedEncodingInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testSetJmsAmqpTypedEncoding() throws JMSException {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TYPED_ENCODING, true);
        Mockito.verify(message).setUseAmqpTypedEncoding(true);
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWithNonObjectMessage() throws JMSException {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWhenUsingSerializatio() throws JMSException {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(false);
        assertEquals(false, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWhenUsingAmqpTypes() throws JMSException {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(true);
        assertEquals(true, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(false);
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingInPropertyNamesWhenSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(true);
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TYPED_ENCODING));
    }
}
