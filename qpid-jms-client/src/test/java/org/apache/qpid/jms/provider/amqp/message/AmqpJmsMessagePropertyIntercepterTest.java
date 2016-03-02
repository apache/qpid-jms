/*
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
import static org.junit.Assert.fail;

import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AmqpJmsMessagePropertyIntercepterTest {

    //---------- Non-Intercepted -------------------------------------------------//

    @Test
    public void testCreate() throws JMSException {
        new AmqpJmsMessagePropertyIntercepter();
    }

    @Test
    public void testGetPropertyWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, "SomeRandomPropertyName"));
        Mockito.verify(message).getApplicationProperty(Mockito.anyString());
    }

    @Test
    public void testSetPropertyWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        AmqpJmsMessagePropertyIntercepter.setProperty(message, "SomeRandomPropertyName", "Something");
        Mockito.doThrow(new JMSException("Expected")).when(message).setApplicationProperty(Mockito.anyString(), Mockito.anyString());
        try {
            AmqpJmsMessagePropertyIntercepter.setProperty(message, "SomeRandomPropertyName", "Something");
            fail("Should have thrown");
        } catch (JMSException ex) {
            assertEquals("Expected", ex.getMessage());
        }
    }

    @Test
    public void testPropertyExistsWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, "SomeRandomPropertyName"));
        Mockito.verify(message).applicationPropertyExists(Mockito.anyString());
    }

    //-------- JMS_AMQP_TTL --------------------------------------------------//

    @Test
    public void testJmsAmqpTtlInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_TTL));
    }

    @Test
    public void testGetJmsAmqpTtlWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
    }

    @Test
    public void testSetJmsAmqpTtl() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TTL, 65536L);
        Mockito.verify(message).setAmqpTimeToLiveOverride(65536L);
    }

    @Test
    public void testGetJmsAmqpTtlWhenSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.hasAmqpTimeToLiveOverride()).thenReturn(true);
        Mockito.when(message.getAmqpTimeToLiveOverride()).thenReturn(65536L);

        assertNotNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
        assertEquals(65536L, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TTL));
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlInPropertyNamesWhenSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.hasAmqpTimeToLiveOverride()).thenReturn(true);
        Mockito.when(message.getAmqpTimeToLiveOverride()).thenReturn(65536L);
        Mockito.when(message.getApplicationPropertyNames(Mockito.anySetOf(String.class))).then(new PassPropertyNames());
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlIPropertExistsWhenSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.hasAmqpTimeToLiveOverride()).thenReturn(true);
        Mockito.when(message.getAmqpTimeToLiveOverride()).thenReturn(65536L);
        assertTrue(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_TTL));
    }

    @Test
    public void testJmsAmqpTtlPropertExistsWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_TTL));
    }

    @Test
    public void testSetJmsAmqpTtlConversionChecks() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        try {
            AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TTL, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    //-------- JMS_AMQP_REPLY_TO_GROUP_ID ------------------------------------//

    @Test
    public void testJmsAmqpReplyToGroupIdInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testSetJmsAmqpReplyToGroupId() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID, testValue);
        Mockito.verify(message).setReplyToGroupId(testValue);
    }

    @Test
    public void testGetJmsAmqpReplyToGroupIdWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testGetJmsAmqpReplyToGroupIdWhenSet() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.getReplyToGroupId()).thenReturn(testValue);
        assertNotNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
        assertEquals(testValue, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsJmsAmqpReplyToGroupIdNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID));
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsAmqpReplyToGroupIdInPropertyNamesWhenSet() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.getApplicationPropertyNames(Mockito.anySetOf(String.class))).then(new PassPropertyNames());
        Mockito.when(message.getReplyToGroupId()).thenReturn(testValue);
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsAmqpReplyToGroupIdPropertExistsWhenSet() throws JMSException {
        String testValue = "ReplyToGroupId";
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.getReplyToGroupId()).thenReturn(testValue);
        assertTrue(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testJmsAmqpReplyToGroupIdPropertExistsWhenNotSet() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        Mockito.when(message.getReplyToGroupId()).thenReturn(null);
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_REPLY_TO_GROUP_ID));
        Mockito.when(message.getReplyToGroupId()).thenReturn("");
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_REPLY_TO_GROUP_ID));
    }

    @Test
    public void testSetJmsAmqpReplyToGroupIdConversionChecks() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        try {
            AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_REPLY_TO_GROUP_ID, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    //-------- JMS_AMQP_TYPED_ENCODING ---------------------------------------//

    @Test
    public void testJmsAmqpTypedEncodingInGetAllPropertyNames() throws JMSException {
        assertTrue(AmqpJmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testSetJmsAmqpTypedEncoding() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TYPED_ENCODING, true);
        Mockito.verify(message).setUseAmqpTypedEncoding(true);
    }

    @Test
    public void testSetJmsAmqpTypedEncodingOnNonObjectMessage() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        try {
            AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TYPED_ENCODING, true);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
        }
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWithNonObjectMessage() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertNull(AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWhenUsingSerializatio() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(false);
        assertEquals(false, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testGetJmsAmqpTypedEncodingWhenUsingAmqpTypes() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(true);
        assertEquals(true, AmqpJmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingNotInPropertyNamesWhenNotSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(false);
        assertFalse(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingInPropertyNamesWhenSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(true);
        assertTrue(AmqpJmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingPropertExistsWhenSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(true);
        assertTrue(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingdPropertExistsWhenNotSet() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        Mockito.when(message.isAmqpTypedEncoding()).thenReturn(false);
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testJmsAmqpTypedEncodingdPropertExistsWhenNotAnObjectMessage() throws JMSException {
        AmqpJmsMessageFacade message = createAmqpMessageFacade();
        assertFalse(AmqpJmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_TYPED_ENCODING));
    }

    @Test
    public void testSetJmsAmqpTypedEncodingConversionChecks() throws JMSException {
        AmqpJmsObjectMessageFacade message = createAmqpObjectMessageFacade();
        try {
            AmqpJmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_TYPED_ENCODING, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    //--------- Utilities ----------------------------------------------------//

    private AmqpJmsMessageFacade createAmqpMessageFacade() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getApplicationPropertyNames(Mockito.anySetOf(String.class))).then(new PassPropertyNames());
        return message;
    }

    private AmqpJmsObjectMessageFacade createAmqpObjectMessageFacade() {
        AmqpJmsObjectMessageFacade message = Mockito.mock(AmqpJmsObjectMessageFacade.class);
        Mockito.when(message.getApplicationPropertyNames(Mockito.anySetOf(String.class))).then(new PassPropertyNames());
        return message;
    }

    private class PassPropertyNames implements Answer<Set<String>> {

        @SuppressWarnings("unchecked")
        @Override
        public Set<String> answer(InvocationOnMock invocation) throws Throwable {
            return (Set<String>) invocation.getArguments()[0];
        }
    }
}
