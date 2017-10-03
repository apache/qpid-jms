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
package org.apache.qpid.jms.message;

import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_DELIVERY_COUNT;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPSEQ;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_USERID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_AMQP_ACK_TYPE;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_CORRELATIONID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DELIVERYTIME;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DELIVERY_MODE;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DESTINATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_EXPIRATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_MESSAGEID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_PRIORITY;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REDELIVERED;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REPLYTO;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TIMESTAMP;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TYPE;
import static org.apache.qpid.jms.message.JmsMessageSupport.RELEASED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.JmsAcknowledgeCallback;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.junit.Test;
import org.mockito.Mockito;

public class JmsMessagePropertyIntercepterTest {

    //---------- Non-Intercepted -------------------------------------------------//

    @Test
    public void testCreate() throws JMSException {
        new JmsMessagePropertyIntercepter();
    }

    @Test
    public void testGetPropertyWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, "SomeRandomPropertyName"));
        Mockito.verify(facade).getProperty(Mockito.anyString());
    }

    @Test
    public void testSetPropertyWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, "SomeRandomPropertyName", "Something");
        Mockito.doThrow(new JMSException("Expected")).when(facade).setProperty(Mockito.anyString(), Mockito.anyString());
        try {
            JmsMessagePropertyIntercepter.setProperty(message, "SomeRandomPropertyName", "Something");
            fail("Should have thrown");
        } catch (JMSException ex) {
            assertEquals("Expected", ex.getMessage());
        }
    }

    @Test
    public void testPropertyExistsWithNonInterceptedNameCallsIntoFacade() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, "SomeRandomPropertyName"));
        Mockito.verify(facade).propertyExists(Mockito.anyString());
    }

    //---------- JMSDestination --------------------------------------------------//

    @Test
    public void testJMSDestinationInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_DESTINATION));
    }

    @Test
    public void testGetJMSDestinationWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_DESTINATION));
        Mockito.verify(facade).getDestination();
    }

    @Test
    public void testGetJMSDestinationWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination destination = new JmsQueue("TestDestination");
        Mockito.when(facade.getDestination()).thenReturn(destination);
        assertEquals(destination.getAddress(), JmsMessagePropertyIntercepter.getProperty(message, JMS_DESTINATION));
    }

    @Test(expected=JMSException.class)
    public void testSetJMSDestination() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        String destinationName = new String("TestDestination");
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DESTINATION, destinationName);
    }

    @Test
    public void testJMSDestinationInGetPropertyNamesWhenSet() throws JMSException {
        doJMSDestinationInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSDestinationInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSDestinationInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSDestinationInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(facade.getDestination()).thenReturn(queue);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_DESTINATION));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DESTINATION));
        }
    }

    @Test
    public void testJMSDestinationNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DESTINATION));
    }

    @Test
    public void testJMSDestinationPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(facade.getDestination()).thenReturn(queue);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DESTINATION));
    }

    @Test
    public void testJMSDestinationPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDestination()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DESTINATION));
    }

    @Test
    public void testSetJMSDestinationConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_DESTINATION, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSDestinationClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDestination()).thenReturn(null);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setDestination(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setDestination(null);
    }

    //---------- JMSReplyTo --------------------------------------------------//

    @Test
    public void testJMSReplyToInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_REPLYTO));
    }

    @Test
    public void testGetJMSReplyToWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_REPLYTO));
        Mockito.verify(facade).getReplyTo();
    }

    @Test
    public void testGetJMSReplyToWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination destination = new JmsQueue("TestDestination");
        Mockito.when(facade.getReplyTo()).thenReturn(destination);
        assertEquals(destination.getAddress(), JmsMessagePropertyIntercepter.getProperty(message, JMS_REPLYTO));
    }

    @Test(expected=JMSException.class)
    public void testSetJMSReplyTo() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        String destinationName = new String("TestDestination");
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REPLYTO, destinationName);
    }

    @Test
    public void testJMSReplyToInGetPropertyNamesWhenSet() throws JMSException {
        doJMSReplyToInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSReplyToNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSReplyToInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSReplyToInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(facade.getReplyTo()).thenReturn(queue);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_REPLYTO));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_REPLYTO));
        }
    }

    @Test
    public void testJMSReplyToNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_REPLYTO));
    }

    @Test
    public void testJMSReplyToPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(facade.getReplyTo()).thenReturn(queue);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_REPLYTO));
    }

    @Test
    public void testJMSReplyToPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getReplyTo()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_REPLYTO));
    }

    @Test
    public void testSetJMSReplyToConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_REPLYTO, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSRepltyToClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setReplyTo(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setReplyTo(null);
    }

    //---------- JMSType -----------------------------------------------------//

    @Test
    public void testJMSTypeInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_TYPE));
    }

    @Test
    public void testGetJMSTypeWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_TYPE));
        Mockito.verify(facade).getType();
    }

    @Test
    public void testGetJMSTypeWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getType()).thenReturn("MsgType");
        assertEquals("MsgType", JmsMessagePropertyIntercepter.getProperty(message, JMS_TYPE));
    }

    @Test
    public void testSetJMSType() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_TYPE, "SomeType");
        Mockito.verify(facade).setType("SomeType");
    }

    @Test
    public void testJMSTypeInGetPropertyNamesWhenSet() throws JMSException {
        doJMSTypeInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSTypeNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSTypeInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSTypeInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getType()).thenReturn("SomeType");
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_TYPE));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_TYPE));
        }
    }

    @Test
    public void testJMSTypeNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_TYPE));
    }

    @Test
    public void testJMSTypePropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getType()).thenReturn("SomeType");
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_TYPE));
    }

    @Test
    public void testJMSTypePropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getType()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_TYPE));
    }

    @Test
    public void testSetJMSTypeConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_TYPE, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSTypeClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setType(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setType(null);
    }

    //---------- JMSDeliveryMode ---------------------------------------------//

    @Test
    public void testJMSDeliveryModeInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testGetJMSDeliveryModeWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertEquals("NON_PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
        Mockito.verify(facade).isPersistent();
    }

    @Test
    public void testGetJMSDeliverModeSetDurable() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(true);
        assertEquals("PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testGetJMSDeliverModeSetNonDurable() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(false);
        assertEquals("NON_PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testSetJMSDeliveryMode() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, "PERSISTENT");
        Mockito.verify(facade).setPersistent(true);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, "NON_PERSISTENT");
        Mockito.verify(facade).setPersistent(false);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, 2);
        Mockito.verify(facade).setPersistent(true);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, 1);
        Mockito.verify(facade).setPersistent(false);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, true);
        Mockito.verify(facade).setPersistent(true);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, false);
        Mockito.verify(facade).setPersistent(false);
        Mockito.reset(message, facade);
        Mockito.when(message.getFacade()).thenReturn(facade);
    }

    @Test
    public void testJMSDeliveryModeInGetPropertyNamesWhenPersistent() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(true);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testJMSDeliveryModeInGetPropertyNamesWhenNotPersistent() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(false);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testJMSDeliveryModeNotInGetPropertyNamesWhenExcludingStandardJMSHeaders() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(true);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testJMSDeliveryModePropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(true);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testJMSDeliveryModePropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isPersistent()).thenReturn(false);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testSetJMSDeliveryModeConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, "SOMETHING");
            fail("Should have thrown an exception for this call");
        } catch (NumberFormatException e) {
        }
    }

    @Test
    public void testJMSDeliveryModeClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setPersistent(true);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setPersistent(true);
    }

    //---------- JMSPriority ---------------------------------------------//

    @Test
    public void testJMSPriorityInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_PRIORITY));
    }

    @Test
    public void testGetJMSPriorityWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(4);
        assertEquals(4, JmsMessagePropertyIntercepter.getProperty(message, JMS_PRIORITY));
        Mockito.verify(facade).getPriority();
    }

    @Test
    public void testGetJMSPriorityWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(9);
        assertEquals(9, JmsMessagePropertyIntercepter.getProperty(message, JMS_PRIORITY));
    }

    @Test
    public void testSetJMSPriority() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_PRIORITY, 9);
        Mockito.verify(facade).setPriority((byte) 9);
    }

    @Test
    public void testJMSPriorityInGetPropertyNamesWhenDefault() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(Message.DEFAULT_PRIORITY);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_PRIORITY));
    }

    @Test
    public void testJMSPriorityInGetPropertyNamesWhenNotDefault() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(1);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_PRIORITY));
    }

    @Test
    public void testJMSPriorityNotInGetPropertyNamesWhenExcludingStandardJMSHeaders() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(1);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_PRIORITY));
    }

    @Test
    public void testJMSPriorityPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(1);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_PRIORITY));
    }

    @Test
    public void testJMSPriorityPropertExistsWhenSetToDefault() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getPriority()).thenReturn(4);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_PRIORITY));
    }

    @Test
    public void testSetJMSPriorityConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_PRIORITY, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSPriorityClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setPriority(4);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setPriority(4);
    }

    //---------- JMSMessageID ---------------------------------------------//

    @Test
    public void testJMSMessageIDInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_MESSAGEID));
    }

    @Test
    public void testGetJMSMessageIdWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_MESSAGEID));
        Mockito.verify(facade).getMessageId();
    }

    @Test
    public void testGetJMSMessageIdWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getMessageId()).thenReturn("MESSAGE_ID");
        assertEquals("MESSAGE_ID", JmsMessagePropertyIntercepter.getProperty(message, JMS_MESSAGEID));
    }

    @Test
    public void testSetJMSMessageId() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_MESSAGEID, "ID:123456-789");
        Mockito.verify(facade).setMessageId("ID:123456-789");
    }

    @Test
    public void testJMSMessageIDInGetPropertyNamesWhenSet() throws JMSException {
        doJMSMessageIDInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSMessageIDNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSMessageIDInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSMessageIDInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getMessageId()).thenReturn("MESSAGE_ID");
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_MESSAGEID));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_MESSAGEID));
        }
    }

    @Test
    public void testJMSMessageIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_MESSAGEID));
    }

    @Test
    public void testJMSMessageIDPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getMessageId()).thenReturn("MESSAGE_ID");
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_MESSAGEID));
    }

    @Test
    public void testJMSMessageIDPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getMessageId()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_MESSAGEID));
    }

    @Test
    public void testSetJMSMessageIDConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_MESSAGEID, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSMessageIDClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setMessageId(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setMessageId(null);
    }

    //---------- JMSTimestamp ---------------------------------------------//

    @Test
    public void testJMSTimestampInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_TIMESTAMP));
    }

    @Test
    public void testGetJMSTimeStampWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getTimestamp()).thenReturn(0L);
        assertEquals(Long.valueOf(0L), JmsMessagePropertyIntercepter.getProperty(message, JMS_TIMESTAMP));
        Mockito.verify(facade).getTimestamp();
    }

    @Test
    public void testGetJMSTimeStampWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getTimestamp()).thenReturn(900L);
        assertEquals(900L, JmsMessagePropertyIntercepter.getProperty(message, JMS_TIMESTAMP));
    }

    @Test
    public void testSetJMSTimeStamp() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_TIMESTAMP, 65536L);
        Mockito.verify(facade).setTimestamp(65536L);
    }

    @Test
    public void testJMSTimeStampInGetPropertyNamesWhenSet() throws JMSException {
        doJMSTimeStampInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSTimeStampNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSTimeStampInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSTimeStampInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getTimestamp()).thenReturn(900L);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_TIMESTAMP));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_TIMESTAMP));
        }
    }

    @Test
    public void testJMSTimeStampNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_TIMESTAMP));
    }

    @Test
    public void testJMSTimestampPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getTimestamp()).thenReturn(900L);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_TIMESTAMP));
    }

    @Test
    public void testJMSTimestampPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getTimestamp()).thenReturn(0L);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_TIMESTAMP));
    }

    @Test
    public void testSetJMSTimestampConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_TIMESTAMP, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSTimeStampClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setTimestamp(0);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setTimestamp(0);
    }

    //---------- JMSCorrelationID ---------------------------------------------//

    @Test
    public void testJMSCorrelationIDInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_CORRELATIONID));
    }

    @Test
    public void testGetJMSCorrelationIdWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_CORRELATIONID));
        Mockito.verify(facade).getCorrelationId();
    }

    @Test
    public void testGetJMSCorrelationIdWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getCorrelationId()).thenReturn("MESSAGE_ID");
        assertEquals("MESSAGE_ID", JmsMessagePropertyIntercepter.getProperty(message, JMS_CORRELATIONID));
    }

    @Test
    public void testSetJMSCorrelationId() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_CORRELATIONID, "ID:123456-789");
        Mockito.verify(facade).setCorrelationId("ID:123456-789");
    }

    @Test
    public void testJMSCorrelationIDInGetPropertyNamesWhenSet() throws JMSException {
        doJMSCorrelationIDInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSCorrelationIDNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSCorrelationIDInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSCorrelationIDInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getCorrelationId()).thenReturn("MESSAGE_ID");
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_CORRELATIONID));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_CORRELATIONID));
        }
    }

    @Test
    public void testJMSCorrelationIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_CORRELATIONID));
    }

    @Test
    public void testJMSCorrelationIDPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getCorrelationId()).thenReturn("MESSAGE_ID");
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_CORRELATIONID));
    }

    @Test
    public void testJMSCorrelationIDPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getCorrelationId()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_CORRELATIONID));
    }

    @Test
    public void testSetJMSCorrelationIDConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_CORRELATIONID, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSCorrelationIDClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setCorrelationId(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setCorrelationId(null);
    }

    //---------- JMSExpiration ---------------------------------------------//

    @Test
    public void testJMSExpirationInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_EXPIRATION));
    }

    @Test
    public void testGetJMSExpirationWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getExpiration()).thenReturn(0L);
        assertEquals(Long.valueOf(0L), JmsMessagePropertyIntercepter.getProperty(message, JMS_EXPIRATION));
        Mockito.verify(facade).getExpiration();
    }

    @Test
    public void testGetJMSExpirationWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getExpiration()).thenReturn(900L);
        assertEquals(900L, JmsMessagePropertyIntercepter.getProperty(message, JMS_EXPIRATION));
    }

    @Test
    public void testSetJMSExpiration() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_EXPIRATION, 65536L);
        Mockito.verify(facade).setExpiration(65536L);
    }

    @Test
    public void testJMSExpirationInGetPropertyNamesWhenSet() throws JMSException {
        doJMSExpirationInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSExpirationNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSExpirationInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSExpirationInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getExpiration()).thenReturn(900L);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_EXPIRATION));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_EXPIRATION));
        }
    }

    @Test
    public void testJMSExpirationNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_EXPIRATION));
    }

    @Test
    public void testJMSExpirationPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getExpiration()).thenReturn(900L);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_EXPIRATION));
    }

    @Test
    public void testJMSExpirationPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getExpiration()).thenReturn(0L);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_EXPIRATION));
    }

    @Test
    public void testSetJMSExpirationConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_EXPIRATION, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSExpirationClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setExpiration(0);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setExpiration(0);
    }

    //---------- JMSRedelivered ---------------------------------------------//

    @Test
    public void testJMSRedeliveredInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_REDELIVERED));
    }

    @Test
    public void testGetJMSRedeliveredWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse((Boolean) JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
        Mockito.verify(facade).isRedelivered();
    }

    @Test
    public void testGetJMSRedeliveredWhenSetTrue() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(true);
        assertEquals(true, JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
    }

    @Test
    public void testGetJMSRedeliveredWhenSetFalse() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(false);
        assertEquals(false, JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
    }

    @Test
    public void testSetJMSRedelivered() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REDELIVERED, true);
        Mockito.verify(facade).setRedelivered(true);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REDELIVERED, false);
        Mockito.verify(facade).setRedelivered(false);
    }

    @Test
    public void testJMSRedeliveredInGetPropertyNamesWhenSet() throws JMSException {
        doJMSRedeliveredInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSRedeliveredNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSRedeliveredInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSRedeliveredInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(true);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_REDELIVERED));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_REDELIVERED));
        }
    }

    @Test
    public void testJMSRedeliveredNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(false);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_REDELIVERED));
    }

    @Test
    public void testJMSRedeliveredPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(true);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_REDELIVERED));
    }

    @Test
    public void testJMSRedeliveredPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.isRedelivered()).thenReturn(false);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_REDELIVERED));
    }

    @Test
    public void testSetJMSRedeliveredConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_REDELIVERED, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSRedeliveredClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setRedelivered(false);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setRedelivered(false);
    }

    //---------- JMSXGroupID ---------------------------------------------//

    @Test
    public void testJMSXGroupIDInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMSX_GROUPID));
    }

    @Test
    public void testGetJMSXGroupIdWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPID));
        Mockito.verify(facade).getGroupId();
    }

    @Test
    public void testGetJMSXGroupIdWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupId()).thenReturn("GROUP_ID");
        assertEquals("GROUP_ID", JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPID));
    }

    @Test
    public void testSetJMSXGroupId() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPID, "MyGroupID");
        Mockito.verify(facade).setGroupId("MyGroupID");
    }

    @Test
    public void testJMSXGroupIDInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupId()).thenReturn("GROUP_ID");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_GROUPID));
        assertTrue(JMSX_GROUPID + " is not a header and should be included",
                  JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMSX_GROUPID));
    }

    @Test
    public void testJMSXGroupIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_GROUPID));
    }

    @Test
    public void testJMSXGroupIDPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupId()).thenReturn("GROUP_ID");
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_GROUPID));
    }

    @Test
    public void testJMSXGroupIDPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupId()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_GROUPID));
    }

    @Test
    public void testSetJMSXGroupIDConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPID, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSXGroupIDClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade).setGroupId(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade, Mockito.times(2)).setGroupId(null);
    }

    //---------- JMSXGroupSeq ---------------------------------------------//

    @Test
    public void testJMSXGroupSeqInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMSX_GROUPSEQ));
    }

    @Test
    public void testGetJMSXGroupSeqWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertEquals(Integer.valueOf(0), JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPSEQ));
        Mockito.verify(facade).getGroupSequence();
    }

    @Test
    public void testGetJMSXGroupSeqWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupSequence()).thenReturn(95);
        assertEquals(95, JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPSEQ));
    }

    @Test
    public void testSetJMSXGroupSeq() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPSEQ, 65536);
        Mockito.verify(facade).setGroupSequence(65536);
    }

    @Test
    public void testJMSXGroupSeqInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupSequence()).thenReturn(1);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_GROUPSEQ));
        assertTrue(JMSX_GROUPSEQ + " is not a header and should be included",
                   JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMSX_GROUPSEQ));
    }

    @Test
    public void testJMSXGroupSeqNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_GROUPSEQ));
    }

    @Test
    public void testJMSXGroupSeqPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupSequence()).thenReturn(5);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_GROUPSEQ));
    }

    @Test
    public void testJMSXGroupSeqPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getGroupSequence()).thenReturn(0);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_GROUPSEQ));
    }

    @Test
    public void testSetJMSXGroupSeqConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPSEQ, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSXGroupSeqClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade).setGroupSequence(0);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade, Mockito.times(2)).setGroupSequence(0);
    }

    //---------- JMSXDeliveryCount ---------------------------------------------//

    @Test
    public void testJMSXDeliveryCountInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testGetJMSXDeliveryCountWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertEquals(Integer.valueOf(0), JmsMessagePropertyIntercepter.getProperty(message, JMSX_DELIVERY_COUNT));
        Mockito.verify(facade).getDeliveryCount();
    }

    @Test
    public void testGetJMSXDeliverCountWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryCount()).thenReturn(2);
        assertEquals(2, JmsMessagePropertyIntercepter.getProperty(message, JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testSetJMSXDeliveryCount() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_DELIVERY_COUNT, 32768);
        Mockito.verify(facade).setDeliveryCount(32768);
    }

    @Test
    public void testJMSXDeliveryCountInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryCount()).thenReturn(2);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_DELIVERY_COUNT));
        assertTrue(JMSX_DELIVERY_COUNT + " is not a header and should be included",
                   JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testJMSXDeliveryCountInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testJMSXDeliveryCountPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryCount()).thenReturn(5);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testJMSXDeliveryCountPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryCount()).thenReturn(0);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testSetJMSXDeliverCountConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMSX_DELIVERY_COUNT, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSXDeliveryCountClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade).setDeliveryCount(1);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade, Mockito.times(2)).setDeliveryCount(1);
    }

    //---------- JMSXUserID ---------------------------------------------//

    @Test
    public void testJMSXUserIDInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMSX_USERID));
    }

    @Test
    public void testGetJMSXUserIdWhenNotSetLooksInMessageProperties() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMSX_USERID));
        Mockito.verify(facade).getUserId();
    }

    @Test
    public void testGetJMSXUserIdWhenNotSetHandlesMessagePropertyException() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getProperty("JMSXUserID")).thenThrow(new IllegalArgumentException());
        try {
            JmsMessagePropertyIntercepter.getProperty(message, JMSX_USERID);
            fail("Should have thrown a JMSException");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testGetJMSXUserIdWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getUserId()).thenReturn("Administrator");
        assertEquals("Administrator", JmsMessagePropertyIntercepter.getProperty(message, JMSX_USERID));
    }

    @Test
    public void testSetJMSXUserId() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_USERID, "Administrator");
        Mockito.verify(facade).setUserId("Administrator");
    }

    @Test
    public void testJMSXUserIdCountInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getUserId()).thenReturn("Administrator");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_USERID));
        assertTrue(JMSX_USERID + " is not a header and should be included",
                   JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMSX_USERID));
    }

    @Test
    public void testJMSXUserIdNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMSX_USERID));
    }

    @Test
    public void testJMSXUserIdPropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getUserId()).thenReturn("Administrator");
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_USERID));
    }

    @Test
    public void testJMSXUserIdPropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getUserId()).thenReturn(null);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMSX_USERID));
    }

    @Test
    public void testSetJMSXUserIdConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMSX_USERID, true);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSXUserIDClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade).setUserId(null);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade, Mockito.times(2)).setUserId(null);
    }

    //---------- Ack type property flag  ---------------------------------------//

    @Test
    public void testJMS_AMQP_ACK_TYPEInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testGetJMS_AMQP_ACK_TYPEWhenAckCallbackNotSet() throws JMSException {
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_ACK_TYPE));
        Mockito.verify(message).getAcknowledgeCallback();
    }

    @Test
    public void testGetJMS_AMQP_ACK_TYPEWhenAckCallbackSetButNotUpdated() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_ACK_TYPE));
        Mockito.verify(message, Mockito.atLeast(1)).getAcknowledgeCallback();
    }

    @Test
    public void testGetJMS_AMQP_ACK_TYPEWhenSet() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        callback.setAckType(RELEASED);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        assertEquals(RELEASED, JmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_ACK_TYPE));
        Mockito.verify(message, Mockito.atLeast(1)).getAcknowledgeCallback();
    }

    @Test
    public void testSetJMS_AMQP_ACK_TYPE() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_ACK_TYPE));
        JmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_ACK_TYPE, RELEASED);
        assertEquals(RELEASED, JmsMessagePropertyIntercepter.getProperty(message, JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testJMS_AMQP_ACK_TYPEInGetPropertyNamesWhenSet() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        callback.setAckType(RELEASED);
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);
        Mockito.when(message.getFacade()).thenReturn(facade);

        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testJMS_AMQP_ACK_TYPENotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);
        Mockito.when(message.getFacade()).thenReturn(facade);

        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testJMS_AMQP_ACK_TYPEPropertExistsWhenSet() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        callback.setAckType(RELEASED);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testJMS_AMQP_ACK_TYPEPropertExistsWhenNotSet() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testSetJMS_AMQP_ACK_TYPEConversionChecks() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);

        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_ACK_TYPE, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testSetJMS_AMQP_ACK_TYPEFailsIfNoAcknowledgeCallback() throws JMSException {
        JmsMessage message = Mockito.mock(JmsMapMessage.class);

        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_AMQP_ACK_TYPE, RELEASED);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMS_AMQP_ACK_TYPEClearedWhenRequested() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        callback.setAckType(RELEASED);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);
        Mockito.when(message.getFacade()).thenReturn(facade);

        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
    }

    @Test
    public void testJMS_AMQP_ACK_TYPEWhenMessagePropertiesAreReadOnly() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        callback.setAckType(RELEASED);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getAcknowledgeCallback()).thenReturn(callback);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.doThrow(MessageNotWriteableException.class).when(message).checkReadOnlyProperties();

        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_AMQP_ACK_TYPE));
    }

    //---------- JMSDeliveryTime ---------------------------------------------//

    @Test
    public void testJMSDeliveryTimeInGetAllPropertyNames() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames(message).contains(JMS_DELIVERYTIME));
    }

    @Test
    public void testGetJMSDeliveryWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryTime()).thenReturn(0L);
        assertEquals(Long.valueOf(0L), JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERYTIME));
        Mockito.verify(facade).getDeliveryTime();
    }

    @Test
    public void testGetJMSDeliveryTimeWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryTime()).thenReturn(900L);
        assertEquals(900L, JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERYTIME));
    }

    @Test
    public void testSetJMSDeliveryTime() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERYTIME, 65536L);
        Mockito.verify(facade).setDeliveryTime(65536L, true);
    }

    @Test
    public void testJMSDeliveryTimeInGetPropertyNamesWhenSet() throws JMSException {
        doJMSDeliveryTimeInGetPropertyNamesWhenSetTestImpl(false);
    }

    @Test
    public void testJMSDeliveryTimeNotInGetPropertyNamesWhenSetAndExcludingStandardJMSHeaders() throws JMSException {
        doJMSDeliveryTimeInGetPropertyNamesWhenSetTestImpl(true);
    }

    private void doJMSDeliveryTimeInGetPropertyNamesWhenSetTestImpl(boolean excludeStandardJmsHeaders) throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryTime()).thenReturn(900L);
        if (excludeStandardJmsHeaders) {
            assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, true).contains(JMS_DELIVERYTIME));
        } else {
            assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DELIVERYTIME));
        }
    }

    @Test
    public void testJMSDeliveryTimeNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message, false).contains(JMS_DELIVERYTIME));
    }

    @Test
    public void testJMSDeliveryTimePropertExistsWhenSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryTime()).thenReturn(900L);
        assertTrue(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DELIVERYTIME));
    }

    @Test
    public void testJMSDeliveryTimePropertExistsWhenNotSet() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        Mockito.when(facade.getDeliveryTime()).thenReturn(0L);
        assertFalse(JmsMessagePropertyIntercepter.propertyExists(message, JMS_DELIVERYTIME));
    }

    @Test
    public void testSetJMSDeliveryTimeConversionChecks() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        try {
            JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERYTIME, new byte[1]);
            fail("Should have thrown an exception for this call");
        } catch (JMSException e) {
        }
    }

    @Test
    public void testJMSDeliveryTimeClearedWhenRequested() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage message = Mockito.mock(JmsMapMessage.class);
        Mockito.when(message.getFacade()).thenReturn(facade);
        JmsMessagePropertyIntercepter.clearProperties(message, true);
        Mockito.verify(facade, Mockito.never()).setDeliveryTime(0, true);
        JmsMessagePropertyIntercepter.clearProperties(message, false);
        Mockito.verify(facade).setDeliveryTime(0, true);
    }
}
