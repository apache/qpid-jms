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
package org.apache.qpid.jms.message;

import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_DELIVERY_COUNT;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPSEQ;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_USERID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_CORRELATIONID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DELIVERY_MODE;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DESTINATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_EXPIRATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_MESSAGEID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_PRIORITY;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REDELIVERED;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REPLYTO;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TIMESTAMP;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.junit.Test;
import org.mockito.Mockito;

public class JmsMessagePropertyIntercepterTest {

    //---------- JMSReplyTo --------------------------------------------------//

    @Test
    public void testJMSDestinationInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_DESTINATION));
    }

    @Test
    public void testGetJMSDestinationWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_DESTINATION));
        Mockito.verify(message).getDestination();
    }

    @Test
    public void testGetJMSDestinationWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsDestination destination = new JmsQueue("TestDestination");
        Mockito.when(message.getDestination()).thenReturn(destination);
        assertEquals(destination.getName(), JmsMessagePropertyIntercepter.getProperty(message, JMS_DESTINATION));
    }

    @Test
    public void testSetJMSDestination() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        String destinationName = new String("TestDestination");
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DESTINATION, destinationName);
        Mockito.verify(message).setDestinationFromString(destinationName);
    }

    @Test
    public void testJMSDestinationInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(message.getDestination()).thenReturn(queue);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_DESTINATION));
    }

    @Test
    public void testJMSDestinationNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_DESTINATION));
    }

    //---------- JMSReplyTo --------------------------------------------------//

    @Test
    public void testJMSReplyToInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_REPLYTO));
    }

    @Test
    public void testGetJMSReplyToWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_REPLYTO));
        Mockito.verify(message).getReplyTo();
    }

    @Test
    public void testGetJMSReplyToWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsDestination destination = new JmsQueue("TestDestination");
        Mockito.when(message.getReplyTo()).thenReturn(destination);
        assertEquals(destination.getName(), JmsMessagePropertyIntercepter.getProperty(message, JMS_REPLYTO));
    }

    @Test
    public void testSetJMSReplyTo() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        String destinationName = new String("TestDestination");
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REPLYTO, destinationName);
        Mockito.verify(message).setReplyToFromString(destinationName);
    }

    @Test
    public void testJMSReplyToInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsDestination queue = new JmsQueue("TestDestination");
        Mockito.when(message.getReplyTo()).thenReturn(queue);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_REPLYTO));
    }

    @Test
    public void testJMSReplyToNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_REPLYTO));
    }

    //---------- JMSType -----------------------------------------------------//

    @Test
    public void testJMSTypeInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_TYPE));
    }

    @Test
    public void testGetJMSTypeWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_TYPE));
        Mockito.verify(message).getType();
    }

    @Test
    public void testGetJMSTypeWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getType()).thenReturn("MsgType");
        assertEquals("MsgType", JmsMessagePropertyIntercepter.getProperty(message, JMS_TYPE));
    }

    @Test
    public void testSetJMSType() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_TYPE, "SomeType");
        Mockito.verify(message).setType("SomeType");
    }

    @Test
    public void testJMSTypeInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getType()).thenReturn("SomeType");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_TYPE));
    }

    @Test
    public void testJMSTypeNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_TYPE));
    }

    //---------- JMSDeliveryMode ---------------------------------------------//

    @Test
    public void testJMSDeliveryModeInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testGetJMSDeliveryModeWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertEquals("NON_PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
        Mockito.verify(message).isPersistent();
    }

    @Test
    public void testGetJMSDeliverModeSetDurable() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isPersistent()).thenReturn(true);
        assertEquals("PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testGetJMSDeliverModeSetNonDurable() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isPersistent()).thenReturn(false);
        assertEquals("NON_PERSISTENT", JmsMessagePropertyIntercepter.getProperty(message, JMS_DELIVERY_MODE));
    }

    @Test
    public void testSetJMSDeliveryMode() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, "PERSISTENT");
        Mockito.verify(message).setPersistent(true);
        Mockito.reset(message);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, "NON_PERSISTENT");
        Mockito.verify(message).setPersistent(false);
        Mockito.reset(message);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, 2);
        Mockito.verify(message).setPersistent(true);
        Mockito.reset(message);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, 1);
        Mockito.verify(message).setPersistent(false);
        Mockito.reset(message);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, true);
        Mockito.verify(message).setPersistent(true);
        Mockito.reset(message);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_DELIVERY_MODE, false);
        Mockito.verify(message).setPersistent(false);
        Mockito.reset(message);
    }

    @Test
    public void testJMSDeliveryModeInGetPropertyNamesWhenPersistent() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isPersistent()).thenReturn(true);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_DELIVERY_MODE));
    }

    @Test
    public void testJMSDeliveryModeInGetPropertyNamesWhenNotPersistent() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isPersistent()).thenReturn(false);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_DELIVERY_MODE));
    }

    //---------- JMSPriority ---------------------------------------------//

    @Test
    public void testJMSPriorityInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_PRIORITY));
    }

    @Test
    public void testGetJMSPriorityWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getPriority()).thenReturn(4);
        assertEquals(4, JmsMessagePropertyIntercepter.getProperty(message, JMS_PRIORITY));
        Mockito.verify(message).getPriority();
    }

    @Test
    public void testGetJMSPriorityWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getPriority()).thenReturn(9);
        assertEquals(9, JmsMessagePropertyIntercepter.getProperty(message, JMS_PRIORITY));
    }

    @Test
    public void testSetJMSPriority() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_PRIORITY, 9);
        Mockito.verify(message).setPriority((byte) 9);
    }

    @Test
    public void testJMSPriorityInGetPropertyNamesWhenDefault() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getPriority()).thenReturn(Message.DEFAULT_PRIORITY);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_PRIORITY));
    }

    @Test
    public void testJMSPriorityInGetPropertyNamesWhenNotDefault() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getPriority()).thenReturn(1);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_PRIORITY));
    }

    //---------- JMSMessageID ---------------------------------------------//

    @Test
    public void testJMSMessageIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_MESSAGEID));
    }

    @Test
    public void testGetJMSMessageIdWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_MESSAGEID));
        Mockito.verify(message).getMessageId();
    }

    @Test
    public void testGetJMSMessageIdWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getMessageId()).thenReturn("MESSAGE_ID");
        assertEquals("MESSAGE_ID", JmsMessagePropertyIntercepter.getProperty(message, JMS_MESSAGEID));
    }

    @Test
    public void testSetJMSMessageId() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_MESSAGEID, "ID:123456-789");
        Mockito.verify(message).setMessageId("ID:123456-789");
    }

    @Test
    public void testJMSMessageIDInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getMessageId()).thenReturn("MESSAGE_ID");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_MESSAGEID));
    }

    @Test
    public void testJMSMessageIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_MESSAGEID));
    }

    //---------- JMSTimestamp ---------------------------------------------//

    @Test
    public void testJMSTimestampInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_TIMESTAMP));
    }

    @Test
    public void testGetJMSTimeStampWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getTimestamp()).thenReturn(0L);
        assertEquals(Long.valueOf(0L), JmsMessagePropertyIntercepter.getProperty(message, JMS_TIMESTAMP));
        Mockito.verify(message).getTimestamp();
    }

    @Test
    public void testGetJMSTimeStampWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getTimestamp()).thenReturn(900L);
        assertEquals(900L, JmsMessagePropertyIntercepter.getProperty(message, JMS_TIMESTAMP));
    }

    @Test
    public void testSetJMSTimeStamp() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_TIMESTAMP, 65536L);
        Mockito.verify(message).setTimestamp(65536L);
    }

    @Test
    public void testJMSTimeStampInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getTimestamp()).thenReturn(900L);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_TIMESTAMP));
    }

    @Test
    public void testJMSTimeStampNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_TIMESTAMP));
    }

    //---------- JMSCorrelationID ---------------------------------------------//

    @Test
    public void testJMSCorrelationIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_CORRELATIONID));
    }

    @Test
    public void testGetJMSCorrelationIdWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMS_CORRELATIONID));
        Mockito.verify(message).getCorrelationId();
    }

    @Test
    public void testGetJMSCorrelationIdWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getCorrelationId()).thenReturn("MESSAGE_ID");
        assertEquals("MESSAGE_ID", JmsMessagePropertyIntercepter.getProperty(message, JMS_CORRELATIONID));
    }

    @Test
    public void testSetJMSCorrelationId() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_CORRELATIONID, "ID:123456-789");
        Mockito.verify(message).setCorrelationId("ID:123456-789");
    }

    @Test
    public void testJMSCorrelationIDInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getCorrelationId()).thenReturn("MESSAGE_ID");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_CORRELATIONID));
    }

    @Test
    public void testJMSCorrelationIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_CORRELATIONID));
    }

    //---------- JMSExpiration ---------------------------------------------//

    @Test
    public void testJMSExpirationInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_EXPIRATION));
    }

    @Test
    public void testGetJMSExpirationWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getExpiration()).thenReturn(0L);
        assertEquals(Long.valueOf(0L), JmsMessagePropertyIntercepter.getProperty(message, JMS_EXPIRATION));
        Mockito.verify(message).getExpiration();
    }

    @Test
    public void testGetJMSExpirationWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getExpiration()).thenReturn(900L);
        assertEquals(900L, JmsMessagePropertyIntercepter.getProperty(message, JMS_EXPIRATION));
    }

    @Test
    public void testSetJMSExpiration() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_EXPIRATION, 65536L);
        Mockito.verify(message).setExpiration(65536L);
    }

    @Test
    public void testJMSExpirationInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getExpiration()).thenReturn(900L);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_EXPIRATION));
    }

    @Test
    public void testJMSExpirationNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_EXPIRATION));
    }

    //---------- JMSRedelivered ---------------------------------------------//

    @Test
    public void testJMSRedeliveredInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_REDELIVERED));
    }

    @Test
    public void testGetJMSRedeliveredWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse((Boolean) JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
        Mockito.verify(message).isRedelivered();
    }

    @Test
    public void testGetJMSRedeliveredWhenSetTrue() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isRedelivered()).thenReturn(true);
        assertEquals(true, JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
    }

    @Test
    public void testGetJMSRedeliveredWhenSetFalse() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isRedelivered()).thenReturn(false);
        assertEquals(false, JmsMessagePropertyIntercepter.getProperty(message, JMS_REDELIVERED));
    }

    @Test
    public void testSetJMSRedelivered() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REDELIVERED, true);
        Mockito.verify(message).setRedelivered(true);
        JmsMessagePropertyIntercepter.setProperty(message, JMS_REDELIVERED, false);
        Mockito.verify(message).setRedelivered(false);
    }

    @Test
    public void testJMSRedeliveredInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isRedelivered()).thenReturn(true);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_REDELIVERED));
    }

    @Test
    public void testJMSRedeliveredNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.isRedelivered()).thenReturn(false);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMS_REDELIVERED));
    }

    //---------- JMSXGroupID ---------------------------------------------//

    @Test
    public void testJMSXGroupIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_GROUPID));
    }

    @Test
    public void testGetJMSXGroupIdWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPID));
        Mockito.verify(message).getGroupId();
    }

    @Test
    public void testGetJMSXGroupIdWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getGroupId()).thenReturn("GROUP_ID");
        assertEquals("GROUP_ID", JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPID));
    }

    @Test
    public void testSetJMSXGroupId() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPID, "MyGroupID");
        Mockito.verify(message).setGroupId("MyGroupID");
    }

    @Test
    public void testJMSXGroupIDInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getGroupId()).thenReturn("GROUP_ID");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_GROUPID));
    }

    @Test
    public void testJMSXGroupIDNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_GROUPID));
    }

    //---------- JMSXGroupSeq ---------------------------------------------//

    @Test
    public void testJMSXGroupSeqInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_GROUPSEQ));
    }

    @Test
    public void testGetJMSXGroupSeqWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertEquals(Integer.valueOf(0), JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPSEQ));
        Mockito.verify(message).getGroupSequence();
    }

    @Test
    public void testGetJMSXGroupSeqWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getGroupSequence()).thenReturn(95);
        assertEquals(95, JmsMessagePropertyIntercepter.getProperty(message, JMSX_GROUPSEQ));
    }

    @Test
    public void testSetJMSXGroupSeq() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_GROUPSEQ, 65536);
        Mockito.verify(message).setGroupSequence(65536);
    }

    @Test
    public void testJMSXGroupSeqInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getGroupId()).thenReturn("GROUP_ID");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_GROUPSEQ));
    }

    @Test
    public void testJMSXGroupSeqNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_GROUPSEQ));
    }

    //---------- JMSXDeliveryCount ---------------------------------------------//

    @Test
    public void testJMSXDeliveryCountInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testGetJMSXDeliveryCountWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertEquals(Integer.valueOf(0), JmsMessagePropertyIntercepter.getProperty(message, JMSX_DELIVERY_COUNT));
        Mockito.verify(message).getDeliveryCount();
    }

    @Test
    public void testGetJMSXDeliverCountWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getDeliveryCount()).thenReturn(2);
        assertEquals(2, JmsMessagePropertyIntercepter.getProperty(message, JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testSetJMSXDeliveryCount() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_DELIVERY_COUNT, 32768);
        Mockito.verify(message).setDeliveryCount(32768);
    }

    @Test
    public void testJMSXDeliveryCountInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getDeliveryCount()).thenReturn(2);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testJMSXDeliveryCountInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_DELIVERY_COUNT));
    }

    //---------- JMSXUserID ---------------------------------------------//

    @Test
    public void testJMSXUserIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_USERID));
    }

    @Test
    public void testGetJMSXUserIdWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertNull(JmsMessagePropertyIntercepter.getProperty(message, JMSX_USERID));
        Mockito.verify(message).getUserId();
    }

    @Test
    public void testGetJMSXUserIdWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getUserId()).thenReturn("Administrator");
        assertEquals("Administrator", JmsMessagePropertyIntercepter.getProperty(message, JMSX_USERID));
    }

    @Test
    public void testSetJMSXUserId() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        JmsMessagePropertyIntercepter.setProperty(message, JMSX_USERID, "Administrator");
        Mockito.verify(message).setUserId("Administrator");
    }

    @Test
    public void testJMSXUserIdCountInGetPropertyNamesWhenSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getUserId()).thenReturn("Administrator");
        assertTrue(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_USERID));
    }

    @Test
    public void testJMSXUserIdNotInGetPropertyNamesWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertFalse(JmsMessagePropertyIntercepter.getPropertyNames(message).contains(JMSX_USERID));
    }
}
