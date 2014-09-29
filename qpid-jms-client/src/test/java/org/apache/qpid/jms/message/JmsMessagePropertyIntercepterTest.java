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

    //---------- JMSPriority ---------------------------------------------//

    @Test
    public void testJMSPriorityInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_PRIORITY));
    }

    @Test
    public void testGetJMSPriorityWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(message.getPriority()).thenReturn((byte) 4);
        assertEquals(4, JmsMessagePropertyIntercepter.getProperty(message, JMS_PRIORITY));
        Mockito.verify(message).getPriority();
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

    //---------- JMSXDeliveryCount ---------------------------------------------//

    @Test
    public void testJMSXDeliveryCountInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_DELIVERY_COUNT));
    }

    @Test
    public void testGetJMSXDeliveryCountWhenNotSet() throws JMSException {
        JmsMessageFacade message = Mockito.mock(JmsMessageFacade.class);
        assertEquals(Integer.valueOf(1), JmsMessagePropertyIntercepter.getProperty(message, JMSX_DELIVERY_COUNT));
        Mockito.verify(message).getRedeliveryCounter();
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

}
