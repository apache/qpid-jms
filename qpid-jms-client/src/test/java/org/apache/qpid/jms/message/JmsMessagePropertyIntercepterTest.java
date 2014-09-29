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
import static org.junit.Assert.assertTrue;

import javax.jms.JMSException;

import org.junit.Test;

public class JmsMessagePropertyIntercepterTest {

    //---------- JMSReplyTo --------------------------------------------------//

    @Test
    public void testJMSDestinationInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_DESTINATION));
    }

    //---------- JMSReplyTo --------------------------------------------------//

    @Test
    public void testJMSReplyToInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_REPLYTO));
    }

    //---------- JMSType -----------------------------------------------------//

    @Test
    public void testJMSTypeInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_TYPE));
    }

    //---------- JMSDeliveryMode ---------------------------------------------//

    @Test
    public void testJMSDeliveryModeInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_DELIVERY_MODE));
    }

    //---------- JMSPriority ---------------------------------------------//

    @Test
    public void testJMSPriorityInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_PRIORITY));
    }

    //---------- JMSMessageID ---------------------------------------------//

    @Test
    public void testJMSMessageIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_MESSAGEID));
    }

    //---------- JMSTimestamp ---------------------------------------------//

    @Test
    public void testJMSTimestampInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_TIMESTAMP));
    }

    //---------- JMSCorrelationID ---------------------------------------------//

    @Test
    public void testJMSCorrelationIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_CORRELATIONID));
    }

    //---------- JMSExpiration ---------------------------------------------//

    @Test
    public void testJMSExpirationInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_EXPIRATION));
    }

    //---------- JMSRedelivered ---------------------------------------------//

    @Test
    public void testJMSRedeliveredInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMS_REDELIVERED));
    }

    //---------- JMSXGroupID ---------------------------------------------//

    @Test
    public void testJMSXGroupIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_GROUPID));
    }

    //---------- JMSXGroupSeq ---------------------------------------------//

    @Test
    public void testJMSXGroupSeqInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_GROUPSEQ));
    }

    //---------- JMSXDeliveryCount ---------------------------------------------//

    @Test
    public void testJMSXDeliveryCountInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_DELIVERY_COUNT));
    }

    //---------- JMSXUserID ---------------------------------------------//

    @Test
    public void testJMSXUserIDInGetAllPropertyNames() throws JMSException {
        assertTrue(JmsMessagePropertyIntercepter.getAllPropertyNames().contains(JMSX_USERID));
    }

}
