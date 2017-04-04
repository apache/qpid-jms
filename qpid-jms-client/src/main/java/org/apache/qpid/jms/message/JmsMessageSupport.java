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

import javax.jms.JMSException;

import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

/**
 * Set of common utilities and definitions useful for JMS Message handling.
 */
public class JmsMessageSupport {

    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_REPLYTO = "JMSReplyTo";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_MESSAGEID = "JMSMessageID";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_CORRELATIONID = "JMSCorrelationID";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_DELIVERYTIME = "JMSDeliveryTime";

    public static final String JMSX_GROUPID = "JMSXGroupID";
    public static final String JMSX_GROUPSEQ = "JMSXGroupSeq";
    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    public static final String JMSX_USERID = "JMSXUserID";

    public static final String JMS_AMQP_ACK_TYPE = "JMS_AMQP_ACK_TYPE";

    // TODO: advise not using these constants, since doing so wont be portable?
    // Make them package private so they can't be used to begin with?
    public static final int ACCEPTED = 1;
    public static final int REJECTED = 2;
    public static final int RELEASED = 3;
    public static final int MODIFIED_FAILED = 4;
    public static final int MODIFIED_FAILED_UNDELIVERABLE = 5;

    public static ACK_TYPE lookupAckTypeForDisposition(int dispositionType) throws JMSException {
        switch (dispositionType) {
            case JmsMessageSupport.ACCEPTED:
                return ACK_TYPE.ACCEPTED;
            case JmsMessageSupport.REJECTED:
                return ACK_TYPE.REJECTED;
            case JmsMessageSupport.RELEASED:
                return ACK_TYPE.RELEASED;
            case JmsMessageSupport.MODIFIED_FAILED:
                return ACK_TYPE.MODIFIED_FAILED;
            case JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE:
                return ACK_TYPE.MODIFIED_FAILED_UNDELIVERABLE;
            default:
                throw new JMSException("Unable to determine ack type for disposition: " + dispositionType);
        }
    }
}
