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
package org.apache.qpid.jms.impl;

public class ClientProperties
{
    //JMS-defined Property Names
    public static final String JMSXUSERID = "JMSXUserID";
    public static final String JMSXGROUPID = "JMSXGroupID";
    public static final String JMSXGROUPSEQ = "JMSXGroupSeq";

    //Custom Message Property Names
    public static final String JMS_AMQP_TTL = "JMS_AMQP_TTL";
    public static final String JMS_AMQP_REPLY_TO_GROUP_ID = "JMS_AMQP_REPLY_TO_GROUP_ID";
    public static final String JMS_AMQP_TYPED_ENCODING = "JMS_AMQP_TYPED_ENCODING";

    //Message Annotation Names
    public static final String X_OPT_JMS_MSG_TYPE = "x-opt-jms-msg-type";
    public static final String X_OPT_APP_CORRELATION_ID = "x-opt-app-correlation-id";
    public static final String X_OPT_JMS_TYPE = "x-opt-jms-type";

    //Message Annotation Values
    //X_OPT_JMS_MSG_TYPE
    public static final byte GENERIC_MESSAGE_TYPE = 0;
    public static final byte OBJECT_MESSSAGE_TYPE = 1;
    public static final byte MAP_MESSAGE_TYPE = 2;
    public static final byte BYTES_MESSAGE_TYPE = 3;
    public static final byte STREAM_MESSAGE_TYPE = 4;
    public static final byte TEXT_MESSAGE_TYPE = 5;

    //Client configuration System Property names
    public static final String QPID_SET_JMSXUSERID_ON_SEND = "qpid.set-jmsxuserid-on-send";
    public static final String QPID_DEFAULT_CONSUMER_PREFETCH = "qpid.default-consumer-prefetch";
}
