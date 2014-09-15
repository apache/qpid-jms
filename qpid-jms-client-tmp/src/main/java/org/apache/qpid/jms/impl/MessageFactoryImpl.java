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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.engine.AmqpBytesMessage;
import org.apache.qpid.jms.engine.AmqpGenericMessage;
import org.apache.qpid.jms.engine.AmqpListMessage;
import org.apache.qpid.jms.engine.AmqpMapMessage;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.AmqpObjectMessage;
import org.apache.qpid.jms.engine.AmqpTextMessage;

public class MessageFactoryImpl
{
    Message createJmsMessage(AmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        if(amqpMessage instanceof AmqpTextMessage)
        {
            return new TextMessageImpl((AmqpTextMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else if(amqpMessage instanceof AmqpBytesMessage)
        {
            return new BytesMessageImpl((AmqpBytesMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else if(amqpMessage instanceof AmqpObjectMessage)
        {
            return new ObjectMessageImpl((AmqpObjectMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else if(amqpMessage instanceof AmqpListMessage)
        {
            return new StreamMessageImpl((AmqpListMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else if(amqpMessage instanceof AmqpMapMessage)
        {
            return new MapMessageImpl((AmqpMapMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else if(amqpMessage instanceof AmqpGenericMessage)
        {
            return new GenericAmqpMessageImpl((AmqpGenericMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
        }
        else
        {
            //TODO: support other message types
            throw new QpidJmsException("Unknown Message Type");
        }
    }
}
