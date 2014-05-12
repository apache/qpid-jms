/*
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
 */
package org.apache.qpid.jms.engine;

import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.jms.impl.ObjectMessageImpl;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMessageFactory
{
    AmqpMessage createAmqpMessage(Delivery delivery, Message message, AmqpConnection amqpConnection)
    {
        Section body = message.getBody();

        //TODO: This is a temporary hack, need to implement proper support for the new
        //message type annotation by generally rewriting the entire factory method
        AmqpGenericMessage msg = new AmqpGenericMessage(delivery, message, amqpConnection);
        if(msg.messageAnnotationExists(ClientProperties.X_OPT_JMS_MSG_TYPE))
        {
            UnsignedByte ub = (UnsignedByte) msg.getMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE);
            if(ub.shortValue() == ObjectMessageImpl.X_OPT_JMS_MSG_TYPE_VALUE)
            {
                boolean isJavaSerialized = isContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE, message);
                return new AmqpObjectMessage(delivery, message, amqpConnection, !isJavaSerialized);
            }
        }

        if(body == null)
        {
            if(isContentType(AmqpTextMessage.CONTENT_TYPE, message))
            {
                return new AmqpTextMessage(message, delivery, amqpConnection);
            }
            else if(isContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE, message))
            {
                return new AmqpObjectMessage(delivery, message, amqpConnection, false);
            }
            else if(isContentType(AmqpBytesMessage.CONTENT_TYPE, message) || isContentType(null, message))
            {
                return new AmqpBytesMessage(message, delivery, amqpConnection);
            }
        }
        else if(body instanceof Data)
        {
            if(isContentType(AmqpTextMessage.CONTENT_TYPE, message))
            {
                return new AmqpTextMessage(message, delivery, amqpConnection);
            }
            else if(isContentType(AmqpBytesMessage.CONTENT_TYPE, message)  || isContentType(null, message))
            {
                return new AmqpBytesMessage(message, delivery, amqpConnection);
            }
            else if(isContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE, message))
            {
                return new AmqpObjectMessage(delivery, message, amqpConnection, false);
            }
        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null || value instanceof String)
            {
                return new AmqpTextMessage(message, delivery, amqpConnection);
            }
            else if(value instanceof Map)
            {
                return new AmqpMapMessage(message, delivery, amqpConnection);
            }
            else if(value instanceof List)
            {
                return new AmqpListMessage(delivery, message, amqpConnection);
            }
            else if(value instanceof Binary)
            {
                return new AmqpBytesMessage(message, delivery, amqpConnection);
            }
        }
        else
        {
            //TODO: AmqpSequence support
        }

        //Unable to determine a specific message type, return the generic message
        return new AmqpGenericMessage(delivery, message, amqpConnection);
    }

    private boolean isContentType(String contentType, Message message)
    {
        if(contentType == null)
        {
            return message.getContentType() == null;
        }
        else
        {
            return contentType.equals(message.getContentType());
        }
    }
}
