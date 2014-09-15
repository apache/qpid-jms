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

import java.util.Map;

import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMessageFactory
{
    AmqpMessage createAmqpMessage(Delivery delivery, Message message, AmqpConnection amqpConnection)
    {
        Section body = message.getBody();

        Byte msgType = getMessageTypeAnnotation(message);
        if(msgType != null)
        {
            switch(msgType)
            {
                case ClientProperties.GENERIC_MESSAGE_TYPE:
                    return new AmqpGenericMessage(delivery, message, amqpConnection);
                case ClientProperties.OBJECT_MESSSAGE_TYPE:
                    boolean isJavaSerialized = isContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE, message);
                    return new AmqpObjectMessage(message, delivery, amqpConnection, !isJavaSerialized);
                case ClientProperties.MAP_MESSAGE_TYPE:
                    return new AmqpMapMessage(message, delivery, amqpConnection);
                case ClientProperties.TEXT_MESSAGE_TYPE:
                    return new AmqpTextMessage(message, delivery, amqpConnection);
                case ClientProperties.BYTES_MESSAGE_TYPE:
                    return new AmqpBytesMessage(message, delivery, amqpConnection);
                case ClientProperties.STREAM_MESSAGE_TYPE:
                    return new AmqpListMessage(message, delivery, amqpConnection);
            }
        }
        else if(body == null)
        {
            //TODO: accept textual content types other than strictly "text/plain"
            if(isContentType(AmqpTextMessage.CONTENT_TYPE, message))
            {
                return new AmqpTextMessage(message, delivery, amqpConnection);
            }
            else if(isContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE, message))
            {
                return new AmqpObjectMessage(message, delivery, amqpConnection, false);
            }
            else if(isContentType(AmqpBytesMessage.CONTENT_TYPE, message) || isContentType(null, message))
            {
                return new AmqpBytesMessage(message, delivery, amqpConnection);
            }
        }
        else if(body instanceof Data)
        {
            //TODO: accept textual content types other than strictly "text/plain"
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
                return new AmqpObjectMessage(message, delivery, amqpConnection, false);
            }

            //TODO: should this situation throw an exception, or just become a bytes message?
            //Content type is set, but not to something we understand. Falling through to create a generic message.
        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null || value instanceof String)
            {
                return new AmqpTextMessage(message, delivery, amqpConnection);
            }
            else if(value instanceof Binary)
            {
                return new AmqpBytesMessage(message, delivery, amqpConnection);
            }
            else
            {
                return new AmqpObjectMessage(message, delivery, amqpConnection, true);
            }
        }
        else
        {
            //TODO: AmqpSequence support
        }

        //TODO: Unable to determine a specific message type, return the generic message
        return new AmqpGenericMessage(delivery, message, amqpConnection);
    }

    private static boolean isContentType(String contentType, Message message)
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

    private static Byte getMessageTypeAnnotation(Message msg)
    {
        MessageAnnotations messageAnnotations = msg.getMessageAnnotations();
        if(messageAnnotations != null)
        {
            Map<?,?> messageAnnotationsMap = messageAnnotations.getValue();
            if(messageAnnotationsMap != null)
            {
                Symbol annotationKey = Symbol.valueOf(ClientProperties.X_OPT_JMS_MSG_TYPE);

                return (Byte) messageAnnotationsMap.get(annotationKey);
            }
        }

        return null;
    }
}
