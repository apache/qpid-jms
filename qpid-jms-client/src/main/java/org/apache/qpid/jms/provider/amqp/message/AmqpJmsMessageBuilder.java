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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.isContentType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMapMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.util.ContentTypeSupport;
import org.apache.qpid.jms.util.InvalidContentTypeException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * Builder class used to construct the appropriate JmsMessage / JmsMessageFacade
 * objects to wrap an incoming AMQP Message.
 */
public class AmqpJmsMessageBuilder {

    /**
     * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
     * message type for the incoming AMQP message.
     *
     * @param consumer
     *        The provider AMQP Consumer instance where this message arrived at.
     * @param message
     *        The Proton Message object that will be wrapped.
     *
     * @return a JmsMessage instance properly configured for dispatch to the provider listener.
     *
     * @throws IOException if an error occurs while creating the message objects.
     */
    public static JmsMessage createJmsMessage(AmqpConsumer consumer, Message message) throws IOException {

        // First we try the easy way, if the annotation is there we don't have to work hard.
        JmsMessage result = createFromMsgAnnotation(consumer, message);
        if (result != null) {
            return result;
        }

        // Next, match specific section structures and content types
        result = createWithoutAnnotation(consumer, message);
        if (result != null) {
            return result;
        }

        // TODO
        throw new IOException("Could not create a JMS message from incoming message");
    }

    private static JmsMessage createFromMsgAnnotation(AmqpConsumer consumer, Message message) throws IOException {
        Object annotation = AmqpMessageSupport.getMessageAnnotation(JMS_MSG_TYPE, message);
        if (annotation != null) {

            switch ((byte) annotation) {
                case JMS_MESSAGE:
                    return createMessage(consumer, message);
                case JMS_BYTES_MESSAGE:
                    return createBytesMessage(consumer, message);
                case JMS_TEXT_MESSAGE:
                    return createTextMessage(consumer, message, StandardCharsets.UTF_8);
                case JMS_MAP_MESSAGE:
                    return createMapMessage(consumer, message);
                case JMS_STREAM_MESSAGE:
                    return createStreamMessage(consumer, message);
                case JMS_OBJECT_MESSAGE:
                    return createObjectMessage(consumer, message);
                default:
                    throw new IOException("Invalid JMS Message Type annotation value found in message: " + annotation);
            }
        }

        return null;
    }

    private static JmsMessage createWithoutAnnotation(AmqpConsumer consumer, Message message) {
        Section body = message.getBody();

        if (body == null) {
            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                return createObjectMessage(consumer, message);
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message) || isContentType(null, message)) {
                return createBytesMessage(consumer, message);
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (charset != null) {
                    return createTextMessage(consumer, message, charset);
                } else {
                    return createMessage(consumer, message);
                }
            }
        } else if (body instanceof Data) {
            if (isContentType(OCTET_STREAM_CONTENT_TYPE, message) || isContentType(null, message)) {
                return createBytesMessage(consumer, message);
            } else if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                return createObjectMessage(consumer, message);
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (charset != null) {
                    return createTextMessage(consumer, message, charset);
                } else {
                    return createBytesMessage(consumer, message);
                }
            }
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();

            if (value == null || value instanceof String) {
                return createTextMessage(consumer, message, StandardCharsets.UTF_8);
            } else if (value instanceof Binary) {
                return createBytesMessage(consumer, message);
            } else {
                return createObjectMessage(consumer, message);
            }
        } else if (body instanceof AmqpSequence) {
            return createObjectMessage(consumer, message);
        }

        return null;
    }

    private static JmsObjectMessage createObjectMessage(AmqpConsumer consumer, Message message) {
        return new JmsObjectMessage(new AmqpJmsObjectMessageFacade(consumer, message));
    }

    private static JmsStreamMessage createStreamMessage(AmqpConsumer consumer, Message message) {
        return new JmsStreamMessage(new AmqpJmsStreamMessageFacade(consumer, message));
    }

    private static JmsMapMessage createMapMessage(AmqpConsumer consumer, Message message) {
        return new JmsMapMessage(new AmqpJmsMapMessageFacade(consumer, message));
    }

    private static JmsTextMessage createTextMessage(AmqpConsumer consumer, Message message, Charset charset) {
        return new JmsTextMessage(new AmqpJmsTextMessageFacade(consumer, message, charset));
    }

    private static JmsBytesMessage createBytesMessage(AmqpConsumer consumer, Message message) {
        return new JmsBytesMessage(new AmqpJmsBytesMessageFacade(consumer, message));
    }

    private static JmsMessage createMessage(AmqpConsumer consumer, Message message) {
        return new JmsMessage(new AmqpJmsMessageFacade(consumer, message));
    }

    /**
     * @param contentType the contentType of the received message
     * @return the character set to use, or null if not to treat the message as text
     */
    private static Charset getCharsetForTextualContent(String contentType) {
        try {
            return ContentTypeSupport.parseContentTypeForTextualCharset(contentType);
        } catch (InvalidContentTypeException e) {
            return null;
        }
    }
}
