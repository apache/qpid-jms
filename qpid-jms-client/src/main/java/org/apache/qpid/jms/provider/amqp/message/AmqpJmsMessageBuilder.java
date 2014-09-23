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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;

import java.io.IOException;
import java.util.Map;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMapMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.message.Message;

/**
 * Builder class used to construct the appropriate JmsMessage / JmsMessageFacade
 * objects to wrap an incoming AMQP Message.
 */
public class AmqpJmsMessageBuilder {

    private AmqpJmsMessageBuilder() {
    }

    /**
     * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
     * message type for the incoming AMQP message.
     *
     * @param connection
     *        The provider AMQP Connection instance where this message arrived at.
     * @param message
     *        The Proton Message object that will be wrapped.
     *
     * @return a JmsMessage instance properly configured for dispatch to the provider listener.
     *
     * @throws IOException if an error occurs while creating the message objects.
     */
    public static JmsMessage createJmsMessage(AmqpConnection connection, Message message) throws IOException {

        // First we try the easy way, if the annotation is there we don't have to work hard.
        JmsMessage result = createFromMsgAnnotation(connection, message);
        if (result != null) {
            return result;
        }

        // TODO
        throw new IOException("Could not create a JMS message from incoming message");
    }

    private static JmsMessage createFromMsgAnnotation(AmqpConnection connection, Message message) throws IOException {
        Object annotation = getMessageAnnotation(JMS_MSG_TYPE, message);
        if (annotation != null) {

            switch ((byte) annotation) {
                case JMS_MESSAGE:
                    return new JmsMessage(new AmqpJmsMessageFacade(connection, message));
                case JMS_BYTES_MESSAGE:
                    return new JmsBytesMessage(new AmqpJmsBytesMessageFacade(connection, message));
                case JMS_TEXT_MESSAGE:
                    return new JmsTextMessage(new AmqpJmsTextMessageFacade(connection, message));
                case JMS_MAP_MESSAGE:
                    return new JmsMapMessage(new AmqpJmsMapMessageFacade(connection, message));
                case JMS_STREAM_MESSAGE:
                    return new JmsStreamMessage(new AmqpJmsStreamMessageFacade(connection, message));
                case JMS_OBJECT_MESSAGE:
                    return new JmsObjectMessage(new AmqpJmsObjectMessageFacade(connection, message));
                default:
                    throw new IOException("Invalid JMS Message Type annotation found in message");
            }
        }

        return null;
    }

    private static Object getMessageAnnotation(String key, Message message) {
        if (message.getMessageAnnotations() != null) {
            Map<Symbol, Object> annotations = message.getMessageAnnotations().getValue();
            return annotations.get(AmqpMessageSupport.getSymbol(key));
        }

        return null;
    }
}
