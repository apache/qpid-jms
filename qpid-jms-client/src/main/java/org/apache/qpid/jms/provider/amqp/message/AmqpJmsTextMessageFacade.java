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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.jms.JMSException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsTextMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS TextMessage
 * type.
 */
public class AmqpJmsTextMessageFacade extends AmqpJmsMessageFacade implements JmsTextMessageFacade {

    private final Charset charset;

    /**
     * Create a new AMQP Message facade ready for sending.
     *
     * @param connection
     *        the AmqpConnection that under which this facade was created.
     */
    public AmqpJmsTextMessageFacade(AmqpConnection connection) {
        super(connection);
        setMessageAnnotation(JMS_MSG_TYPE, JMS_TEXT_MESSAGE);
        setText(null);
        charset = StandardCharsets.UTF_8;
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param consumer
     *        the consumer that received this message.
     * @param message
     *        the incoming Message instance that is being wrapped.
     * @param charset
     *        the character set to use when decoding the text when the body is a Data section
     */
    public AmqpJmsTextMessageFacade(AmqpConsumer consumer, Message message, Charset charset) {
        super(consumer, message);
        this.charset = charset;
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    @Override
    public byte getJmsMsgType() {
        return JMS_TEXT_MESSAGE;
    }

    @Override
    public AmqpJmsTextMessageFacade copy() throws JMSException {
        AmqpJmsTextMessageFacade copy = new AmqpJmsTextMessageFacade(connection);
        copyInto(copy);
        copy.setText(getText());
        return copy;
    }

    @Override
    public String getText() throws JMSException {
        Section body = getAmqpMessage().getBody();

        if (body == null) {
            return null;
        } else if (body instanceof Data) {
            Data data = (Data) body;
            if (data.getValue() == null || data.getValue().getLength() == 0) {
                return "";
            } else {
                Binary b = data.getValue();
                ByteBuffer buf = ByteBuffer.wrap(b.getArray(), b.getArrayOffset(), b.getLength());

                try {
                    CharBuffer chars = charset.newDecoder().decode(buf);
                    return String.valueOf(chars);
                } catch (CharacterCodingException e) {
                    throw JmsExceptionSupport.create("Cannot decode String in " + charset.displayName(), e);
                }
            }
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();

            if (value == null || value instanceof String) {
                return (String) value;
            } else {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        } else {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }

    @Override
    public void setText(String value) {
        AmqpValue body = new AmqpValue(value);
        getAmqpMessage().setBody(body);
    }

    @Override
    public void clearBody() {
        setText(null);
    }

    Charset getCharset() {
        return charset;
    }
}
