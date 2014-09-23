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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import javax.jms.JMSException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsTextMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
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

    private static final String UTF_8 = "UTF-8";

    /**
     * Content type, only to be used when message uses a data
     * body section, and not when using an amqp-value body section
     */
    public static final String CONTENT_TYPE = "text/plain";

    private final CharsetDecoder decoder =  Charset.forName(UTF_8).newDecoder();

    /**
     * Create a new AMQP Message facade ready for sending.
     *
     * @param connection
     *        The AMQP Connection that created this message.
     */
    public AmqpJmsTextMessageFacade(AmqpConnection connection) {
        super(connection);
        setAnnotation(JMS_MSG_TYPE, JMS_TEXT_MESSAGE);
        setText(null);
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param connection
     *        the connection that created this Facade.
     * @param message
     *        the incoming Message instance that is being wrapped.
     */
    public AmqpJmsTextMessageFacade(AmqpConnection connection, Message message) {
        super(connection, message);
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    @Override
    public byte getJmsMsgType() {
        return JMS_TEXT_MESSAGE;
    }

    @Override
    public JmsTextMessageFacade copy() throws JMSException {
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
                    CharBuffer chars = decoder.decode(buf);
                    return String.valueOf(chars);
                } catch (CharacterCodingException e) {
                    throw JmsExceptionSupport.create("Cannot decode String in UFT-8", e);
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

    @Override
    public boolean isEmpty() {
        Section body = getAmqpMessage().getBody();

        if (body == null) {
            return true;
        } else if (body instanceof Data) {
            Data data = (Data) body;
            if (data.getValue() == null || data.getValue().getLength() == 0) {
                return true;
            }
        }

        return false;
    }
}
