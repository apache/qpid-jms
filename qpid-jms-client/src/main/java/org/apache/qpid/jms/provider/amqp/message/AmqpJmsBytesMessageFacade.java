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
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * A JmsBytesMessageFacade that wraps around Proton AMQP Message instances to provide
 * access to the underlying bytes contained in the message.
 */
public class AmqpJmsBytesMessageFacade extends AmqpJmsMessageFacade implements JmsBytesMessageFacade {

    private static final String CONTENT_TYPE = "application/octet-stream";
    private static final Data EMPTY_DATA = new Data(new Binary(new byte[0]));

    /**
     * Creates a new facade instance
     *
     * @param connection
     *        the AmqpConnection that under which this facade was created.
     */
    public AmqpJmsBytesMessageFacade(AmqpConnection connection) {
        super(connection);
        setContentType(CONTENT_TYPE);
        setAnnotation(JMS_MSG_TYPE, JMS_BYTES_MESSAGE);
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param consumer
     *        the consumer that received this message.
     * @param message
     *        the incoming Message instance that is being wrapped.
     */
    public AmqpJmsBytesMessageFacade(AmqpConsumer consumer, Message message) {
        super(consumer, message);
    }

    @Override
    public AmqpJmsBytesMessageFacade copy() {
        AmqpJmsBytesMessageFacade copy = new AmqpJmsBytesMessageFacade(connection);
        copyInto(copy);
        return copy;
    }

    @Override
    public byte getJmsMsgType() {
        return JMS_BYTES_MESSAGE;
    }

    @Override
    public boolean isEmpty() {
        Binary payload = getBinaryFromBody();
        return payload != null && payload.getLength() > 0;
    }

    @Override
    public ByteBuf getContent() {
        ByteBuf result = Unpooled.EMPTY_BUFFER;
        Binary payload = getBinaryFromBody();
        if (payload != null && payload.getLength() > 0) {
            result = Unpooled.wrappedBuffer(payload.getArray(), payload.getArrayOffset(), payload.getLength());
        }

        return result;
    }

    @Override
    public void setContent(ByteBuf content) {
        Data body = EMPTY_DATA;
        if (content != null) {
            body = new Data(new Binary(content.array(), content.arrayOffset(), content.readableBytes()));
        }

        getAmqpMessage().setBody(body);
    }

    private Binary getBinaryFromBody() {
        Section body = getAmqpMessage().getBody();
        Binary result = null;

        if (body == null) {
            return result;
        }

        if (body instanceof Data) {
            Binary payload = ((Data) body).getValue();
            if (payload != null && payload.getLength() != 0) {
                result = payload;
            }
        } else if(body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value == null) {
                return result;
            }

            if (value instanceof Binary) {
                Binary payload = (Binary)value;
                if (payload != null && payload.getLength() != 0) {
                    result = payload;
                }
            } else {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        } else {
            throw new IllegalStateException("Unexpected body content type: " + body.getClass().getSimpleName());
        }

        return result;
    }
}
