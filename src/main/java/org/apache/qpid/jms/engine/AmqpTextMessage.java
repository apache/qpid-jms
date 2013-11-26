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

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.impl.DataImpl;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpTextMessage extends AmqpMessage
{
    /**
     * Content type, only to be used when message uses a data
     * body section, and not when using an amqp-value body section
     */
    public static final String CONTENT_TYPE = "text/plain";

    /**
     * Message type annotation value, only to be used when message uses
     * an amqp-value body section containing a null value, not otherwise.
     */
    public static final String MSG_TYPE_ANNOTATION_VALUE = "TextMessage";

    public AmqpTextMessage()
    {
        super();
        setText(null);
    }

    public AmqpTextMessage(Delivery delivery, Message message, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);
    }

    public void setText(String text)
    {
        AmqpValue body = new AmqpValue(text);
        getMessage().setBody(body);

        //TODO: clear the content-type in the case where we had received
        //a message containing Data+ContentType
    }

    /**
     * @throws IllegalStateException if the underlying message content can't be retrieved as a String or null
     */
    public String getText() throws IllegalStateException
    {
        Section body = getMessage().getBody();

        if(body == null)
        {
            return null;
        }
        else if(body instanceof Data)
        {
            Data data = (Data) body;
            if(data.getValue() == null || data.getValue().getLength() == 0)
            {
                return null;
            }
            else
            {
                Binary b = data.getValue();

                ByteBuffer buf = ByteBuffer.wrap(b.getArray(), b.getArrayOffset(), b.getLength());
                org.apache.qpid.proton.codec.Data codecData = new DataImpl();
                codecData.decode(buf);

                if(codecData.isNull())
                {
                    return null;
                }
                else
                {
                    return codecData.getString();
                }
            }

        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null || value instanceof String)
            {
                return (String) value;
            }
            else
            {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        }
        else
        {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }
}
