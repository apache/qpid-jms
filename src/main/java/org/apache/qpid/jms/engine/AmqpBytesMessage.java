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

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpBytesMessage extends AmqpMessage
{
    public static final String CONTENT_TYPE = "application/octet-stream";
    private long _length;

    //message to be sent
    public AmqpBytesMessage()
    {
        super();
        setContentType(CONTENT_TYPE);
    }

    //message just received
    public AmqpBytesMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);
    }

    /**
     * Sets the bytes included in the Data body section of the underlying AMQP message.
     *
     * If the original body was not a data section, the content type is set to {@link AmqpBytesMessage#CONTENT_TYPE}
     *
     * A null value clears the body section entirely.
     * @param bytes the contents of the data section, or null to clear the body entirely
     */
    public void setBytes(byte[] bytes)
    {
        boolean origBodyTypeData = getMessage().getBody() instanceof Data;

        Data body = null;
        if(bytes != null)
        {
            body = new Data(new Binary(bytes));
        }

        getMessage().setBody(body);

        if(!origBodyTypeData)
        {
            setContentType(CONTENT_TYPE);
        }
    }

    /**
     * @throws IllegalStateException if the underlying message content can't be retrieved as bytes
     */
    public ByteArrayInputStream getByteArrayInputStream() throws IllegalStateException
    {
        Section body = getMessage().getBody();

        if(body == null)
        {
            _length = 0;
            return createEmptyByteArrayInputStream();
        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null)
            {
                _length = 0;
                return createEmptyByteArrayInputStream();
            }
            else if(value instanceof Binary)
            {
                Binary b = (Binary)value;
                _length = b.getLength();
                return new ByteArrayInputStream(b.getArray(), b.getArrayOffset(), b.getLength());
            }
            else
            {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        }
        else if(body instanceof Data)
        {
            Binary b = ((Data) body).getValue();
            if(b == null)
            {
                _length = 0;
                return createEmptyByteArrayInputStream();
            }
            else
            {
                _length = b.getLength();
                return new ByteArrayInputStream(b.getArray(), b.getArrayOffset(), b.getLength());
            }
        }
        else
        {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }

    private ByteArrayInputStream createEmptyByteArrayInputStream()
    {
        return new ByteArrayInputStream(new byte[0]);
    }

    /**
     * @throws IllegalStateException if the underlying message content can't be retrieved as bytes
     */
    public long getBytesLength() throws IllegalStateException
    {
        getByteArrayInputStream();
        return _length;
    }

    public void convertBodyToDataSectionIfNecessary()
    {
        Section body = getMessage().getBody();
        byte[] bytes;

        if(body == null)
        {
            bytes = new byte[0];
        }
        else if(body instanceof Data)
        {
            //Nothing to do
            return;
        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null)
            {
                bytes = new byte[0];
            }
            else if(value instanceof Binary)
            {
                Binary b = (Binary)value;
                int length = b.getLength();
                int offset = b.getArrayOffset();

                bytes = Arrays.copyOfRange(b.getArray(), offset, offset + length);
            }
            else
            {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        }
        else
        {
            throw new IllegalStateException("Unexpected body content type: " + body.getClass().getSimpleName());
        }

        setBytes(bytes);
    }
}
