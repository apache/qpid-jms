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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpListMessage extends AmqpMessage
{
    private List<Object> _list;
    private int _position = 0;

    //message to be sent
    public AmqpListMessage()
    {
        super();
        _list = createListBody(getMessage());
        setMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE, ClientProperties.STREAM_MESSAGE_TYPE);
    }

    //message just received
    public AmqpListMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);
        _list = processBodyList(message);
    }

    @SuppressWarnings("unchecked")
    private List<Object> processBodyList(Message message)
    {
        Section body = getMessage().getBody();

        if(body == null)
        {
            return createListBody(message);
        }
        else if(body instanceof AmqpValue)
        {
            Object value = ((AmqpValue) body).getValue();

            if(value == null)
            {
                return createListBody(message);
            }
            else if(value instanceof List<?>)
            {
                return (List<Object>) value;
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

    private List<Object> createListBody(Message message)
    {
        List<Object> list = new ArrayList<Object>();
        message.setBody(new AmqpValue(list));

        return list;
    }

    /**
     * Adds the provided Object to the list.
     *
     * If the value provided is a byte[] its entry in the list is as an AMQP Binary, and the
     * underlying array is NOT copied and MUST NOT be subsequently altered.
     */
    public void add(final Object o)
    {
        Object val = o;
        if(val instanceof byte[])
        {
            val = new Binary((byte[]) o);
        }

        _list.add(val);
    }

    /**
     *
     * Returns the next value in the list, or throws {@link IndexOutOfBoundsException} if there are no unread entries remaining.
     *
     * If the value being returned is a byte[] representing AMQP binary, the array returned IS a copy.
     *
     * @throws IndexOutOfBoundsException
     */
    public Object get() throws IndexOutOfBoundsException
    {
        Object object = _list.get(_position++);
        if(object instanceof Binary)
        {
            //We will return a byte[]. It is possibly only part of the underlying array, copy that bit.
            Binary bin = ((Binary) object);

            return Arrays.copyOfRange(bin.getArray(), bin.getArrayOffset(), bin.getLength());
        }

        return object;
    }

    public void clear()
    {
        _list.clear();
        resetPosition();
    }

    public void resetPosition()
    {
        _position = 0;
    }

    public void decrementPosition()
    {
        _position--;
    }

}
