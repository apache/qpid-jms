/*
 *
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
 *
 */
package org.apache.qpid.jms.engine;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;

public class AmqpSender extends AmqpLink
{
    private long tag;
    private byte[] _buffer = new byte[1024];
    private final Sender _protonSender;

    public AmqpSender(AmqpSession amqpSession, Sender protonSender)
    {
        super(amqpSession, protonSender);
        _protonSender = protonSender;
    }

    public AmqpSentMessage sendMessage(Message message)
    {
        synchronized (getAmqpConnection())
        {
            byte[] bufferBytes = new byte[8];
            ByteBuffer buffer = ByteBuffer.wrap(bufferBytes);

            buffer.putLong(0,tag++);

            Delivery del = _protonSender.delivery(bufferBytes);

            int encoded;
            while (true)
            {
                try
                {
                    encoded = message.encode(_buffer, 0, _buffer.length);
                    break;
                }
                catch (java.nio.BufferOverflowException e)
                {
                    _buffer = new byte[_buffer.length * 2];
                }
            }
            _protonSender.send(_buffer, 0, encoded);
            _protonSender.advance();

            AmqpSentMessage amqpSentMessage = new AmqpSentMessage(del, this);
            del.setContext(amqpSentMessage);

            return amqpSentMessage;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AmqpSender [tag=").append(tag)
            .append(", _protonSender=").append(_protonSender)
            .append("]");
        return builder.toString();
    }
}
