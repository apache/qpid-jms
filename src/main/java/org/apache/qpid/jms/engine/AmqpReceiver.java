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

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;

public class AmqpReceiver extends AmqpLink
{
    private Receiver _protonReceiver;
    private byte[] _buffer = new byte[1024];

    public AmqpReceiver(AmqpSession amqpSession, Receiver protonReceiver)
    {
        super(amqpSession, protonReceiver);        
        _protonReceiver = protonReceiver;
    }

    public void credit(int credit)
    {
        _protonReceiver.flow(credit);
    }

    public AmqpReceivedMessage receiveNoWait()
    {
        synchronized (getAmqpConnection())
        {
            Delivery currentDelivery = _protonReceiver.current();
            if(currentDelivery != null)
            {
                if(currentDelivery.getContext() == null)
                {
                    if (currentDelivery.isReadable() && !currentDelivery.isPartial())
                    {

                        int total = 0;
                        int start = 0;
                        while (true)
                        {
                            int read = _protonReceiver.recv(_buffer, start, _buffer.length - start);
                            total += read;
                            if (read == (_buffer.length - start))
                            {
                                //may need to expand the buffer (is there a better test?)
                                byte[] old = _buffer;
                                _buffer = new byte[_buffer.length*2];
                                System.arraycopy(old, 0, _buffer, 0, old.length);
                                start += read;
                            }
                            else
                            {
                                break;
                            }
                        }
                        Message message = getAmqpConnection().getMessageFactory().createMessage();
                        message.decode(_buffer, 0, total);

                        AmqpReceivedMessage amqpMessage = new AmqpReceivedMessage(currentDelivery, message, this);
                        currentDelivery.setContext(amqpMessage);
                        _protonReceiver.advance();
                        return amqpMessage;
                    }
                }                
            }
        }
        return null;
    }

}
