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

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;

/**
 * Thread-safe (all state is guarded by the corresponding {@link AmqpConnection} monitor)
 *
 */
public class AmqpMessage
{

    private final AmqpReceiver _amqpReceiver;
    private final Delivery _delivery;
    private final Message _message;

    public AmqpMessage(Delivery delivery, Message message, AmqpReceiver amqpReceiver)
    {
        _delivery = delivery;
        _amqpReceiver = amqpReceiver;
        _message = message;
    }

    /**
     * Currently used when creating a message that we intend to send
     */
    public AmqpMessage()
    {
        _message = Proton.message();
        _amqpReceiver = null;
        _delivery = null;
    }

    Message getMessage()
    {
        return _message;
    }

    public void accept(boolean settle)
    {
        synchronized (_amqpReceiver.getAmqpConnection())
        {
            _delivery.disposition(Accepted.getInstance());
            if(settle)
            {
                _delivery.settle();
            }
        }
    }

    public void settle()
    {
        synchronized (_amqpReceiver.getAmqpConnection())
        {
            _delivery.settle();
        }
    }

    /**
     * If using proton-j, returns true if locally or remotely settled.
     * If using proton-c, returns true if remotely settled.
     * TODO - remove this hack when Proton-J and -C APIs are properly aligned
     * The C API defines isSettled as being true if the delivery has been settled locally OR remotely
     */
    public boolean isSettled()
    {
        synchronized (_amqpReceiver.getAmqpConnection())
        {
            return _delivery.isSettled() || ((_delivery instanceof DeliveryImpl && ((DeliveryImpl)_delivery).remotelySettled()));
        }
    }

    public void setText(String string)
    {
        AmqpValue body = new AmqpValue(string);
        _message.setBody(body);
    }
}
