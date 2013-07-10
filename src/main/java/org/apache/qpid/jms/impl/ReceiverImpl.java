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
package org.apache.qpid.jms.impl;

import org.apache.qpid.jms.engine.AmqpReceivedMessage;
import org.apache.qpid.jms.engine.AmqpReceiver;
import org.apache.qpid.proton.TimeoutException;

public class ReceiverImpl extends LinkImpl
{
    private final AmqpReceiver _amqpReceiver;

    public ReceiverImpl(SessionImpl sessionImpl, AmqpReceiver amqpReceiver)
    {
        super(sessionImpl, amqpReceiver);
        _amqpReceiver = amqpReceiver;
    }

    public ReceivedMessageImpl receive(long timeout) throws TimeoutException, InterruptedException
    {
        getConnectionImpl().lock();
        try
        {
            MessageReceivedPredicate messageReceievedCondition = new MessageReceivedPredicate();
            getConnectionImpl().waitUntil(messageReceievedCondition, timeout);
            getConnectionImpl().stateChanged();

            return new ReceivedMessageImpl(messageReceievedCondition.getReceivedMessage(), this);
        }
        finally
        {
            getConnectionImpl().releaseLock();
        }
    }

    public void credit(int credit)
    {
        getConnectionImpl().lock();
        try
        {
            _amqpReceiver.credit(credit);
            getConnectionImpl().stateChanged();
        }
        finally
        {
            getConnectionImpl().releaseLock();
        }
    }

    private final class MessageReceivedPredicate extends SimplePredicate
    {
        AmqpReceivedMessage _message;

        public MessageReceivedPredicate()
        {
            super("Message received", _amqpReceiver);
        }

        @Override
        public boolean test()
        {
            if(_message == null)
            {
                _message = _amqpReceiver.receiveNoWait();
            }
            return _message != null;
        }

        public AmqpReceivedMessage getReceivedMessage()
        {
            return _message;
        }
    }

}
