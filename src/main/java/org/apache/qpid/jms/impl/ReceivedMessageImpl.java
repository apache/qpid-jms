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

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpReceivedMessage;
import org.apache.qpid.proton.TimeoutException;
import org.apache.qpid.proton.message.Message;

public class ReceivedMessageImpl
{
    private AmqpReceivedMessage _amqpMessage;
    private ReceiverImpl _receiverImpl;

    public ReceivedMessageImpl(AmqpReceivedMessage amqpMessage, ReceiverImpl receiverImpl)
    {
        _amqpMessage = amqpMessage;
        _receiverImpl = receiverImpl;
    }

    public void accept(boolean settle)
    {
        _receiverImpl.getConnectionImpl().lock();
        try
        {
            _amqpMessage.accept();
            if(settle)
            {
                _amqpMessage.settle();
            }
            _receiverImpl.getConnectionImpl().stateChanged();
        }
        finally
        {
            _receiverImpl.getConnectionImpl().releaseLock();
        }
    }

    public void settle()
    {       
        _receiverImpl.getConnectionImpl().lock();
        try
        {
            _amqpMessage.settle();
            _receiverImpl.getConnectionImpl().stateChanged();
        }
        finally
        {
            _receiverImpl.getConnectionImpl().releaseLock();
        }
    }

    public void waitUntilSettled() throws TimeoutException, InterruptedException
    {
        _receiverImpl.getConnectionImpl().lock();
        try
        {
            _receiverImpl.getConnectionImpl().waitUntil(new Predicate()
            {
                @Override
                public boolean test()
                {
                    return _amqpMessage.isSettled();
                }
            }, AmqpConnection.TIMEOUT);
        }
        finally
        {
            _receiverImpl.getConnectionImpl().releaseLock();
        }
    }

    public Message getMessage()
    {
        return _amqpMessage.getMessage();
    }
}
