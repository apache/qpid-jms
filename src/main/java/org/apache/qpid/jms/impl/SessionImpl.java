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
import org.apache.qpid.jms.engine.AmqpReceiver;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSession;
import org.apache.qpid.jms.engine.ConnectionException;
import org.apache.qpid.jms.engine.LinkException;
import org.apache.qpid.proton.TimeoutException;

public class SessionImpl
{
    private AmqpSession _amqpSession;
    private ConnectionImpl _connectionImpl;

    public SessionImpl(AmqpSession amqpSession, ConnectionImpl connectionImpl)
    {
        _amqpSession = amqpSession;
        _connectionImpl = connectionImpl;
    }

    public void establish() throws TimeoutException, InterruptedException
    {
        _connectionImpl.waitUntil(new SimplePredicate("Session established", _amqpSession)
        {
            @Override
            public boolean test()
            {
                return _amqpSession.isEstablished();
            }
        }, AmqpConnection.TIMEOUT);
    }

    public void close() throws TimeoutException, InterruptedException, ConnectionException
    {
        _connectionImpl.lock();
        try
        {
            _amqpSession.close();
            _connectionImpl.stateChanged();
            while(!_amqpSession.isClosed())
            {
                _connectionImpl.waitUntil(new SimplePredicate("Session is closed", _amqpSession)
                {
                    @Override
                    public boolean test()
                    {
                        return _amqpSession.isClosed();
                    }
                }, AmqpConnection.TIMEOUT);
            }

            if(_amqpSession.getSessionError().getCondition() != null)
            {
                throw new ConnectionException("Session close failed: " + _amqpSession.getSessionError());
            }
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }

    ConnectionImpl getConnectionImpl()
    {
        return _connectionImpl;
    }

    public SenderImpl createSender(String name, String address) throws TimeoutException, InterruptedException, LinkException
    {
        _connectionImpl.lock();
        try
        {
            AmqpSender amqpSender = _amqpSession.createAmqpSender(name, address);
            SenderImpl sender = new SenderImpl(this, amqpSender);
            _connectionImpl.stateChanged();
            sender.establish();
            return sender;
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }

    public ReceiverImpl createReceiver(String name, String address) throws TimeoutException, InterruptedException, LinkException
    {
        _connectionImpl.lock();
        try
        {
            AmqpReceiver amqpReceiver = _amqpSession.createAmqpReceiver(name, address);
            ReceiverImpl receiver = new ReceiverImpl(this, amqpReceiver);
            _connectionImpl.stateChanged();
            receiver.establish();
            return receiver;
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }
}
