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
import org.apache.qpid.jms.engine.AmqpLink;
import org.apache.qpid.jms.engine.ConnectionException;
import org.apache.qpid.proton.TimeoutException;

public class LinkImpl
{
    private SessionImpl _sessionImpl;
    private ConnectionImpl _connectionImpl;
    private AmqpLink _amqpLink;

    public LinkImpl(SessionImpl sessionImpl, AmqpLink amqpLink)
    {
        _sessionImpl = sessionImpl;
        _connectionImpl = _sessionImpl.getConnectionImpl();
        _amqpLink = amqpLink;
    }

    public void establish() throws TimeoutException, InterruptedException
    {
        _connectionImpl.waitUntil(new Predicate()
        {
            public boolean test()
            {
                return _amqpLink.isEstablished();
            }
        }, AmqpConnection.TIMEOUT);
    }

    public void close() throws TimeoutException, InterruptedException, ConnectionException
    {
        _connectionImpl.lock();        
        try
        {
            _amqpLink.close();
            _connectionImpl.stateChanged();
            while(!_amqpLink.isClosed())
            {
                _connectionImpl.waitUntil(new Predicate()
                {
                    public boolean test()
                    {
                        return _amqpLink.isClosed();
                    }
                }, AmqpConnection.TIMEOUT);
            }

            //TODO: link errors? E.g:
            //            if(_amqpSender.getLinkError().getCondition() != null)
            //            {
            //                throw new ConnectionException("Sender close failed: " + _amqpSender.getLinkError());
            //            }
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

}