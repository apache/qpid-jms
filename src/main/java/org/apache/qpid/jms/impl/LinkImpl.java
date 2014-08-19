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

import javax.jms.JMSException;

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpLink;
import org.apache.qpid.jms.engine.AmqpResourceRequest;

public class LinkImpl
{
    private ConnectionImpl _connectionImpl;
    private AmqpLink _amqpLink;

    public LinkImpl(ConnectionImpl connectionImpl, AmqpLink amqpLink)
    {
        _connectionImpl = connectionImpl;
        _amqpLink = amqpLink;
    }

    public void close() throws JMSException
    {
        _connectionImpl.lock();
        try
        {
            _amqpLink.close();
            _connectionImpl.stateChanged();
            _connectionImpl.waitUntil(new SimplePredicate("Link is closed", _amqpLink)
            {
                @Override
                public boolean test()
                {
                    return _amqpLink.isClosed();
                }
            }, AmqpConnection.TIMEOUT);
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

    public void open(AmqpResourceRequest<Void> request) throws JmsTimeoutException, JmsInterruptedException
    {
        _amqpLink.open(request);
    }
}