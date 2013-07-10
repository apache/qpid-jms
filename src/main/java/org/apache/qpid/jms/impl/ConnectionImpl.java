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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpConnectionDriver;
import org.apache.qpid.jms.engine.AmqpSession;
import org.apache.qpid.jms.engine.ConnectionException;
import org.apache.qpid.proton.TimeoutException;

public class ConnectionImpl
{
    private static final Logger _logger = Logger.getLogger(ConnectionImpl.class.getName());

    private AmqpConnection _amqpConnection;

    /** The driver dedicated to this connection */
    private AmqpConnectionDriver _amqpConnectionDriver;
    private ConnectionLock _connectionLock;

    public ConnectionImpl(String clientName, String remoteHost, int port, String username, String password)
    {
        _amqpConnection = new AmqpConnection(clientName, remoteHost, port);
        _amqpConnection.setUsername(username);
        _amqpConnection.setPassword(password);

        try
        {
            _amqpConnectionDriver = new AmqpConnectionDriver();
            _amqpConnectionDriver.registerConnection(_amqpConnection);
        }
        catch (IOException e)
        {
            // TODO this will eventually be moved elsewhere
            throw new RuntimeException(e);
        }

        _connectionLock = new ConnectionLock(this);
        _connectionLock.setConnectionStateChangeListener(new ConnectionStateChangeListener()
        {
            @Override
            public void stateChanged(ConnectionImpl connection)
            {
                connection._amqpConnectionDriver.setLocallyUpdated(connection._amqpConnection);
            }
        });
    }

    void waitUntil(Predicate condition, long timeoutMillis) throws TimeoutException, InterruptedException
    {
        long deadline = timeoutMillis < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMillis;

        boolean wait = deadline > System.currentTimeMillis();
        boolean first = true;
        boolean done = false;

        synchronized (_amqpConnection)
        {
            while (first || (!done && wait))
            {
                if(_logger.isLoggable(Level.FINER))
                {
                    _logger.log(Level.FINER,
                            "About to waitUntil {0}. first={1}, done={2}, wait={3}",
                            new Object[] {condition, first, done, wait});
                }
                if (wait && !done && !first)
                {
                    _amqpConnection.wait(timeoutMillis < 0 ? 0 : deadline - System.currentTimeMillis());
                }

                wait = deadline > System.currentTimeMillis();
                done = done || condition.test();
                first = false;
            }
            if(_logger.isLoggable(Level.FINER))
            {
                _logger.log(Level.FINER,
                        "Finished waitUntil {0}. first={1}, done={2}, wait={3}",
                        new Object[] {condition, first, done, wait});
            }

            if (!done)
            {
                throw new TimeoutException(timeoutMillis, condition.getCurrentState());
            }
        }
    }

    public void connect() throws IOException, ConnectionException, TimeoutException, InterruptedException
    {
        lock();
        try
        {
            waitUntil(new SimplePredicate("Connection established or failed", _amqpConnection)
            {
                @Override
                public boolean test()
                {
                    return _amqpConnection.isConnected() || _amqpConnection.isAuthenticationError() || _amqpConnection.getConnectionError().getCondition() != null;
                }
            }, AmqpConnection.TIMEOUT);

            if(_amqpConnection.getConnectionError().getCondition() != null)
            {
                throw new ConnectionException("Connection failed: 1 " + _amqpConnection.getConnectionError());
            }

            if(_amqpConnection.isAuthenticationError())
            {
                throw new ConnectionException("Connection failed: 2");
            }

            if(!_amqpConnection.isConnected())
            {
                throw new ConnectionException("Connection failed: 3");
            }
        }
        finally
        {
            releaseLock();
        }
    }

    public void close() throws TimeoutException, InterruptedException, ConnectionException
    {
        lock();
        try
        {
            _amqpConnection.close();
            stateChanged();
            waitUntil(new SimplePredicate("Connection is closed", _amqpConnection)
            {
                @Override
                public boolean test()
                {
                    return _amqpConnection.isClosed();
                }
            }, AmqpConnection.TIMEOUT);

            _amqpConnectionDriver.stop();

            if(_amqpConnection.getConnectionError().getCondition() != null)
            {
                throw new ConnectionException("Connection close failed: " + _amqpConnection.getConnectionError());
            }
        }
        finally
        {
            releaseLock();
        }
    }

    public SessionImpl createSession() throws TimeoutException, InterruptedException
    {
        lock();
        try
        {
            AmqpSession amqpSession = _amqpConnection.createSession();

            SessionImpl session = new SessionImpl(amqpSession, this);
            stateChanged();
            session.establish();

            return session;
        }
        finally
        {
            releaseLock();
        }
    }

    void lock()
    {
        _connectionLock.lock();
    }

    void releaseLock()
    {
        _connectionLock.unlock();
    }

    void stateChanged()
    {
        _connectionLock.stateChanged();
    }

}
