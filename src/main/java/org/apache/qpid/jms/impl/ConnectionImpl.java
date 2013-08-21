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

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpConnectionDriver;
import org.apache.qpid.jms.engine.AmqpSession;
import org.apache.qpid.jms.engine.ConnectionException;
import org.apache.qpid.proton.TimeoutException;

/**
 * A JMS connection.
 * Thread-safety:
 * <ul>
 * <li>All public methods are thread-safe</li>
 * <li>Other internal classes must use the connection's lock and state-change methods -
 *     see {@link #lock()}/{@link #releaseLock()} and {@link #stateChanged()} for details.</li>
 * </ul>
 */
public class ConnectionImpl implements Connection
{
    private static final Logger _logger = Logger.getLogger(ConnectionImpl.class.getName());

    private AmqpConnection _amqpConnection;

    /** The driver dedicated to this connection */
    private AmqpConnectionDriver _amqpConnectionDriver;

    private ConnectionLock _connectionLock;

    /**
     * TODO: accept a client id
     * TODO: defer connection to the broker if client has not been set. Defer it until any other method is called.
     */
    public ConnectionImpl(String clientName, String remoteHost, int port, String username, String password) throws JMSException
    {
        _amqpConnection = new AmqpConnection(clientName, remoteHost, port);
        _amqpConnection.setUsername(username);
        _amqpConnection.setPassword(password);

        try
        {
            _amqpConnectionDriver = new AmqpConnectionDriver();
            _amqpConnectionDriver.registerConnection(_amqpConnection);

            _connectionLock = new ConnectionLock(this);
            _connectionLock.setConnectionStateChangeListener(new ConnectionStateChangeListener()
            {
                @Override
                public void stateChanged(ConnectionImpl connection)
                {
                    connection._amqpConnectionDriver.setLocallyUpdated(connection._amqpConnection);
                }
            });

            connect();
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
            JMSException jmse = new JMSException("Interrupted while trying to create connection");
            jmse.setLinkedException(e);
            throw jmse;
        }
        catch (TimeoutException | IOException | ConnectionException e)
        {
            JMSException jmse = new JMSException("Unable to create connection");
            jmse.setLinkedException(e);
            throw jmse;
        }
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

    private void connect() throws IOException, ConnectionException, TimeoutException, InterruptedException
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

    @Override
    public void close() throws JMSException
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
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
            JMSException jmse = new JMSException("Interrupted while trying to close connection");
            jmse.setLinkedException(e);
            throw jmse;
        }
        catch (TimeoutException | ConnectionException e)
        {
            JMSException jmse = new JMSException("Unable to close connection");
            jmse.setLinkedException(e);
            throw jmse;
        }
        finally
        {
            releaseLock();
        }
    }

    /**
     * TODO the params are ignored - fix this
     */
    @Override
    public SessionImpl createSession(boolean transacted, int acknowledgeMode) throws JMSException
    {
        if(transacted)
        {
            throw new UnsupportedOperationException("Only transacted=false is currently supported");
        }
        if(acknowledgeMode != Session.AUTO_ACKNOWLEDGE)
        {
            throw new UnsupportedOperationException("Only acknowledgeMode=AUTO_ACKNOWLEDGE is currently supported");
        }

        lock();
        try
        {
            AmqpSession amqpSession = _amqpConnection.createSession();

            SessionImpl session = new SessionImpl(amqpSession, this);
            stateChanged();
            session.establish();

            return session;
        }
        catch (TimeoutException e)
        {
            JMSException jmse = new JMSException("Unable to create session");
            jmse.setLinkedException(e);
            throw jmse;
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
            JMSException jmse = new JMSException("Interrupted while trying to create session");
            jmse.setLinkedException(e);
            throw jmse;
        }
        finally
        {
            releaseLock();
        }
    }

    /**
     * TODO add @Override when we start implementing the JMS2 API.
     */
    public Session createSession() throws JMSException
    {
        return createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * <p>
     * Acquire the connection lock.
     * </p>
     * <p>
     * Must be held by an application thread before reading or modifying
     * the state of this connection or any of its associated child objects
     * (e.g. sessions, senders, receivers, links, and messages).
     * Also must be held when calling {@link #stateChanged()}.
     * </p>
     * <p>
     * Following these rules ensures that this lock is acquired BEFORE the lock(s) managed by {@link AmqpConnection}.
     * </p>
     *
     * @see #releaseLock()
     */
    void lock()
    {
        _connectionLock.lock();
    }

    /**
     * @see #lock()
     */
    void releaseLock()
    {
        _connectionLock.unlock();
    }

    /**
     * Inform the connection that its state has been locally changed so that, for example,
     * it can schedule network I/O to occur.
     * The caller must first acquire the connection lock (via {@link #lock()}).
     */
    void stateChanged()
    {
        _connectionLock.stateChanged();
    }

    @Override
    public String getClientID() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setClientID(String clientID) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void start() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void stop() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

}
