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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.jms.engine.temp.AbstractEventHandler;
import org.apache.qpid.jms.engine.temp.EventHandler;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;

/**
 * An AMQP connection.
 *
 * This class is thread-safe.
 *
 * The other classes in this package are not thread-safe unless explicitly stated.
 * Obtain the {@link AmqpConnection} lock first to use them in a thread-safe
 * manner.
 *
 */
public class AmqpConnection
{
    private static Logger _logger = Logger.getLogger("qpid.jms-client.connection");

    /**
     * Default timeout in milliseconds.
     * TODO define a proper way for the timeout to be specified.
     */
    public static final long TIMEOUT = Long.getLong("org.apache.qpid.jms.connection.timeout", 10_000L);

    private final Connection _connection;
    private boolean _connected;
    private boolean _authenticationError;

    private SaslEngineFactory _saslEngineFactory = new SaslEngineFactoryImpl();
    private SaslEngine _saslEngine;

    private String _username;
    private String _password;

    private Collection<Session> _pendingSessions = new ArrayList<Session>();
    private Collection<Session> _pendingCloseSessions = new ArrayList<Session>();
    private Collection<Link> _pendingLinks = new ArrayList<Link>();
    private Collection<Link> _pendingCloseLinks = new ArrayList<Link>();

    private String _remoteHost;
    private int _port;

    private Sasl _sasl;

    private boolean _closed;

    private EventHandler _eventHandler = new AmqpConnectionEventHandler();

    public AmqpConnection(String clientName, String remoteHost, int port)
    {
        _remoteHost = remoteHost;
        _port = port;
        _connection = Connection.Factory.create();

        _connection.setContainer(clientName);
        _connection.setHostname(remoteHost);
        _connection.setContext(this);

        //This doesn't open the TCP connection, just changes the state
        _connection.open();
    }

    Connection getConnection()
    {
        return _connection;
    }

    String getRemoteHost()
    {
        return _remoteHost;
    }

    int getPort()
    {
        return _port;
    }

    public synchronized AmqpSession createSession()
    {
        Session session = _connection.session();

        AmqpSession amqpSession = new AmqpSession(this, session);
        session.setContext(amqpSession);
        session.open();

        addPendingSession(session);

        return amqpSession;
    }

    void addPendingSession(Session session)
    {
        _pendingSessions.add(session);
    }

    void addPendingCloseSession(Session session)
    {
        _pendingCloseSessions.add(session);
    }

    void addPendingLink(Link link)
    {
        _pendingLinks.add(link);
    }

    void addPendingCloseLink(Link link)
    {
        _pendingCloseLinks.add(link);
    }

    /**
     * @return the username
     */
    public synchronized String getUsername()
    {
        return _username;
    }

    /**
     * @param username the username to set
     */
    public synchronized void setUsername(String username)
    {
        _username = username;
    }

    /**
     * @return the password
     */
    public synchronized String getPassword()
    {
        return _password;
    }

    /**
     * @param password the password to set
     */
    public synchronized void setPassword(String password)
    {
        _password = password;
    }

    /**
     * For all the the "pending" AmqpXXX objects, update their state to reflect the remote state of their
     * Proton counterparts, and remove them from the pending set.
     *
     * The "pending" AmqpXXX objects are the ones that whose local modifications are expected to cause a
     * remote state change, e.g. newly created sessions.
     *
     * @return true if any AmqpXXX objects were updated by this method
     */
    public synchronized boolean process()
    {
        boolean updated = false;
        //Connection
        EndpointState connectionRemoteState = _connection.getRemoteState();
        if (!_connected && (connectionRemoteState == EndpointState.UNINITIALIZED))
        {
            if(_sasl != null)
            {
                updated |= processSasl();
            }
        }

        if (!_connected && (connectionRemoteState == EndpointState.ACTIVE))
        {
            _logger.log(Level.FINEST, "Set connected to true");
            updated = true;
            _connected = true;
        }

        if(_connected && (_connection.getLocalState() == EndpointState.CLOSED && connectionRemoteState == EndpointState.CLOSED))
        {
            _closed = true;
            _connected = false;
            updated = true;
        }

        notifyAll();
        return updated;
    }

    private Object getRemoteNode(Link link)
    {
        if(link instanceof Sender)
        {
            return  link.getRemoteTarget();
        }
        else if(link instanceof Receiver)
        {
            return link.getRemoteSource();
        }
        else
        {
            throw new IllegalArgumentException(String.format("%s is not a %s or a %s", link, Sender.class, Receiver.class));
        }
    }

    private boolean processSasl()
    {
        boolean updated = false;
        switch(_sasl.getState())
        {
            case PN_SASL_IDLE:
                String[] remoteMechanisms = _sasl.getRemoteMechanisms();
                if(remoteMechanisms != null && remoteMechanisms.length != 0)
                {
                    Map<String,Object> properties = new HashMap<String,Object>();
                    if(_username != null)
                    {
                        properties.put(SaslEngineFactory.USERNAME_PROPERTY, _username);
                    }
                    if(_password != null)
                    {
                        properties.put(SaslEngineFactory.PASSWORD_PROPERTY, _password);
                    }
                    _saslEngine = _saslEngineFactory.createSaslEngine(properties,remoteMechanisms);
                    if(_saslEngine == null)
                    {
                        _authenticationError = true;
                    }
                    else
                    {
                        _sasl.setMechanisms(_saslEngine.getMechanism());

                        byte[] initialResponse = _saslEngine.getResponse(new byte[0]);
                        if(initialResponse != null && initialResponse.length != 0)
                        {
                            _sasl.send(initialResponse, 0, initialResponse.length);
                        }
                    }
                    updated = true;
                }
                break;
            case PN_SASL_STEP:
                if(_sasl.pending() != 0)
                {
                    byte[] challenge = new byte[_sasl.pending()];
                    _sasl.recv(challenge, 0, challenge.length);
                    byte[] response = _saslEngine.getResponse(challenge);
                    _sasl.send(response,0,response.length);
                    updated = true;
                }
                break;
            case PN_SASL_FAIL:
                if(!_authenticationError)
                {
                    _authenticationError = true;
                    updated = true;
                }
                break;
            default:
        }
        return updated;
    }

    public synchronized void setSasl(Sasl sasl)
    {
        _sasl = sasl;
    }

    public synchronized boolean isConnected()
    {
        return _connected;
    }

    public synchronized ErrorCondition getConnectionError()
    {
        return _connection.getCondition();
    }

    public synchronized void close()
    {
        _connection.close();
        notifyAll();
    }

    public synchronized boolean isClosed()
    {
        return _closed;
    }

    public synchronized boolean isAuthenticationError()
    {
        return _authenticationError;
    }

    public EventHandler getEventHandler()
    {
        return _eventHandler;
    }

    private class AmqpConnectionEventHandler extends AbstractEventHandler
    {
        // == Session ==

        @Override
        public void onRemoteOpen(Session session)
        {
            if(session.getRemoteState() != EndpointState.UNINITIALIZED)
            {
                AmqpSession amqpSession = (AmqpSession) session.getContext();
                amqpSession.setEstablished();
                _pendingSessions.remove(session);//TODO: delete pending sessions?
            }
        };

        @Override
        public void onRemoteClose(Session session)
        {
            if(session.getRemoteState() == EndpointState.CLOSED)
            {
                AmqpSession amqpSession = (AmqpSession) session.getContext();
                amqpSession.setClosed();
                _pendingCloseSessions.remove(session);//TODO: delete pending close sessions?
            }
        }

        // == Link ==

        @Override
        public void onRemoteOpen(Link link)
        {
            if(link.getRemoteState() == EndpointState.ACTIVE)
            {
                AmqpLink amqpLink = (AmqpLink) link.getContext();
                if(getRemoteNode(link) != null)
                {
                    amqpLink.setEstablished();
                }
                else
                {
                    amqpLink.setLinkError();
                }
                _pendingLinks.remove(link);//TODO: delete pending links?
            }
        }

        @Override
        public void onRemoteClose(Link link)
        {
            if(link.getRemoteState() == EndpointState.CLOSED)
            {
                AmqpLink amqpLink = (AmqpLink) link.getContext();
                amqpLink.setClosed();
                _pendingCloseLinks.remove(link);//TODO: delete pending close links?
            }
        }
    }
}
