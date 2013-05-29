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
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.proton.ProtonFactoryLoader;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.EngineFactory;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.message.MessageFactory;

@SuppressWarnings("rawtypes")
public class AmqpConnection
{
    public static final long TIMEOUT = 30000L;

    private static Logger _logger = Logger.getLogger("qpid.jms-client.connection");


    private static final ProtonFactoryLoader protonFactoryLoader = new ProtonFactoryLoader();

    private final EngineFactory _engineFactory;
    private final Connection _connection;
    private boolean _connected;
    private boolean _authenticationError;

    private SaslEngineFactory _saslEngineFactory = new SaslEngineFactoryImpl();
    private SaslEngine _saslEngine;

    private MessageFactory _messageFactory = defaultMessageFactory();

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

    public AmqpConnection(String clientName, String remoteHost, int port)
    {
        _remoteHost = remoteHost;
        _port = port;
        _engineFactory = defaultEngineFactory();
        _connection = _engineFactory.createConnection();

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
    public String getUsername()
    {
        return _username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username)
    {
        _username = username;
    }

    /**
     * @return the password
     */
    public String getPassword()
    {
        return _password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password)
    {
        _password = password;
    }

    public synchronized boolean process()
    {
        boolean updated = false;
        //Connection
        EndpointState remoteState = _connection.getRemoteState();
        if (!_connected && (remoteState == EndpointState.UNINITIALIZED))
        {
            if(_sasl != null)
            {
                updated |= processSasl();
            }
        }

        if (!_connected && (remoteState == EndpointState.ACTIVE))
        {
            _logger.log(Level.FINEST, "Set connected to true");
            updated = true;
            _connected = true;
        }

        if(_connected && (_connection.getLocalState() == EndpointState.CLOSED && remoteState == EndpointState.CLOSED))
        {
            _closed = true;
            _connected = false;
            updated = true;
        }

        //Sessions

        Iterator<Session> pendingSessions = _pendingSessions.iterator();
        Session s;
        while(pendingSessions.hasNext())
        {
            s = pendingSessions.next();
            if(s.getRemoteState() != EndpointState.UNINITIALIZED)
            {
                AmqpSession amqpSession = (AmqpSession) s.getContext();
                amqpSession.setEstablished();
                pendingSessions.remove();
                updated = true;
            }            
        }

        Iterator<Session> pendingCloseSessions = _pendingCloseSessions.iterator();
        while(pendingCloseSessions.hasNext())
        {
            s = pendingCloseSessions.next();
            if(s.getRemoteState() == EndpointState.CLOSED)
            {
                AmqpSession amqpSession = (AmqpSession) s.getContext();
                amqpSession.setClosed();
                pendingCloseSessions.remove();
                updated = true;
            }            
        }

        //Links
        Iterator<Link> pendingLinks = _pendingLinks.iterator();
        Link l;
        while(pendingLinks.hasNext())
        {
            l = pendingLinks.next();
            if(l.getRemoteState() != EndpointState.UNINITIALIZED)
            {
                AmqpLink amqpLink = (AmqpLink) l.getContext();
                amqpLink.setEstablished();
                pendingLinks.remove();
                updated = true;
            }
        }

        Iterator<Link> pendingCloseLinks = _pendingCloseLinks.iterator();
        while(pendingCloseLinks.hasNext())
        {
            l = pendingCloseLinks.next();
            if(l.getRemoteState() == EndpointState.CLOSED)
            {
                AmqpLink amqpLink = (AmqpLink) l.getContext();
                amqpLink.setClosed();
                pendingCloseLinks.remove();
                updated = true;
            }
        }

        notifyAll();
        return updated;
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


    MessageFactory getMessageFactory()
    {
        return _messageFactory;
    }

    @SuppressWarnings("unchecked")
    private static EngineFactory defaultEngineFactory()
    {
        return (EngineFactory) protonFactoryLoader.loadFactory(EngineFactory.class);
    }

    @SuppressWarnings("unchecked")
    private static MessageFactory defaultMessageFactory()
    {
        return (MessageFactory) protonFactoryLoader.loadFactory(MessageFactory.class);
    }

    public void setSasl(Sasl sasl)
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
}
