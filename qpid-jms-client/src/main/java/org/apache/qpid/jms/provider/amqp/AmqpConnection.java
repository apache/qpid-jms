/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSSecurityException;
import javax.jms.Session;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFactory;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sasl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AbstractAmqpResource<JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final AmqpJmsMessageFactory amqpMessageFactory;

    private final URI remoteURI;
    private final Map<JmsSessionId, AmqpSession> sessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsDestination, AmqpTemporaryDestination> tempDests = new HashMap<JmsDestination, AmqpTemporaryDestination>();
    private final AmqpProvider provider;
    private boolean connected;
    private AmqpSaslAuthenticator authenticator;
    private final AmqpSession connectionSession;

    private String queuePrefix;
    private String topicPrefix;
    private String tempQueuePrefix;
    private String tempTopicPrefix;

    public AmqpConnection(AmqpProvider provider, Connection protonConnection, Sasl sasl, JmsConnectionInfo info) {
        super(info, protonConnection);

        this.provider = provider;
        this.remoteURI = provider.getRemoteURI();
        this.amqpMessageFactory = new AmqpJmsMessageFactory(this);

        if (sasl != null) {
            this.authenticator = new AmqpSaslAuthenticator(sasl, info);
        }

        this.info.getConnectionId().setProviderHint(this);

        this.queuePrefix = info.getQueuePrefix();
        this.topicPrefix = info.getTopicPrefix();
        this.tempQueuePrefix = info.getTempQueuePrefix();
        this.tempTopicPrefix = info.getTempTopicPrefix();

        // Create a Session for this connection that is used for Temporary Destinations
        // and perhaps later on management and advisory monitoring.
        JmsSessionInfo sessionInfo = new JmsSessionInfo(this.info, -1);
        sessionInfo.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);

        this.connectionSession = new AmqpSession(this, sessionInfo);
    }

    @Override
    protected void doOpen() {
        this.endpoint.setContainer(info.getClientId());
        this.endpoint.setHostname(remoteURI.getHost());
    }

    @Override
    protected void doClose() {
    }

    public AmqpSession createSession(JmsSessionInfo sessionInfo) {
        AmqpSession session = new AmqpSession(this, sessionInfo);
        return session;
    }

    public AmqpTemporaryDestination createTemporaryDestination(JmsDestination destination) {
        AmqpTemporaryDestination temporary = new AmqpTemporaryDestination(connectionSession, destination);
        return temporary;
    }

    /**
     * Called on receiving an event from Proton indicating a state change on the remote
     * side of the Connection.
     */
    @Override
    public void processStateChange() {

        if (!connected && isOpen()) {
            connected = true;
            connectionSession.open(new AsyncResult() {

                @Override
                public boolean isComplete() {
                    return connected;
                }

                @Override
                public void onSuccess() {
                    LOG.debug("AMQP Connection Session opened.");
                    opened();
                }

                @Override
                public void onFailure(Throwable result) {
                    LOG.debug("AMQP Connection Session failed to open.");
                    failed(IOExceptionSupport.create(result));
                }
            });
        }

        EndpointState localState = endpoint.getLocalState();
        EndpointState remoteState = endpoint.getRemoteState();

        // We are still active (connected or not) and something on the remote end has
        // closed us, signal an error if one was sent.
        if (localState == EndpointState.ACTIVE && remoteState != EndpointState.ACTIVE) {
            if (endpoint.getRemoteCondition().getCondition() != null) {
                LOG.info("Error condition detected on Connection open {}.", endpoint.getRemoteCondition().getCondition());
                Exception remoteError = getRemoteError();
                if (isAwaitingOpen()) {
                    openRequest.onFailure(remoteError);
                } else {
                    provider.fireProviderException(remoteError);
                }
            }
        }

        // Transition cleanly to closed state.
        if (localState == EndpointState.CLOSED && remoteState == EndpointState.CLOSED) {
            LOG.debug("{} has been closed successfully.", this);
            closed();
        }
    }

    public void processSaslAuthentication() {
        if (connected || authenticator == null) {
            return;
        }

        try {
            if (authenticator.authenticate()) {
                authenticator = null;
            }
        } catch (JMSSecurityException ex) {
            failed(ex);
        }
    }

    void addTemporaryDestination(AmqpTemporaryDestination destination) {
        tempDests.put(destination.getJmsDestination(), destination);
    }

    void removeTemporaryDestination(AmqpTemporaryDestination destination) {
        tempDests.remove(destination.getJmsDestination());
    }

    void addSession(AmqpSession session) {
        this.sessions.put(session.getSessionId(), session);
    }

    void removeSession(AmqpSession session) {
        this.sessions.remove(session.getSessionId());
    }

    public JmsConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public Connection getProtonConnection() {
        return this.endpoint;
    }

    public URI getRemoteURI() {
        return this.remoteURI;
    }

    public String getUsername() {
        return this.info.getUsername();
    }

    public String getPassword() {
        return this.info.getPassword();
    }

    public AmqpProvider getProvider() {
        return this.provider;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getTempQueuePrefix() {
        return tempQueuePrefix;
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        this.tempQueuePrefix = tempQueuePrefix;
    }

    public String getTempTopicPrefix() {
        return tempTopicPrefix;
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        this.tempTopicPrefix = tempTopicPrefix;
    }

    /**
     * Retrieve the indicated Session instance from the list of active sessions.
     *
     * @param sessionId
     *        The JmsSessionId that's associated with the target session.
     *
     * @return the AmqpSession associated with the given id.
     */
    public AmqpSession getSession(JmsSessionId sessionId) {
        if (sessionId.getProviderHint() instanceof AmqpSession) {
            return (AmqpSession) sessionId.getProviderHint();
        }
        return this.sessions.get(sessionId);
    }

    /**
     * @return true if the provider has been configured for presettle operations.
     */
    public boolean isPresettleConsumers() {
        return provider.isPresettleConsumers();
    }

    /**
     * @return true if the provider has been configured for presettle operations.
     */
    public boolean isPresettleProducers() {
        return provider.isPresettleProducers();
    }

    /**
     * @return the AMQP based JmsMessageFactory for this Connection.
     */
    public AmqpJmsMessageFactory getAmqpMessageFactory() {
        return this.amqpMessageFactory;
    }

    @Override
    public String toString() {
        return "AmqpConnection { " + getConnectionInfo().getConnectionId() + " }";
    }
}

