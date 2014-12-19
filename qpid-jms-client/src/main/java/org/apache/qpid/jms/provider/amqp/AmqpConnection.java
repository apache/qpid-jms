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

import javax.jms.JMSException;
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

public class AmqpConnection extends AmqpAbstractResource<JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final AmqpJmsMessageFactory amqpMessageFactory;

    private final URI remoteURI;
    private final Map<JmsSessionId, AmqpSession> sessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsDestination, AmqpTemporaryDestination> tempDests = new HashMap<JmsDestination, AmqpTemporaryDestination>();
    private final AmqpProvider provider;
    private boolean connected;
    private AmqpSaslAuthenticator authenticator;
    private final AmqpConnectionSession connectionSession;
    private AmqpConnectionProperties properties;

    private String queuePrefix;
    private String topicPrefix;

    private boolean objectMessageUsesAmqpTypes = false;
    private boolean anonymousProducerCache = false;
    private int anonymousProducerCacheSize = 10;

    public AmqpConnection(AmqpProvider provider, Connection protonConnection, Sasl sasl, JmsConnectionInfo info) {
        super(info, protonConnection);

        this.provider = provider;
        this.remoteURI = provider.getRemoteURI();
        this.amqpMessageFactory = new AmqpJmsMessageFactory(this);

        if (sasl != null) {
            this.authenticator = new AmqpSaslAuthenticator(sasl, info);
        }

        this.resource.getConnectionId().setProviderHint(this);

        this.queuePrefix = info.getQueuePrefix();
        this.topicPrefix = info.getTopicPrefix();

        // Create a Session for this connection that is used for Temporary Destinations
        // and perhaps later on management and advisory monitoring.
        JmsSessionInfo sessionInfo = new JmsSessionInfo(this.resource, -1);
        sessionInfo.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);

        this.connectionSession = new AmqpConnectionSession(this, sessionInfo);
    }

    @Override
    protected void doOpen() {
        getEndpoint().setContainer(resource.getClientId());
        getEndpoint().setHostname(remoteURI.getHost());
        super.doOpen();
    }

    public AmqpSession createSession(JmsSessionInfo sessionInfo) {
        AmqpSession session = new AmqpSession(this, sessionInfo);
        return session;
    }

    public AmqpTemporaryDestination createTemporaryDestination(JmsDestination destination) {
        AmqpTemporaryDestination temporary = new AmqpTemporaryDestination(connectionSession, destination);
        return temporary;
    }

    public void unsubscribe(String subscriptionName, AsyncResult request) {

        for (AmqpSession session : sessions.values()) {
            if (session.containsSubscription(subscriptionName)) {
                request.onFailure(new JMSException("Cannot remove an active durable subscription"));
                return;
            }
        }

        connectionSession.unsubscribe(subscriptionName, request);
    }

    /**
     * Called on receiving an event from Proton indicating a state change on the remote
     * side of the Connection.
     */
    @Override
    public void processStateChange() {

        if (!connected && isOpen()) {
            connected = true;

            this.properties = new AmqpConnectionProperties(
                getEndpoint().getRemoteOfferedCapabilities(), getEndpoint().getRemoteProperties());

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

        EndpointState localState = getEndpoint().getLocalState();
        EndpointState remoteState = getEndpoint().getRemoteState();

        // We are still active (connected or not) and something on the remote end has
        // closed us, signal an error if one was sent.
        if (localState == EndpointState.ACTIVE && remoteState != EndpointState.ACTIVE) {
            if (getEndpoint().getRemoteCondition().getCondition() != null) {
                LOG.info("Error condition detected on Connection open {}.", getEndpoint().getRemoteCondition().getCondition());
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
        return this.resource;
    }

    public Connection getProtonConnection() {
        return this.getEndpoint();
    }

    public URI getRemoteURI() {
        return this.remoteURI;
    }

    public String getUsername() {
        return this.resource.getUsername();
    }

    public String getPassword() {
        return this.resource.getPassword();
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
     * @return true if anonymous producers should be cached or closed on send complete.
     */
    public boolean isAnonymousProducerCache() {
        return anonymousProducerCache;
    }

    /**
     * @param anonymousProducerCache
     *        enable or disables the caching or anonymous producers.
     */
    public void setAnonymousProducerCache(boolean anonymousProducerCache) {
        this.anonymousProducerCache = anonymousProducerCache;
    }

    /**
     * @return the number of anonymous producers stored in each cache.
     */
    public int getAnonymousProducerCacheSize() {
        return anonymousProducerCacheSize;
    }

    /**
     * @param anonymousProducerCacheSize
     *        the number of producers each anonymous producer instance will cache.
     */
    public void setAnonymousProducerCacheSize(int anonymousProducerCacheSize) {
        this.anonymousProducerCacheSize = anonymousProducerCacheSize;
    }

    /**
     * @return true if new ObjectMessage instance should default to using AMQP Typed bodies.
     */
    public boolean isObjectMessageUsesAmqpTypes() {
        return objectMessageUsesAmqpTypes;
    }

    /**
     * Configures the body type used in ObjectMessage instances that are sent from
     * this connection.
     *
     * @param objectMessageUsesAmqpTypes
     *        the objectMessageUsesAmqpTypes value to set.
     */
    public void setObjectMessageUsesAmqpTypes(boolean objectMessageUsesAmqpTypes) {
        this.objectMessageUsesAmqpTypes = objectMessageUsesAmqpTypes;
    }

    /**
     * @return the AMQP based JmsMessageFactory for this Connection.
     */
    public AmqpJmsMessageFactory getAmqpMessageFactory() {
        return this.amqpMessageFactory;
    }

    /**
     * Returns the connection properties for an established connection which defines the various
     * capabilities and configuration options of the remote connection.  Prior to the establishment
     * of a connection this method returns null.
     *
     * @return the properties available for this connection or null if not connected.
     */
    public AmqpConnectionProperties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "AmqpConnection { " + getConnectionInfo().getConnectionId() + " }";
    }
}
