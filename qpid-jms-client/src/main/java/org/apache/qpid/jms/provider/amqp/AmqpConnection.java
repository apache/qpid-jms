/*
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.builders.AmqpSessionBuilder;
import org.apache.qpid.jms.provider.amqp.builders.AmqpTemporaryDestinationBuilder;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFactory;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRemotelyClosedException;
import org.apache.qpid.proton.engine.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AmqpAbstractResource<JmsConnectionInfo, Connection> implements AmqpResourceParent {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private AmqpSubscriptionTracker subTracker = new AmqpSubscriptionTracker();

    private final AmqpJmsMessageFactory amqpMessageFactory;

    private final URI remoteURI;
    private final Map<JmsSessionId, AmqpSession> sessions = new HashMap<JmsSessionId, AmqpSession>();
    private final Map<JmsDestination, AmqpTemporaryDestination> tempDests = new HashMap<JmsDestination, AmqpTemporaryDestination>();
    private final AmqpProvider provider;
    private final AmqpConnectionProperties properties;
    private AmqpConnectionSession connectionSession;

    private boolean objectMessageUsesAmqpTypes = false;
    private boolean anonymousProducerCache = false;
    private int anonymousProducerCacheSize = 10;

    public AmqpConnection(AmqpProvider provider, JmsConnectionInfo info, Connection protonConnection) {
        super(info, protonConnection, provider);

        this.provider = provider;
        this.remoteURI = provider.getRemoteURI();
        this.amqpMessageFactory = new AmqpJmsMessageFactory(this);

        // Create connection properties initialized with defaults from the JmsConnectionInfo
        this.properties = new AmqpConnectionProperties(info, provider);
    }

    public void createSession(JmsSessionInfo sessionInfo, AsyncResult request) {
        AmqpSessionBuilder builder = new AmqpSessionBuilder(this, sessionInfo);
        builder.buildResource(request);
    }

    public void createTemporaryDestination(JmsTemporaryDestination destination, AsyncResult request) {
        AmqpTemporaryDestinationBuilder builder = new AmqpTemporaryDestinationBuilder(connectionSession, destination);
        builder.buildResource(request);
    }

    public AmqpTemporaryDestination getTemporaryDestination(JmsTemporaryDestination destination) {
        return tempDests.get(destination);
    }

    public void unsubscribe(String subscriptionName, AsyncResult request) {
        // Check if there is an active (i.e open subscriber) shared or exclusive durable subscription using this name
        if (subTracker.isActiveDurableSub(subscriptionName)) {
            request.onFailure(new ProviderException("Can't remove an active durable subscription: " + subscriptionName));
            return;
        }

        boolean hasClientID = getResourceInfo().isExplicitClientID();

        connectionSession.unsubscribe(subscriptionName, hasClientID, request);
    }

    @Override
    public void addChildResource(AmqpResource resource) {
        if (resource instanceof AmqpConnectionSession) {
            connectionSession = (AmqpConnectionSession) resource;
        } else if (resource instanceof AmqpSession) {
            AmqpSession session = (AmqpSession) resource;
            sessions.put(session.getSessionId(), session);
        } else if (resource instanceof AmqpTemporaryDestination) {
            AmqpTemporaryDestination tempDest = (AmqpTemporaryDestination) resource;
            tempDests.put(tempDest.getResourceInfo(), tempDest);
        }
    }

    @Override
    public void removeChildResource(AmqpResource resource) {
        if (resource instanceof AmqpSession) {
            AmqpSession session = (AmqpSession) resource;
            sessions.remove(session.getSessionId());
        } else if (resource instanceof AmqpTemporaryDestination) {
            AmqpTemporaryDestination tempDest = (AmqpTemporaryDestination) resource;
            tempDests.remove(tempDest.getResourceInfo());
        }
    }

    @Override
    public void handleResourceClosure(AmqpProvider provider, ProviderException cause) {
        if (connectionSession != null) {
            connectionSession.handleResourceClosure(getProvider(), cause);
        }

        List<AmqpSession> sessionList = new ArrayList<>(sessions.values());
        for (AmqpSession session : sessionList) {
            session.handleResourceClosure(provider, cause);
        }

        List<AmqpTemporaryDestination> tempDestsList = new ArrayList<>(tempDests.values());
        for (AmqpTemporaryDestination tempDest : tempDestsList) {
            tempDest.handleResourceClosure(provider, cause);
        }
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws ProviderException {
        getResourceInfo().setState(ResourceState.REMOTELY_CLOSED);

        if (isAwaitingClose()) {
            closeResource(provider, null, true); // Close was expected so ignore any endpoint errors.
        } else {
            // This will create a fatal level exception that stops the provider possibly triggering reconnect
            ProviderConnectionRemotelyClosedException cause = AmqpSupport.convertToConnectionClosedException(
                provider, getEndpoint(), getEndpoint().getRemoteCondition());

            closeResource(provider, cause, true);
        }
    }

    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public AmqpProvider getProvider() {
        return provider;
    }

    public String getQueuePrefix() {
        return properties.getQueuePrefix();
    }

    public void setQueuePrefix(String queuePrefix) {
        properties.setQueuePrefix(queuePrefix);
    }

    public String getTopicPrefix() {
        return properties.getTopicPrefix();
    }

    public void setTopicPrefix(String topicPrefix) {
        properties.setTopicPrefix(topicPrefix);
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
        return sessions.get(sessionId);
    }

    /**
     * Retrieves the AmqpConnectionSession owned by this AmqpConnection.
     *
     * @return the AmqpConnectionSession owned by this AmqpConnection.
     */
    public AmqpConnectionSession getConnectionSession() {
        return connectionSession;
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
        return amqpMessageFactory;
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

    public AmqpSubscriptionTracker getSubTracker() {
        return subTracker;
    }

    /**
     * Allows a connection resource to schedule a task for future execution.
     *
     * @param task
     *      The Runnable task to be executed after the given delay.
     * @param delay
     *      The delay in milliseconds to schedule the given task for execution.
     *
     * @return a ScheduledFuture instance that can be used to cancel the task.
     */
    public ScheduledFuture<?> schedule(final Runnable task, long delay) {
        if (task == null) {
            LOG.trace("Resource attempted to schedule a null task.");
            return null;
        }

        return getProvider().getScheduler().schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "AmqpConnection { " + getResourceInfo().getId() + " }";
    }
}
