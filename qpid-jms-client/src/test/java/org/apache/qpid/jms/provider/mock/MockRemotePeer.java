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
package org.apache.qpid.jms.provider.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.ProviderException;

/**
 * Context shared between all MockProvider instances.
 */
public class MockRemotePeer {

    public static MockRemotePeer INSTANCE;

    private final Map<String, MockProvider> activeProviders = new ConcurrentHashMap<String, MockProvider>();
    private final MockProviderStats contextStats = new MockProviderStats();

    private MockProvider lastRegistered;
    private JmsOutboundMessageDispatch lastRecordedMessage;

    private boolean offline;

    private ResourceLifecycleFilter createFilter;
    private ResourceLifecycleFilter startFilter;
    private ResourceLifecycleFilter stopFilter;
    private ResourceLifecycleFilter destroyFilter;

    private final Map<Destination, List<PendingCompletion>> pendingCompletions =
        new ConcurrentHashMap<Destination, List<PendingCompletion>>();

    public void connect(MockProvider provider) throws ProviderException {
        if (offline) {
            throw new ProviderException("Provider is offline");
        }

        if (provider != null) {
            activeProviders.put(provider.getProviderId(), provider);
            lastRegistered = provider;
        }
    }

    public void disconnect(MockProvider provider) {
        if (provider != null) {
            activeProviders.remove(provider.getProviderId());
        }
    }

    public void createResource(JmsResource resource) throws Exception {
        if (createFilter != null) {
            createFilter.onLifecycleEvent(resource);
        }
    }

    public void startResource(JmsResource resource) throws Exception {
        if (startFilter != null) {
            startFilter.onLifecycleEvent(resource);
        }
    }

    public void stopResource(JmsResource resource) throws Exception {
        if (stopFilter != null) {
            stopFilter.onLifecycleEvent(resource);
        }
    }

    public void destroyResource(JmsResource resource) throws Exception {
        if (destroyFilter != null) {
            destroyFilter.onLifecycleEvent(resource);
        }
    }

    public void start() {
        contextStats.reset();
        activeProviders.clear();
        lastRegistered = null;
        offline = false;

        MockRemotePeer.INSTANCE = this;
    }

    public void terminate() {
        shutdown();

        MockRemotePeer.INSTANCE = null;
    }

    public void shutdown() {
        offline = true;
        List<MockProvider> active = new ArrayList<MockProvider>(activeProviders.values());
        for (MockProvider provider : active) {
            provider.signalConnectionFailed();
        }
        activeProviders.clear();
        lastRegistered = null;
    }

    public void shutdownQuietly() {
        offline = true;
        activeProviders.clear();
        lastRegistered = null;
    }

    public void silentlyCloseConnectedProviders() {
        List<MockProvider> active = new ArrayList<MockProvider>(activeProviders.values());
        for (MockProvider provider : active) {
            provider.silentlyClose();
        }
    }

    public MockProvider getProvider(String providerId) {
        return activeProviders.get(providerId);
    }

    public MockProvider getLastRegistered() {
        return lastRegistered;
    }

    public MockProviderStats getContextStats() {
        return contextStats;
    }

    public void setResourceCreateFilter(ResourceLifecycleFilter filter) {
        createFilter = filter;
    }

    public void setResourceStartFilter(ResourceLifecycleFilter filter) {
        startFilter = filter;
    }

    public void setResourceStopFilter(ResourceLifecycleFilter filter) {
        stopFilter = filter;
    }

    public void setResourceDestroyFilter(ResourceLifecycleFilter filter) {
        destroyFilter = filter;
    }

    public JmsOutboundMessageDispatch getLastReceivedMessage() {
        return lastRecordedMessage;
    }

    //----- Record operations for connected providers ------------------------//

    public void recordSend(MockProvider provider, JmsOutboundMessageDispatch envelope) {
        this.lastRecordedMessage = envelope;
    }

    //----- Controls handling of Message Send Completions --------------------//

    public void recordPendingCompletion(MockProvider provider, JmsOutboundMessageDispatch envelope) {
        Destination destination = envelope.getDestination();
        if (!pendingCompletions.containsKey(destination)) {
            pendingCompletions.put(destination, new ArrayList<PendingCompletion>());
        }

        pendingCompletions.get(destination).add(new PendingCompletion(provider, envelope));
    }

    public void completeAllPendingSends(Destination destination) {
        if (pendingCompletions.containsKey(destination)) {

            for (List<PendingCompletion> pendingSends : pendingCompletions.values()) {
                for (PendingCompletion pending : pendingSends) {
                    pending.provider.getProviderListener().onCompletedMessageSend(pending.envelope);
                }
            }

            pendingCompletions.remove(destination);
        }
    }

    public void failAllPendingSends(Destination destination, ProviderException error) {
        if (pendingCompletions.containsKey(destination)) {

            for (List<PendingCompletion> pendingSends : pendingCompletions.values()) {
                for (PendingCompletion pending : pendingSends) {
                    pending.provider.getProviderListener().onFailedMessageSend(pending.envelope, error);
                }
            }

            pendingCompletions.remove(destination);
        }
    }

    public void completePendingSend(Message message) throws JMSException {
        List<PendingCompletion> pendingSends = pendingCompletions.get(message.getJMSDestination());
        Iterator<PendingCompletion> iterator = pendingSends.iterator();
        while (iterator.hasNext()) {
            PendingCompletion pending = iterator.next();
            if (pending.envelope.getMessage().getJMSMessageID().equals(message.getJMSMessageID())) {
                pending.provider.getProviderListener().onCompletedMessageSend(pending.envelope);
                iterator.remove();
            }
        }
    }

    public void completePendingSend(JmsOutboundMessageDispatch envelope) throws JMSException {
        List<PendingCompletion> pendingSends = pendingCompletions.get(envelope.getDestination());
        Iterator<PendingCompletion> iterator = pendingSends.iterator();
        while (iterator.hasNext()) {
            PendingCompletion pending = iterator.next();
            if (pending.envelope.getMessage().getJMSMessageID().equals(envelope.getMessage().getJMSMessageID())) {
                pending.provider.getProviderListener().onCompletedMessageSend(pending.envelope);
                iterator.remove();
            }
        }
    }

    public void failPendingSend(Message message, ProviderException error) throws JMSException {
        List<PendingCompletion> pendingSends = pendingCompletions.get(message.getJMSDestination());
        Iterator<PendingCompletion> iterator = pendingSends.iterator();
        while (iterator.hasNext()) {
            PendingCompletion pending = iterator.next();
            if (pending.envelope.getMessage().getJMSMessageID().equals(message.getJMSMessageID())) {
                pending.provider.getProviderListener().onFailedMessageSend(pending.envelope, error);
                iterator.remove();
            }
        }
    }

    public void failPendingSend(JmsOutboundMessageDispatch envelope, ProviderException error) throws JMSException {
        List<PendingCompletion> pendingSends = pendingCompletions.get(envelope.getDestination());
        Iterator<PendingCompletion> iterator = pendingSends.iterator();
        while (iterator.hasNext()) {
            PendingCompletion pending = iterator.next();
            if (pending.envelope.getMessage().getJMSMessageID().equals(envelope.getMessage().getJMSMessageID())) {
                pending.provider.getProviderListener().onFailedMessageSend(pending.envelope, error);
                iterator.remove();
            }
        }
    }

    public List<JmsOutboundMessageDispatch> getPendingCompletions(Destination destination) {
        List<JmsOutboundMessageDispatch> result = null;

        if (pendingCompletions.containsKey(destination)) {
            result = new ArrayList<JmsOutboundMessageDispatch>();
            List<PendingCompletion> pendingMessages = pendingCompletions.get(destination);
            for (PendingCompletion pending : pendingMessages) {
                result.add(pending.envelope);
            }
        } else {
            result = Collections.emptyList();
        }

        return result;
    }

    //----- Internal classes used for record state information ---------------//

    private class PendingCompletion {

        public final MockProvider provider;
        public final JmsOutboundMessageDispatch envelope;

        public PendingCompletion(MockProvider provider, JmsOutboundMessageDispatch envelope) {
            this.provider = provider;
            this.envelope = envelope;
        }
    }
}
