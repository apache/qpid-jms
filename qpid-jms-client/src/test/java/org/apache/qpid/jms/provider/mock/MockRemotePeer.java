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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.jms.meta.JmsResource;

/**
 * Context shared between all MockProvider instances.
 */
public class MockRemotePeer {

    public static MockRemotePeer INSTANCE;

    private final Map<String, MockProvider> activeProviders = new ConcurrentHashMap<String, MockProvider>();
    private final MockProviderStats contextStats = new MockProviderStats();

    private MockProvider lastRegistered;
    private boolean offline;

    private ResourceLifecycleFilter createFilter;
    private ResourceLifecycleFilter startFilter;
    private ResourceLifecycleFilter stopFilter;
    private ResourceLifecycleFilter destroyFilter;

    public void connect(MockProvider provider) throws IOException {
        if (offline) {
            throw new IOException();
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

    public void shutdown() {
        offline = true;
        List<MockProvider> active = new ArrayList<MockProvider>(activeProviders.values());
        for (MockProvider provider : active) {
            provider.signalConnectionFailed();
        }
        activeProviders.clear();
        lastRegistered = null;

        MockRemotePeer.INSTANCE = null;
    }

    public void shutdownQuietly() {
        offline = true;
        activeProviders.clear();
        lastRegistered = null;

        MockRemotePeer.INSTANCE = null;
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
}
