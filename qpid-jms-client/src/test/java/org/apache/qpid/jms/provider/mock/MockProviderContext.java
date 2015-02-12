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
package org.apache.qpid.jms.provider.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context shared between all MockProvider instances.
 */
public class MockProviderContext {

    public static MockProviderContext INSTANCE;

    private final Map<String, MockProvider> activeProviders = new ConcurrentHashMap<String, MockProvider>();
    private final MockProviderStats contextStats = new MockProviderStats();

    private MockProvider lastRegistered;
    private boolean offline;

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

    public void start() {
        contextStats.reset();
        activeProviders.clear();
        lastRegistered = null;
        offline = false;

        MockProviderContext.INSTANCE = this;
    }

    public void shutdown() {
        offline = true;
        List<MockProvider> active = new ArrayList<MockProvider>(activeProviders.values());
        for (MockProvider provider : active) {
            provider.signalConnectionFailed();
        }
        activeProviders.clear();
        lastRegistered = null;

        MockProviderContext.INSTANCE = null;
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
}
