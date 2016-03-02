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
package org.apache.qpid.jms.provider.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that the provider factory correctly creates and configures the provider.
 */
public class FiloverProviderFactoryTest extends QpidJmsTestCase {

    private URI baseURI;
    private final FailoverProviderFactory factory = new FailoverProviderFactory();
    private FailoverProvider provider;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        baseURI = new URI("failover:(amqp://localhost:5672,amqp://localhost:5674)");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testCreateProvider() throws Exception {
        assertNotNull(factory.getName());
        Provider provider = factory.createProvider(baseURI);
        assertNotNull(provider);
        assertTrue(provider instanceof FailoverProvider);
    }

    @Test(timeout = 60000)
    public void testCreateProviderInitializesToDefaults() throws Exception {
        Provider provider = factory.createProvider(baseURI);
        assertNotNull(provider);
        assertTrue(provider instanceof FailoverProvider);

        FailoverProvider failover = (FailoverProvider) provider;

        assertEquals(FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY, failover.getInitialReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_RECONNECT_DELAY, failover.getReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY, failover.getMaxReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS, failover.getStartupMaxReconnectAttempts());
        assertEquals(FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS, failover.getMaxReconnectAttempts());
        assertEquals(FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF, failover.isUseReconnectBackOff());
        assertEquals(FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER, failover.getReconnectBackOffMultiplier(), 0.0);
        assertEquals(FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS, failover.getWarnAfterReconnectAttempts());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, failover.isRandomize());
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testCreateProviderWithUnknownOption() throws Exception {
        URI badURI = new URI(baseURI.toString() + "?failover.unknown=true");
        factory.createProvider(badURI);
    }

    @Test(timeout = 60000)
    public void testCreateWithOptions() throws Exception {
        URI configured = new URI(baseURI.toString() +
            "?failover.initialReconnectDelay=" + (FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY + 1) +
            "&failover.reconnectDelay=" + (FailoverProvider.DEFAULT_RECONNECT_DELAY + 2) +
            "&failover.maxReconnectDelay=" + (FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY + 3) +
            "&failover.startupMaxReconnectAttempts=" + (FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS + 4) +
            "&failover.maxReconnectAttempts=" + (FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS + 5) +
            "&failover.warnAfterReconnectAttempts=" + (FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS + 6) +
            "&failover.useReconnectBackOff=" + (!FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF) +
            "&failover.reconnectBackOffMultiplier=" + (FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER + 1.0d) +
            "&failover.randomize=" + (!FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED));

        Provider provider = factory.createProvider(configured);
        assertNotNull(provider);
        assertTrue(provider instanceof FailoverProvider);

        FailoverProvider failover = (FailoverProvider) provider;

        assertEquals(FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY + 1, failover.getInitialReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_RECONNECT_DELAY + 2, failover.getReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY + 3, failover.getMaxReconnectDelay());
        assertEquals(FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS + 4, failover.getStartupMaxReconnectAttempts());
        assertEquals(FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS + 5, failover.getMaxReconnectAttempts());
        assertEquals(FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS + 6, failover.getWarnAfterReconnectAttempts());
        assertEquals(!FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF, failover.isUseReconnectBackOff());
        assertEquals(FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER + 1.0d, failover.getReconnectBackOffMultiplier(), 0.0);
        assertEquals(!FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, failover.isRandomize());
    }

    @Test(timeout = 60000)
    public void testNestedOptionsArePassedAlong() throws Exception {
        URI configured = new URI(baseURI.toString() +
            "?failover.nested.transport.tcpNoDelay=true&failover.randomize=false");

        Provider provider = factory.createProvider(configured);
        assertNotNull(provider);
        assertTrue(provider instanceof FailoverProvider);

        FailoverProvider failover = (FailoverProvider) provider;
        assertFalse(failover.isRandomize());

        Map<String, String> nested = failover.getNestedOptions();
        assertNotNull(nested);
        assertFalse(nested.isEmpty());
        assertEquals(1, nested.size());

        assertEquals("true", nested.get("transport.tcpNoDelay"));
    }
}
