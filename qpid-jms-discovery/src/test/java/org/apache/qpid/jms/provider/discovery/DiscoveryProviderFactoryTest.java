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
package org.apache.qpid.jms.provider.discovery;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.failover.FailoverProvider;
import org.apache.qpid.jms.provider.failover.FailoverProviderFactory;
import org.junit.Test;

public class DiscoveryProviderFactoryTest {

    @Test
    public void testCreateDiscoveryProvider() throws Exception {
        URI discoveryUri = new URI("discovery:(multicast://default)");
        Provider provider = DiscoveryProviderFactory.create(discoveryUri);

        assertNotNull("Provider was not created", provider);
        assertEquals("Provider was not of expected type", DiscoveryProvider.class, provider.getClass());

        DiscoveryProvider discovery = (DiscoveryProvider) provider;

        assertNotNull("Next provider was not present", discovery.getNext());
        assertEquals("Next Provider was not of expected type", FailoverProvider.class, discovery.getNext().getClass());

        FailoverProvider failoverProvider = (FailoverProvider) discovery.getNext();
        assertTrue("Expected no nested options", failoverProvider.getNestedOptions().isEmpty());
    }

    @Test
    public void testCreateDiscoveryProviderWithFailoverSyntaxMainOption() throws Exception {
        String optionPrefix = FailoverProviderFactory.FAILOVER_OPTION_PREFIX;
        assertEquals("Unexpected option prefix", "failover.", optionPrefix);
        doCreateDiscoveryProviderWithMainOptionTestImpl(optionPrefix);
    }

    @Test
    public void testCreateDiscoveryProviderWithDiscoverSyntaxMainOption() throws Exception {
        String optionPrefix = DiscoveryProviderFactory.DISCOVERY_OPTION_PREFIX;
        assertEquals("Unexpected option prefix", "discovery.", optionPrefix);
        doCreateDiscoveryProviderWithMainOptionTestImpl(optionPrefix);
    }

    private void doCreateDiscoveryProviderWithMainOptionTestImpl(String optionPrefix) throws URISyntaxException, Exception {
        String optionKey = "reconnectBackOffMultiplier";
        double option = 3.14159;
        String optionValue = String.valueOf(option);

        assertFalse(String.valueOf(FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER).equals(optionValue));

        URI discoveryUri = new URI("discovery:(multicast://default)?" + optionPrefix  + optionKey + "=" + optionValue);
        Provider provider = DiscoveryProviderFactory.create(discoveryUri);

        assertNotNull("Provider was not created", provider);
        assertEquals("Provider was not of expected type", DiscoveryProvider.class, provider.getClass());

        DiscoveryProvider discovery = (DiscoveryProvider) provider;

        assertNotNull("Next provider was not present", discovery.getNext());
        assertEquals("Next Provider was not of expected type", FailoverProvider.class, discovery.getNext().getClass());

        FailoverProvider failoverProvider = (FailoverProvider) discovery.getNext();

        assertEquals("option not as expected", option, failoverProvider.getReconnectBackOffMultiplier(), 0.0);
    }

    @Test
    public void testCreateDiscoveryProviderWithFailoverSyntaxNestedOptions() throws Exception {
        String optionPrefix = FailoverProviderFactory.FAILOVER_OPTION_PREFIX + FailoverProviderFactory.FAILOVER_NESTED_OPTION_PREFIX_ADDON;
        assertEquals("Unexpected option prefix", "failover.nested.", optionPrefix);
        doCreateDiscoveryProviderWithNestedOptionsTestImpl(optionPrefix);
    }

    @Test
    public void testCreateDiscoveryProviderWithDiscoveredSyntaxNestedOption() throws Exception {
        String optionPrefix = DiscoveryProviderFactory.DISCOVERY_OPTION_PREFIX + DiscoveryProviderFactory.DISCOVERY_DISCOVERED_OPTION_PREFIX_ADON;
        assertEquals("Unexpected option prefix", "discovery.discovered.", optionPrefix);
        doCreateDiscoveryProviderWithNestedOptionsTestImpl(optionPrefix);
    }

    private void doCreateDiscoveryProviderWithNestedOptionsTestImpl(String optionPrefix) throws URISyntaxException, Exception {
        String clientIdOptionKey = "jms.clientID";
        String clientIdValue = "myTestClientID";
        URI discoveryUri = new URI("discovery:(multicast://default)?" + optionPrefix  + clientIdOptionKey + "=" + clientIdValue);
        Provider provider = DiscoveryProviderFactory.create(discoveryUri);

        assertNotNull("Provider was not created", provider);
        assertEquals("Provider was not of expected type", DiscoveryProvider.class, provider.getClass());

        DiscoveryProvider discovery = (DiscoveryProvider) provider;

        assertNotNull("Next provider was not present", discovery.getNext());
        assertEquals("Next Provider was not of expected type", FailoverProvider.class, discovery.getNext().getClass());

        FailoverProvider failoverProvider = (FailoverProvider) discovery.getNext();
        failoverProvider.getNestedOptions();

        assertEquals("Expected nested options", 1, failoverProvider.getNestedOptions().size());
        assertTrue("Expected nested clientID option to be present", failoverProvider.getNestedOptions().containsKey(clientIdOptionKey));
        assertEquals("nested clientID option not as expected", clientIdValue, failoverProvider.getNestedOptions().get(clientIdOptionKey));
    }

    @Test
    public void testCreateDiscoveryProviderWithFailoverSyntaxUnusedMainOption() throws Exception {
        doCreateDiscoveryProviderWithUnusedMainOptionTestImpl(FailoverProviderFactory.FAILOVER_OPTION_PREFIX);
    }

    @Test
    public void testCreateDiscoveryProviderWithDiscoverSyntaxUnusedMainOption() throws Exception {
        doCreateDiscoveryProviderWithUnusedMainOptionTestImpl(DiscoveryProviderFactory.DISCOVERY_OPTION_PREFIX);
    }

    private void doCreateDiscoveryProviderWithUnusedMainOptionTestImpl(String optionPrefix) throws URISyntaxException, Exception {
        URI discoveryUri = new URI("discovery:(multicast://default)?" + optionPrefix  + "unusedOption=something");

        try {
            DiscoveryProviderFactory.create(discoveryUri);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }
}
