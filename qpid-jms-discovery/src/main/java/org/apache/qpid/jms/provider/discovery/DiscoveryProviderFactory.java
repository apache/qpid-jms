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

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.provider.failover.FailoverProvider;
import org.apache.qpid.jms.provider.failover.FailoverProviderFactory;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;
import org.apache.qpid.jms.util.URISupport.CompositeData;

/**
 * Factory for creating the Discovery Provider
 */
public class DiscoveryProviderFactory extends ProviderFactory {

    /**
     * Prefix used for all properties that apply specifically to the DiscoveryProvider
     */
    public static final String DISCOVERY_OPTION_PREFIX = "discovery.";

    /**
     * Prefix addon used for all properties that should be applied to any discovered remote URIs.
     */
    public static final String DISCOVERY_DISCOVERED_OPTION_PREFIX_ADON = "discovered.";

    @Override
    public Provider createProvider(URI remoteURI) throws Exception {

        CompositeData composite = URISupport.parseComposite(remoteURI);
        Map<String, String> options = composite.getParameters();

        // Gather failover and discovery options.
        Map<String, String> failoverOptions = PropertyUtil.filterProperties(options, FailoverProviderFactory.FAILOVER_OPTION_PREFIX);
        Map<String, String> failoverNestedOptions = PropertyUtil.filterProperties(failoverOptions, FailoverProviderFactory.FAILOVER_NESTED_OPTION_PREFIX_ADDON);

        Map<String, String> discoveryOptions = PropertyUtil.filterProperties(options, DISCOVERY_OPTION_PREFIX);
        Map<String, String> discoveredOptions = PropertyUtil.filterProperties(discoveryOptions, DISCOVERY_DISCOVERED_OPTION_PREFIX_ADON);

        // Combine the provider options, and the nested/discovered options.
        Map<String, String> mainOptions = new HashMap<String, String>();
        mainOptions.putAll(failoverOptions);
        mainOptions.putAll(discoveryOptions);

        Map<String, String> nestedOptions = new HashMap<String, String>();
        nestedOptions.putAll(failoverNestedOptions);
        nestedOptions.putAll(discoveredOptions);

        // Failover will apply the nested options to each URI while attempting to connect.
        FailoverProvider failover = new FailoverProvider(nestedOptions);
        Map<String, String> leftOverDiscoveryOptions = PropertyUtil.setProperties(failover, mainOptions);

        DiscoveryProvider discovery = new DiscoveryProvider(remoteURI, failover);
        Map<String, String> unusedOptions = PropertyUtil.setProperties(discovery, leftOverDiscoveryOptions);

        if (!unusedOptions.isEmpty()) {
            String msg = ""
                + " Not all options could be set on the Discovery provider."
                + " Check the options are spelled correctly."
                + " Unused parameters=[" + unusedOptions + "]."
                + " This Provider cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        List<URI> agentURIs = composite.getComponents();
        List<DiscoveryAgent> discoveryAgents = new ArrayList<DiscoveryAgent>(agentURIs.size());

        for (URI agentURI : agentURIs) {
            discoveryAgents.add(DiscoveryAgentFactory.createAgent(agentURI));
        }

        discovery.setDiscoveryAgents(discoveryAgents);

        return discovery;
    }

    @Override
    public String getName() {
        return "Discovery";
    }
}
