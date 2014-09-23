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
package org.apache.qpid.jms.provider.discovery;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.jms.provider.ProviderWrapper;
import org.apache.qpid.jms.provider.failover.FailoverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AsyncProvider instance that wraps the FailoverProvider and listens for
 * events about discovered remote peers using a configured DiscoveryAgent
 * instance.
 */
public class DiscoveryProvider extends ProviderWrapper<FailoverProvider> implements DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryProviderFactory.class);

    private final URI discoveryUri;
    private DiscoveryAgent discoveryAgent;
    private final ConcurrentHashMap<String, URI> serviceURIs = new ConcurrentHashMap<String, URI>();

    /**
     * Creates a new instance of the DiscoveryProvider.
     *
     * The Provider is created and initialized with the original URI used to create it,
     * and an instance of a FailoverProcider which it will use to initiate and maintain
     * connections to the discovered peers.
     *
     * @param discoveryUri
     * @param next
     */
    public DiscoveryProvider(URI discoveryUri, FailoverProvider next) {
        super(next);
        this.discoveryUri = discoveryUri;
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        if (this.discoveryAgent == null) {
            throw new IllegalStateException("No DiscoveryAgent configured.");
        }

        discoveryAgent.setDiscoveryListener(this);
        discoveryAgent.start();

        super.start();
    }

    @Override
    public void close() {
        discoveryAgent.close();
        super.close();
    }

    //------------------- Property Accessors ---------------------------------//

    /**
     * @return the original URI used to configure this DiscoveryProvider.
     */
    public URI getDiscoveryURI() {
        return this.discoveryUri;
    }

    /**
     * @return the configured DiscoveryAgent instance used by this DiscoveryProvider.
     */
    public DiscoveryAgent getDiscoveryAgent() {
        return this.discoveryAgent;
    }

    /**
     * Sets the discovery agent used by this provider to locate remote peer instance.
     *
     * @param agent
     *        the agent to use to discover remote peers
     */
    public void setDiscoveryAgent(DiscoveryAgent agent) {
        this.discoveryAgent = agent;
    }

    //------------------- Discovery Event Handlers ---------------------------//

    @Override
    public void onServiceAdd(DiscoveryEvent event) {
        String url = event.getPeerUri();
        if (url != null) {
            try {
                URI uri = new URI(url);
                LOG.info("Adding new peer connection URL: {}", uri);
                serviceURIs.put(event.getPeerUri(), uri);
                next.add(uri);
            } catch (URISyntaxException e) {
                LOG.warn("Could not add remote URI: {} due to bad URI syntax: {}", url, e.getMessage());
            }
        }
    }

    @Override
    public void onServiceRemove(DiscoveryEvent event) {
        URI uri = serviceURIs.get(event.getPeerUri());
        if (uri != null) {
            next.remove(uri);
        }
    }

    //------------------- Connection State Handlers --------------------------//

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        this.discoveryAgent.resume();
        super.onConnectionInterrupted(remoteURI);
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        this.discoveryAgent.suspend();
        super.onConnectionRestored(remoteURI);
    }
}
