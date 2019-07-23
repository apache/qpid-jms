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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderWrapper;
import org.apache.qpid.jms.provider.failover.FailoverProvider;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Provider instance that wraps the FailoverProvider and listens for events
 * about discovered remote peers using a configured set of DiscoveryAgent instance.
 */
public class DiscoveryProvider extends ProviderWrapper<FailoverProvider> implements DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryProviderFactory.class);

    private final URI discoveryUri;
    private final List<DiscoveryAgent> discoveryAgents = new ArrayList<DiscoveryAgent>();

    private ScheduledExecutorService sharedScheduler;

    /**
     * Creates a new instance of the DiscoveryProvider.
     *
     * The Provider is created and initialized with the original URI used to create it,
     * and an instance of a FailoverProcider which it will use to initiate and maintain
     * connections to the discovered peers.
     *
     * @param discoveryUri
     *      The URI that configures the discovery provider.
     * @param next
     *      The FailoverProvider that will be used to manage connections.
     */
    public DiscoveryProvider(URI discoveryUri, FailoverProvider next) {
        super(next);
        this.discoveryUri = discoveryUri;
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        if (discoveryAgents.isEmpty()) {
            throw new IllegalStateException("No DiscoveryAgent configured.");
        }

        for (DiscoveryAgent discoveryAgent : discoveryAgents) {
            discoveryAgent.setDiscoveryListener(this);
            if (discoveryAgent.isSchedulerRequired()) {
                discoveryAgent.setScheduler(getSharedScheduler());
            }
            discoveryAgent.start();
        }

        super.start();
    }

    @Override
    public void close() {
        ThreadPoolUtils.shutdownGraceful(sharedScheduler);

        for (DiscoveryAgent discoveryAgent : discoveryAgents) {
            discoveryAgent.close();
        }

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
     * @return a list of configured DiscoveryAgent instances used by this DiscoveryProvider.
     */
    public List<DiscoveryAgent> getDiscoveryAgents() {
        return Collections.unmodifiableList(discoveryAgents);
    }

    /**
     * Sets the discovery agents used by this provider to locate remote peer instance.
     *
     * @param agents
     *        the agents to use to discover remote peers
     */
    public void setDiscoveryAgents(List<DiscoveryAgent> agents) {
        discoveryAgents.addAll(agents);
    }

    //------------------- Discovery Event Handlers ---------------------------//

    @Override
    public void onServiceAdd(URI remoteURI) {
        if (remoteURI != null) {
            LOG.debug("Adding URI of remote peer: {}", remoteURI);
            next.add(remoteURI);
        }
    }

    @Override
    public void onServiceRemove(URI remoteURI) {
        if (remoteURI != null) {
            LOG.debug("Removing URI of remote peer: {}", remoteURI);
            next.remove(remoteURI);
        }
    }

    //------------------- Connection State Handlers --------------------------//

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        for (DiscoveryAgent discoveryAgent : discoveryAgents) {
            discoveryAgent.resume();
        }

        super.onConnectionInterrupted(remoteURI);
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        for (DiscoveryAgent discoveryAgent : discoveryAgents) {
            discoveryAgent.suspend();
        }

        super.onConnectionRestored(remoteURI);
    }

    //----- Internal implementation ------------------------------------------//

    private ScheduledExecutorService getSharedScheduler() {
        if (sharedScheduler == null) {
            sharedScheduler = Executors.newSingleThreadScheduledExecutor(
                new QpidJMSThreadFactory("DiscoveryProvider :[" + getDiscoveryURI() + "]", true));
        }

        return sharedScheduler;
    }
}
