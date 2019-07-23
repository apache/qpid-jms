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
package org.apache.qpid.jms.provider.discovery.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.discovery.DiscoveryAgent;
import org.apache.qpid.jms.provider.discovery.DiscoveryListener;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.apache.qpid.jms.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovery agent that watches a file and periodically reads in remote URIs
 * from that file.
 */
public class FileWatcherDiscoveryAgent implements DiscoveryAgent {

    private static final Logger LOG = LoggerFactory.getLogger(FileWatcherDiscoveryAgent.class);

    private static final int DEFAULT_UPDATE_INTERVAL = 30000;

    private ScheduledExecutorService scheduler;
    private final Set<URI> discovered = new LinkedHashSet<URI>();

    private final URI discoveryURI;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private DiscoveryListener listener;
    private int updateInterval = DEFAULT_UPDATE_INTERVAL;
    private boolean warnOnWatchedReadError;

    public FileWatcherDiscoveryAgent(URI discoveryURI) throws URISyntaxException {
        this.discoveryURI = URISupport.removeQuery(discoveryURI);
    }

    @Override
    public void setDiscoveryListener(DiscoveryListener listener) {
        this.listener = listener;
    }

    public DiscoveryListener getDiscoveryListener() {
        return this.listener;
    }

    @Override
    public boolean isSchedulerRequired() {
        return true;
    }

    @Override
    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        if (listener == null) {
            throw new IllegalStateException("No DiscoveryListener configured.");
        }

        if (scheduler == null) {
            throw new IllegalStateException("No scheduler service has been provided.");
        }

        if (started.compareAndSet(false, true)) {
            scheduler.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    LOG.debug("Performing watched resources scheduled update: {}", getDiscvoeryURI());
                    updateWatchedResources();
                }
            }, 0, getUpdateInterval(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            ThreadPoolUtils.shutdownGraceful(scheduler);
        }
    }

    @Override
    public void suspend() {
        // We don't suspend watching the file updates are passive.
    }

    @Override
    public void resume() {
        // We don't suspend watching the file updates are passive.
    }

    @Override
    public String toString() {
        return "FileWatcherDiscoveryAgent: listener:" + getDiscvoeryURI();
    }

    // ---------- Property Accessors ------------------------------------------//

    /**
     * @return the original URI used to create the Discovery Agent.
     */
    public URI getDiscvoeryURI() {
        return this.discoveryURI;
    }

    /**
     * @return the configured resource update interval.
     */
    public int getUpdateInterval() {
        return updateInterval;
    }

    /**
     * @param updateInterval
     *        the update interval to use for watching resources for changes.
     */
    public void setUpdateInterval(int updateInterval) {
        this.updateInterval = updateInterval;
    }

    //----- Internal implementation ------------------------------------------//

    private void updateWatchedResources() {
        String fileURL = getDiscvoeryURI().toString();
        if (fileURL != null) {
            BufferedReader in = null;
            String newUris = null;
            StringBuffer buffer = new StringBuffer();

            try {
                in = new BufferedReader(getURLStream(fileURL));
                while (true) {
                    String line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    buffer.append(line);
                }
                newUris = buffer.toString();
            } catch (IOException ioe) {
                if (!warnOnWatchedReadError) {
                    LOG.warn("Failed to read watched resource: " + fileURL);
                    LOG.trace("Resource read error:", ioe);
                } else {
                    LOG.debug("Failed to read watched resource: " + fileURL);
                    LOG.trace("Resource read error:", ioe);
                }
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ioe) {
                        // ignore
                    }
                }
            }

            processURIs(newUris);
        }
    }

    private InputStreamReader getURLStream(String path) throws IOException {
        InputStreamReader result = null;
        URL url = null;

        try {
            url = new URL(path);
            result = new InputStreamReader(url.openStream());
        } catch (MalformedURLException e) {
            // ignore - it could be a path to a a local file
        }

        if (result == null) {
            result = new FileReader(path);
        }

        return result;
    }

    private final void processURIs(String updatedURIs) {
        if (updatedURIs != null) {
            updatedURIs = updatedURIs.trim();
            if (!updatedURIs.isEmpty()) {
                List<URI> list = new ArrayList<URI>();
                StringTokenizer tokenizer = new StringTokenizer(updatedURIs, ",");
                while (tokenizer.hasMoreTokens()) {
                    String str = tokenizer.nextToken();
                    try {
                        URI uri = new URI(str);
                        list.add(uri);
                    } catch (Exception e) {
                        LOG.error("Failed to parse broker address: " + str, e);
                    }
                }
                if (list.isEmpty() == false) {
                    try {
                        updateURIs(list);
                    } catch (IOException e) {
                        LOG.error("Failed to update transport URI's from: " + updatedURIs, e);
                    }
                }
            }
        }
    }

    private void updateURIs(List<URI> updates) throws IOException {

        // Remove any previously discovered URIs that are no longer in the watched resource
        HashSet<URI> removedPeers = new HashSet<URI>(discovered);
        removedPeers.removeAll(updates);

        for (URI removed : removedPeers) {
            listener.onServiceRemove(removed);
        }

        // Only add the newly discovered remote peers
        HashSet<URI> addedPeers = new HashSet<URI>(updates);
        addedPeers.removeAll(discovered);

        for (URI addition : addedPeers) {
            listener.onServiceAdd(addition);
        }

        // Now just store the new set of URIs and advertise them.
        discovered.clear();
        discovered.addAll(updates);
    }
}
