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
package org.apache.qpid.jms.provider.failover;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import org.apache.qpid.jms.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the list of available failover URIs that are used to connect
 * and recover a connection.
 */
public class FailoverUriPool {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverUriPool.class);

    private final LinkedList<URI> uris;
    private final Map<String, String> nestedOptions;
    private boolean randomize;

    public FailoverUriPool() {
        this.uris = new LinkedList<URI>();
        this.nestedOptions = Collections.emptyMap();
    }

    public FailoverUriPool(URI[] uris, Map<String, String> nestedOptions) {
        this.uris = new LinkedList<URI>();
        if (nestedOptions != null) {
            this.nestedOptions = nestedOptions;
        } else {
            this.nestedOptions = Collections.emptyMap();
        }

        if (uris != null) {
            for (URI uri : uris) {
                this.add(uri);
            }
        }
    }

    /**
     * Returns the next URI in the pool of URIs.  The URI will be shifted to the
     * end of the list and not be attempted again until the full list has been
     * returned once.
     *
     * @return the next URI that should be used for a connection attempt.
     */
    public URI getNext() {
        URI next = null;
        if (!uris.isEmpty()) {
            next = uris.removeFirst();
            uris.addLast(next);
        }

        return next;
    }

    /**
     * Reports that the Failover Provider connected to the last URI returned from
     * this pool.  If the Pool is set to randomize this will result in the Pool of
     * URIs being shuffled in preparation for the next connect cycle.
     */
    public void connected() {
        if (isRandomize()) {
            Collections.shuffle(uris);
        }
    }

    /**
     * @return true if this pool returns the URI values in random order.
     */
    public boolean isRandomize() {
        return randomize;
    }

    /**
     * Sets whether the URIs that are returned by this pool are returned in random
     * order or not.  If false the URIs are returned in FIFO order.
     *
     * @param randomize
     *        true to have the URIs returned in a random order.
     */
    public void setRandomize(boolean randomize) {
        this.randomize = randomize;
        if (randomize) {
            Collections.shuffle(uris);
        }
    }

    /**
     * Adds a new URI to the pool if not already contained within.  The URI will have
     * any nest options that have been configured added to its existing set of options.
     *
     * @param uri
     *        The new URI to add to the pool.
     */
    public void add(URI uri) {
        if (!contains(uri)) {
            if (!nestedOptions.isEmpty()) {
                try {
                    URISupport.applyParameters(uri, nestedOptions);
                } catch (URISyntaxException e) {
                    LOG.debug("Failed to add nested options to uri: {}", uri);
                }
            }

            this.uris.add(uri);
        }
    }

    /**
     * Remove a URI from the pool if present, otherwise has no effect.
     *
     * @param uri
     *        The URI to attempt to remove from the pool.
     */
    public void remove(URI uri) {
        this.uris.remove(uri);
    }

    /**
     * Returns the currently set value for nested options which will be added to each
     * URI that is returned from the pool.
     *
     * @return the Map instance containing the nest options which can be empty if none set.
     */
    public Map<String, String> getNestedOptions() {
        return nestedOptions;
    }

    private boolean contains(URI newURI) {
        boolean result = false;
        for (URI uri : uris) {
            if (compareURIs(newURI, uri)) {
                result = true;
                break;
            }
        }

        return result;
    }

    private boolean compareURIs(final URI first, final URI second) {
        boolean result = false;
        if (first == null || second == null) {
            return result;
        }

        if (first.getPort() == second.getPort()) {
            InetAddress firstAddr = null;
            InetAddress secondAddr = null;
            try {
                firstAddr = InetAddress.getByName(first.getHost());
                secondAddr = InetAddress.getByName(second.getHost());

                if (firstAddr.equals(secondAddr)) {
                    result = true;
                }
            } catch(IOException e) {
                if (firstAddr == null) {
                    LOG.error("Failed to Lookup INetAddress for URI[ " + first + " ] : " + e);
                } else {
                    LOG.error("Failed to Lookup INetAddress for URI[ " + second + " ] : " + e);
                }

                if (first.getHost().equalsIgnoreCase(second.getHost())) {
                    result = true;
                }
            }
        }

        return result;
    }
}
