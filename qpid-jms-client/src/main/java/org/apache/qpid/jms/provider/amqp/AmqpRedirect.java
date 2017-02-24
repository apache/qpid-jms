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
package org.apache.qpid.jms.provider.amqp;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PATH;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SCHEME;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.transports.TransportFactory;
import org.apache.qpid.jms.util.FactoryFinder;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;
import org.apache.qpid.proton.amqp.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the AMQP Redirect Map
 */
public class AmqpRedirect {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpRedirect.class);

    private static final FactoryFinder<ProviderFactory> PROVIDER_FACTORY_FINDER =
            new FactoryFinder<ProviderFactory>(ProviderFactory.class,
                "META-INF/services/" + ProviderFactory.class.getPackage().getName().replace(".", "/") + "/redirects/");

    private final Map<Symbol, Object> redirect;
    private final AmqpProvider provider;

    public AmqpRedirect(Map<Symbol, Object> redirect, AmqpProvider provider) {
        this.redirect = redirect;
        this.provider = provider;

        if (provider == null) {
            throw new IllegalArgumentException("A provider instance is required");
        }

        URI remoteURI = provider.getRemoteURI();
        if (remoteURI == null || remoteURI.getScheme() == null || remoteURI.getScheme().isEmpty()) {
            throw new IllegalArgumentException("The provider instance must provide a valid scheme");
        }
    }

    public AmqpRedirect validate() throws Exception {
        String networkHost = (String) redirect.get(NETWORK_HOST);
        if (networkHost == null || networkHost.isEmpty()) {
            throw new IOException("Redirection information not set, missing network host.");
        }

        try {
            Integer.parseInt(redirect.get(PORT).toString());
        } catch (Exception ex) {
            throw new IOException("Redirection information contained invalid port.");
        }

        String sourceScheme = provider.getRemoteURI().getScheme();
        String scheme = (String) redirect.get(SCHEME);
        if (scheme != null && !scheme.isEmpty() && !scheme.equals(sourceScheme)) {

            // Attempt to located a provider using normal scheme (amqp, amqps, etc...)
            ProviderFactory factory = null;
            try {
                factory = ProviderFactory.findProviderFactory(scheme);
            } catch (Throwable error) {
                LOG.trace("Couldn't find AMQP prefixed Provider using scheme: {}", scheme);
            }

            if (factory == null) {
                // Attempt to located a transport level redirect (ws, wss, etc...)
                try {
                    factory = findProviderFactoryByTransportScheme(scheme);
                } catch (Throwable error) {
                    LOG.trace("Couldn't find Provider using transport scheme: {}", scheme);
                }
            }

            if (factory == null || !(factory instanceof AmqpProviderFactory)) {
                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
            }

            LOG.trace("Found provider: {} for redirect: {}", factory.getName(), scheme);

            AmqpProviderFactory amqpFactory = (AmqpProviderFactory) factory;
            String transportType = amqpFactory.getTransportScheme();

            if (transportType == null || transportType.isEmpty()) {
                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
            }

            TransportFactory transportFactory = TransportFactory.findTransportFactory(transportType);
            if (transportFactory == null) {
                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
            }

            // Check for insecure redirect and whether it is allowed.
            if (provider.getTransport().isSecure() && !transportFactory.isSecure() && !provider.isAllowNonSecureRedirects()) {
                throw new IOException("Attempt to redirect to an insecure connection type: " + transportType);
            }

            // Update the redirect information with the resolved target scheme used to create
            // the provider for the redirection.
            redirect.put(SCHEME, amqpFactory.getProviderScheme());
        }

        // Check it actually converts to URI since we require it do so later
        toURI();

        return this;
    }

    /**
     * @return the redirection map that backs this object
     */
    public Map<Symbol, Object> getRedirectMap() {
        return redirect;
    }

    /**
     * @return the host name of the container being redirected to.
     */
    public String getHostname() {
        return (String) redirect.get(OPEN_HOSTNAME);
    }

    /**
     * @return the DNS host name or IP address of the peer this connection is being redirected to.
     */
    public String getNetworkHost() {
        return (String) redirect.get(NETWORK_HOST);
    }

    /**
     * @return the port number on the peer this connection is being redirected to.
     */
    public int getPort() {
        return Integer.parseInt(redirect.get(PORT).toString());
    }

    /**
     * @return the scheme that the remote indicated the redirect connection should use.
     */
    public String getScheme() {
        String scheme = (String) redirect.get(SCHEME);
        if (scheme == null || scheme.isEmpty()) {
            scheme = provider.getRemoteURI().getScheme();
        }

        return scheme;
    }

    /**
     * @return the path that the remote indicated should be path of the redirect URI.
     */
    public String getPath() {
        return (String) redirect.get(PATH);
    }

    /**
     * Construct a URI from the redirection information available.
     *
     * @return a URI that matches the redirection information provided.
     *
     * @throws Exception if an error occurs construct a URI from the redirection information.
     */
    public URI toURI() throws Exception {
        Map<String, String> queryOptions = PropertyUtil.parseQuery(provider.getRemoteURI());

        URI result = new URI(getScheme(), null, getNetworkHost(), getPort(), getPath(), null, null);

        String hostname = getHostname();
        if (hostname != null && !hostname.isEmpty()) {
            // Ensure we replace any existing vhost option with the redirect version.
            queryOptions = new LinkedHashMap<>(queryOptions);
            queryOptions.put("amqp.vhost", hostname);
        }

        return URISupport.applyParameters(result, queryOptions);
    }

    private static ProviderFactory findProviderFactoryByTransportScheme(String scheme) throws IOException {
        if (scheme == null || scheme.isEmpty()) {
            throw new IOException("No Transport scheme specified.");
        }

        ProviderFactory factory = null;
        try {
            factory = PROVIDER_FACTORY_FINDER.newInstance(scheme);
        } catch (Throwable e) {
            throw new IOException("Provider NOT found using redirect scheme: [" + scheme + "]", e);
        }

        return factory;
    }
}
