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
package org.apache.qpid.jms.provider;

import java.io.IOException;
import java.net.URI;

import org.apache.qpid.jms.util.FactoryFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that all JMS Providers must implement.
 */
public abstract class ProviderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ProviderFactory.class);

    private static final FactoryFinder<ProviderFactory> PROVIDER_FACTORY_FINDER =
        new FactoryFinder<ProviderFactory>(ProviderFactory.class,
            "META-INF/services/" + ProviderFactory.class.getPackage().getName().replace(".", "/") + "/");

    /**
     * Creates an instance of the given AsyncProvider and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Broker.
     *
     * @return a new AsyncProvider instance.
     *
     * @throws Exception if an error occurs while creating the Provider instance.
     */
    public abstract Provider createProvider(URI remoteURI) throws Exception;

    /**
     * Creates an instance of the given AsyncProvider and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Broker.
     * @param futureFactory
     * 		  The {@link ProviderFutureFactory} to use when creating the new {@link Provider}.
     *
     * @return a new AsyncProvider instance.
     *
     * @throws Exception if an error occurs while creating the Provider instance.
     */
    public abstract Provider createProvider(URI remoteURI, ProviderFutureFactory futureFactory) throws Exception;

    /**
     * @return the name of this Provider.
     */
    public abstract String getName();

    /**
     * Static create method that performs the ProviderFactory search and handles the
     * configuration and setup.
     *
     * @param remoteURI
     *        the URI of the remote peer.
     *
     * @return a new AsyncProvider instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the AsyncProvider instance.
     */
    public static Provider create(URI remoteURI) throws Exception {
        return create(remoteURI, null);
    }

    /**
     * Static create method that performs the ProviderFactory search and handles the
     * configuration and setup.
     *
     * @param remoteURI
     *        the URI of the remote peer.
     * @param futureFactory
     * 		  the {@link ProviderFutureFactory} to use when building the new {@link Provider}.
     *
     * @return a new AsyncProvider instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the AsyncProvider instance.
     */
    public static Provider create(URI remoteURI, ProviderFutureFactory futureFactory) throws Exception {
        Provider result = null;

        try {
            ProviderFactory factory = findProviderFactory(remoteURI);
            result = factory.createProvider(remoteURI, futureFactory);
        } catch (Exception ex) {
            LOG.error("Failed to create Provider instance for {}, due to: {}", remoteURI.getScheme(), ex);
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    /**
     * Searches for a ProviderFactory by using the scheme from the given URI.
     *
     * The search first checks the local cache of provider factories before moving on
     * to search in the class path.
     *
     * @param location
     *        The URI whose scheme will be used to locate a ProviderFactory.
     *
     * @return a provider factory instance matching the URI's scheme.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static ProviderFactory findProviderFactory(URI location) throws IOException {
        if (location == null) {
            throw new IOException("No Provider location specified.");
        }

        return findProviderFactory(location.getScheme());
    }

    /**
     * Searches for a ProviderFactory by using the scheme given.
     *
     * The search first checks the local cache of provider factories before moving on
     * to search in the class path.
     *
     * @param scheme
     *        The URI scheme that describes the desired factory.
     *
     * @return a provider factory instance matching the scheme.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static ProviderFactory findProviderFactory(String scheme) throws IOException {
        if (scheme == null || scheme.isEmpty()) {
            throw new IOException("No Provider scheme specified.");
        }

        ProviderFactory factory = null;
        try {
            factory = PROVIDER_FACTORY_FINDER.newInstance(scheme);
        } catch (Throwable e) {
            throw new IOException("Provider scheme NOT recognized: [" + scheme + "]", e);
        }

        return factory;
    }
}
