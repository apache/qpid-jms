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
package org.apache.qpid.jms.transports;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.util.FactoryFinder;
import org.apache.qpid.jms.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that all Transport types must implement.
 */
public abstract class TransportFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TransportFactory.class);

    private static final FactoryFinder<TransportFactory> TRANSPORT_FACTORY_FINDER =
        new FactoryFinder<TransportFactory>(TransportFactory.class,
            "META-INF/services/" + TransportFactory.class.getPackage().getName().replace(".", "/") + "/");

    /**
     * Creates an instance of the given Transport and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Peer.
     *
     * @return a new Transport instance.
     *
     * @throws Exception if an error occurs while creating the Transport instance.
     */
    public Transport createTransport(URI remoteURI) throws Exception {
        Map<String, String> map = PropertyUtil.parseQuery(remoteURI);
        Map<String, String> transportURIOptions = PropertyUtil.filterProperties(map, "transport.");

        remoteURI = PropertyUtil.replaceQuery(remoteURI, map);

        TransportOptions transportOptions = applyTransportConfiguration(doCreateTransportOptions(), transportURIOptions);
        Transport result = doCreateTransport(remoteURI, transportOptions);

        return result;
    }

    /**
     * Create and return an instance of TransportOptions appropriate for the Transport
     * type that this factory will return.
     *
     * @return a newly allocated TransportOptions instance appropriate to the factory.
     */
    protected TransportOptions doCreateTransportOptions() {
        return new TransportOptions();
    }

    /**
     * Apply URI options to a freshly created {@link TransportOptions} instance which will be used
     * when the actual {@link Transport} is created.
     *
     * @param transportOptions
     * 		The {@link TransportOptions} instance to configure.
     * @param transportURIOptions
     * 		The URI options to apply to the given {@link TransportOptions}.
     * @return
     */
    protected TransportOptions applyTransportConfiguration(TransportOptions transportOptions, Map<String, String> transportURIOptions) {
        Map<String, String> unused = PropertyUtil.setProperties(transportOptions, transportURIOptions);
        if (!unused.isEmpty()) {
            String msg = " Not all transport options could be set on the " + getName() +
                         " Transport. Check the options are spelled correctly." +
                         " Unused parameters=[" + unused + "]." +
                         " This provider instance cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        return transportOptions;
    }

    /**
     * Create the actual Transport instance for this factory using the provided URI and
     * TransportOptions instances.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Peer.
     * @param transportOptions
     *        The TransportOptions used to configure the new Transport.
     *
     * @return a newly created and configured Transport instance.
     *
     * @throws Exception if an error occurs while creating the Transport instance.
     */
    protected abstract Transport doCreateTransport(URI remoteURI, TransportOptions transportOptions) throws Exception;

    /**
     * @return the name of this Transport.
     */
    public abstract String getName();

    /**
     * @return true if the Transport that this factory provides uses a secure channel.
     */
    public boolean isSecure() {
        return false;
    }

    /**
     * Static create method that performs the TransportFactory search and handles the
     * configuration and setup.
     *
     * @param transportKey
     *        The transport type name used to locate a TransportFactory.
     * @param remoteURI
     *        the URI of the remote peer.
     *
     * @return a new Transport instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the Transport instance.
     */
    public static Transport create(String transportKey, URI remoteURI) throws Exception {
        Transport result = null;

        try {
            TransportFactory factory = findTransportFactory(transportKey);
            result = factory.createTransport(remoteURI);
        } catch (Exception ex) {
            LOG.error("Failed to create Transport instance for {}, due to: {}", remoteURI.getScheme(), ex);
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    /**
     * Searches for a TransportFactory by using the scheme from the given key.
     *
     * The search first checks the local cache of Transport factories before moving on
     * to search in the class-path.
     *
     * @param transportKey
     *        The transport type name used to locate a TransportFactory.
     *
     * @return a Transport factory instance matching the transport key.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static TransportFactory findTransportFactory(String transportKey) throws IOException {
        if (transportKey == null) {
            throw new IOException("No Transport key specified");
        }

        TransportFactory factory = null;
        try {
            factory = TRANSPORT_FACTORY_FINDER.newInstance(transportKey);
        } catch (Throwable e) {
            throw new IOException("Transport type NOT recognized: [" + transportKey + "]", e);
        }

        return factory;
    }
}
