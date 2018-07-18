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

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;
import org.apache.qpid.jms.util.URISupport.CompositeData;

/**
 * Factory for creating instances of the Failover Provider type.
 */
public class FailoverProviderFactory extends ProviderFactory {

    /**
     * Prefix used for all properties that apply specifically to the FailoverProvider
     */
    public static final String FAILOVER_OPTION_PREFIX = "failover.";

    /**
     * Prefix addition used for all nested properties that should be applied to any remote URIs.
     */
    public static final String FAILOVER_NESTED_OPTION_PREFIX_ADDON = "nested.";

    @Override
    public Provider createProvider(URI remoteURI) throws Exception {
        return createProvider(remoteURI, null);
    }

    @Override
    public Provider createProvider(URI remoteURI, ProviderFutureFactory futureFactory) throws Exception {
        CompositeData composite = URISupport.parseComposite(remoteURI);
        Map<String, String> options = composite.getParameters();

        Map<String, String> filtered = PropertyUtil.filterProperties(options, FAILOVER_OPTION_PREFIX);
        Map<String, String> nested = PropertyUtil.filterProperties(filtered, FAILOVER_NESTED_OPTION_PREFIX_ADDON);

        Map<String, String> providerOptions = PropertyUtil.filterProperties(options, "provider.");
        // If we have been given a futures factory to use then we ignore any URI options indicating
        // what to create and just go with what we are given.
        if (futureFactory == null) {
            // Create a configured ProviderFutureFactory for use by the resulting AmqpProvider
            futureFactory = ProviderFutureFactory.create(providerOptions);
            if (!providerOptions.isEmpty()) {
                String msg = ""
                    + " Not all Provider options could be applied during Failover Provider creation."
                    + " Check the options are spelled correctly."
                    + " Unused parameters=[" + providerOptions + "]."
                    + " This provider instance cannot be started.";
                throw new IllegalArgumentException(msg);
            }
        }

        FailoverProvider provider = new FailoverProvider(composite.getComponents(), nested, futureFactory);
        Map<String, String> unused = PropertyUtil.setProperties(provider, filtered);
        if (!unused.isEmpty()) {
            String msg = ""
                + " Not all options could be set on the Failover provider."
                + " Check the options are spelled correctly."
                + " Unused parameters=[" + unused + "]."
                + " This Provider cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        return provider;
    }

    @Override
    public String getName() {
        return "Failover";
    }
}
