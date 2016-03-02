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

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.util.PropertyUtil;

/**
 * Factory for creating the AMQP provider.
 */
public class AmqpProviderFactory extends ProviderFactory {

    public static final String DEFAULT_TRANSPORT_TYPE = "tcp";

    private String transportType = DEFAULT_TRANSPORT_TYPE;

    @Override
    public Provider createProvider(URI remoteURI) throws Exception {

        Map<String, String> map = PropertyUtil.parseQuery(remoteURI.getQuery());
        Map<String, String> providerOptions = PropertyUtil.filterProperties(map, "amqp.");

        remoteURI = PropertyUtil.replaceQuery(remoteURI, map);

        AmqpProvider result = new AmqpProvider(remoteURI);

        result.setTransportType(getTransportType());

        Map<String, String> unused = PropertyUtil.setProperties(result, providerOptions);
        if (!unused.isEmpty()) {
            String msg = ""
                + " Not all provider options could be set on the AMQP Provider."
                + " Check the options are spelled correctly."
                + " Unused parameters=[" + unused + "]."
                + " This provider instance cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        return result;
    }

    @Override
    public String getName() {
        return "AMQP";
    }

    /**
     * @return the transport type used for this provider factory such as 'tcp' or 'ssl'
     */
    public String getTransportType() {
        return transportType;
    }

    /**
     * @param transportType
     *        the transport type name to use when creating a new provider.
     */
    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }
}
