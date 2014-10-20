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
package org.apache.qpid.jms.provider.amqp;

import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;

/**
 * Class used to examine the capabilities and connection properties of the
 * remote connection and provide that information to the client code in a
 * simpler and more easy to digest manner.
 */
public class AmqpConnectionProperties {

    public static final Symbol JMS_MAPPING_VERSION_KEY = Symbol.valueOf("x-opt-jms-mapping-version");
    public static final short JMS_MAPPING_VERSION_VALUE = 0;

    private static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("x-opt-anonymous-relay");

    private String anonymousRelayName;

    /**
     * Creates a new instance of this class from the given remote capabilities and properties.
     *
     * @param capabilities
     *        the capabilities offered by the remote connection.
     * @param properties
     *        the properties offered by the remote connection.
     */
    public AmqpConnectionProperties(Symbol[] capabilities, Map<Symbol, Object> properties) {
        if (capabilities != null) {
            processCapabilities(capabilities);
        }

        if (properties != null) {
            processProperties(properties);
        }
    }

    public boolean isAnonymousRelaySupported() {
        return anonymousRelayName != null;
    }

    public String getAnonymousRelayName() {
        return anonymousRelayName;
    }

    protected void processCapabilities(Symbol[] capabilities) {
        // TODO - Inspect capabilities for configuration options
    }

    protected void processProperties(Map<Symbol, Object> properties) {
        if (properties.containsKey(ANONYMOUS_RELAY)) {
            anonymousRelayName = (String) properties.get(ANONYMOUS_RELAY);
        }
    }
}
