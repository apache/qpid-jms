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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;

/**
 * Class used to examine the capabilities and connection properties of the
 * remote connection and provide that information to the client code in a
 * simpler and more easy to digest manner.
 */
public class AmqpConnectionProperties {

    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");

    private boolean anonymousRelaySupported = false;
    private String queuePrefix = null;
    private String topicPrefix = null;

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

    protected void processCapabilities(Symbol[] capabilities) {
        List<Symbol> list = Arrays.asList(capabilities);
        if (list.contains(ANONYMOUS_RELAY)) {
            anonymousRelaySupported = true;
        }

        // TODO - Inspect capabilities for any other configuration options
    }

    protected void processProperties(Map<Symbol, Object> properties) {
        if (properties.containsKey(QUEUE_PREFIX)) {
            Object o = properties.get(QUEUE_PREFIX);
            if (o instanceof String) {
                queuePrefix = (String) o;
            }
        }

        if (properties.containsKey(TOPIC_PREFIX)) {
            Object o = properties.get(TOPIC_PREFIX);
            if (o instanceof String) {
                topicPrefix = (String) o;
            }
        }

        // TODO - Inspect properties for any other configuration options
    }

    public boolean isAnonymousRelaySupported() {
        return anonymousRelaySupported;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }
}
