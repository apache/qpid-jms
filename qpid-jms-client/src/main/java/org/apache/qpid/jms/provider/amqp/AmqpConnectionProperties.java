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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.QUEUE_PREFIX;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TOPIC_PREFIX;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.proton.amqp.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to examine the capabilities and connection properties of the
 * remote connection and provide that information to the client code in a
 * simpler and more easy to digest manner.
 */
public class AmqpConnectionProperties {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectionProperties.class);

    private final JmsConnectionInfo connectionInfo;

    private boolean anonymousRelaySupported = false;
    private boolean connectionOpenFailed = false;

    /**
     * Creates a new instance of this class with default values read from the
     * given JmsConnectionInfo object.
     *
     * @param connectionInfo
     *        the JmsConnectionInfo object used to populate defaults.
     */
    public AmqpConnectionProperties(JmsConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    /**
     * Configure the properties using values provided from the remote peer.
     *
     * @param capabilities
     *        the capabilities offered by the remote connection.
     * @param properties
     *        the properties offered by the remote connection.
     */
    public void initialize(Symbol[] capabilities, Map<Symbol, Object> properties) {
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
    }

    protected void processProperties(Map<Symbol, Object> properties) {
        if (properties.containsKey(QUEUE_PREFIX)) {
            Object o = properties.get(QUEUE_PREFIX);
            if (o instanceof String) {
                LOG.trace("Remote sent Queue prefix value of: {}", o);
                connectionInfo.setQueuePrefix((String) o);
            }
        }

        if (properties.containsKey(TOPIC_PREFIX)) {
            Object o = properties.get(TOPIC_PREFIX);
            if (o instanceof String) {
                LOG.trace("Remote sent Topic prefix value of: {}", o);
                connectionInfo.setTopicPrefix((String) o);
            }
        }

        if (properties.containsKey(CONNECTION_OPEN_FAILED)) {
            LOG.trace("Remote sent Connection Establishment Failed marker.");
            connectionOpenFailed = true;
        }
    }

    /**
     * @return true if the connection supports sending to an anonymous relay.
     */
    public boolean isAnonymousRelaySupported() {
        return anonymousRelaySupported;
    }

    /**
     * Sets if the connection supports sending to an anonymous relay link.
     *
     * @param anonymousRelaySupported
     *        true if anonymous relay link is supported.
     */
    public void setAnonymousRelaySupported(boolean anonymousRelaySupported) {
        this.anonymousRelaySupported = anonymousRelaySupported;
    }

    /**
     * @return the prefix value used to decorate Queue address values.
     */
    public String getQueuePrefix() {
        return connectionInfo.getQueuePrefix();
    }

    /**
     * Sets the prefix value used to decorate Queue address values.
     *
     * @param queuePrefix
     *        the current queue address prefix.
     */
    public void setQueuePrefix(String queuePrefix) {
        connectionInfo.setQueuePrefix(queuePrefix);
    }

    /**
     * @return the prefix value used to decorate Topic address values.
     */
    public String getTopicPrefix() {
        return connectionInfo.getTopicPrefix();
    }

    /**
     * Sets the prefix value used to decorate Topic address values.
     *
     * @param topicPrefix
     *        the current topic address prefix.
     */
    public void setTopicPrefix(String topicPrefix) {
        connectionInfo.setTopicPrefix(topicPrefix);
    }

    /**
     * Returns true if the remote connection marked the open response as being in
     * a failed state which implies that a close follows.
     *
     * @return the connectionOpenFailed value.
     */
    public boolean isConnectionOpenFailed() {
        return connectionOpenFailed;
    }

    /**
     * Sets the state of the connection open failed flag.  When this flag is set
     * true it implies that the open response will have a close response to follow.
     *
     * @param connectionOpenFailed
     *        the connectionOpenFailed value to use for these properties.
     */
    public void setConnectionOpenFailed(boolean connectionOpenFailed) {
        this.connectionOpenFailed = connectionOpenFailed;
    }
}
