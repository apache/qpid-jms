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
package org.apache.qpid.jms.provider.discovery.multicast;

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.provider.discovery.DiscoveryAgent;
import org.apache.qpid.jms.provider.discovery.DiscoveryAgentFactory;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;

/**
 * Creates and configures a new instance of the mutlicast agent.
 */
public class MulticastDiscoveryAgentFactory extends DiscoveryAgentFactory {

    private static final String DEFAULT_SERVICE = "activemq";

    @Override
    public DiscoveryAgent createDiscoveryAgent(URI discoveryURI) throws Exception {
        MulticastDiscoveryAgent agent = new MulticastDiscoveryAgent(discoveryURI);
        Map<String, String> options = URISupport.parseParameters(discoveryURI);

        options = PropertyUtil.setProperties(agent, options);
        if (!options.isEmpty()) {
            String msg = ""
                + " Not all options could be set on the Multicast discovery."
                + " agent.  Check the options are spelled correctly."
                + " Unused parameters=[" + options + "]."
                + " This agent cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        String service = agent.getService();
        if (service == null || service.isEmpty()) {
            service = DEFAULT_SERVICE;
        }

        PacketParser packetParser = PacketParserFactory.createAgent(service);
        packetParser.setGroup(agent.getGroup());

        agent.setParser(packetParser);

        return agent;
    }

    @Override
    public String getName() {
        return "multicast";
    }
}
