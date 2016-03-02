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

import java.io.IOException;

import org.apache.qpid.jms.util.FactoryFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory used to find and create instances of DiscoveryAgent using the name
 * of the desired agent to locate it's factory class definition file.
 */
public abstract class PacketParserFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PacketParserFactory.class);

    private static final FactoryFinder<PacketParserFactory> AGENT_FACTORY_FINDER =
        new FactoryFinder<PacketParserFactory>(
            PacketParserFactory.class,
            "META-INF/services/org/apache/qpid/jms/provider/agents/multicast-parsers/");

    /**
     * Creates an instance of the given PacketParser
     *
     * @param key
     *        the name of the required packet parser for the agent.
     *
     * @return a new PacketParser instance.
     *
     * @throws Exception if an error occurs while creating the PacketParser instance.
     */
    public abstract PacketParser createPacketParser(String key) throws Exception;

    /**
     * @return the name of this packet parser, e.g ActiveMQ.
     */
    public abstract String getName();

    /**
     * Static create method that performs the PacketParser search and handles the
     * configuration and setup.
     *
     * @param key
     *        the name of the desired PacketParser type.
     *
     * @return a new PacketParser instance that is ready for use.
     *
     * @throws Exception if an error occurs while creating the PacketParser instance.
     */
    public static PacketParser createAgent(String key) throws Exception {
        PacketParser result = null;

        try {
            PacketParserFactory factory = findAgentFactory(key);
            result = factory.createPacketParser(key);
        } catch (Exception ex) {
            LOG.error("Failed to create PacketParserFactory instance for: {}", key);
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    /**
     * Searches for a PacketParserFactory by using the given key.
     *
     * The search first checks the local cache of packet parser factories before moving on
     * to search in the classpath.
     *
     * @param key
     *        The name of the PacketParserFactory that should be located.
     *
     * @return a PacketParserFactory instance matching the given key.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    protected static PacketParserFactory findAgentFactory(String key) throws IOException {
        if (key == null) {
            throw new IOException("No PacketParserFactory name specified: [null]");
        }

        PacketParserFactory factory = null;
        if (factory == null) {
            try {
                factory = AGENT_FACTORY_FINDER.newInstance(key);
            } catch (Throwable e) {
                throw new IOException("Discovery Agent scheme NOT recognized: [" + key + "]", e);
            }
        }

        return factory;
    }
}
