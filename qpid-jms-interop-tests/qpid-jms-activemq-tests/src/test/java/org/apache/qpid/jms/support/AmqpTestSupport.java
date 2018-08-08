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
package org.apache.qpid.jms.support;

import java.net.URI;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport extends QpidJmsTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    private static final int PORT = Integer.getInteger("activemq.test.amqp.port", 0);

    protected boolean isAmqpDiscovery() {
        return false;
    }

    protected String getDiscoveryNetworkInterface() {
        return null;
    }

    protected String getAmqpTransformer() {
        return "jms";
    }

    protected int getSocketBufferSize() {
        return 64 * 1024;
    }

    protected int getIOBufferSize() {
        return 8 * 1024;
    }

    protected boolean isAddOpenWireConnector() {
        return false;
    }

    protected boolean isFrameTracingEnabled() {
        return false;
    }

    @Override
    public URI getBrokerActiveMQClientConnectionURI() {
        if (isAddOpenWireConnector()) {
            try {
                return brokerService.getTransportConnectorByName("openwire").getPublishableConnectURI();
            } catch (Exception e) {
                throw new RuntimeException();
            }
        } else {
            return super.getBrokerActiveMQClientConnectionURI();
        }
    }

    @Override
    protected void addAdditionalConnectors(BrokerService brokerService, Map<String, Integer> portMap) throws Exception {
        int port = PORT;
        if (portMap.containsKey("amqp")) {
            port = portMap.get("amqp");
        }
        TransportConnector connector = brokerService.addConnector(
            "amqp://127.0.0.1:" + port +
            "?transport.transformer=" + getAmqpTransformer() +
            "&transport.socketBufferSize=" + getSocketBufferSize() +
            "&transport.tcpNoDelay=true" +
            "&ioBufferSize=" + getIOBufferSize());
        connector.setName("amqp");
        if (isAmqpDiscovery()) {
            String uriString = "multicast://default";
            if(getDiscoveryNetworkInterface() != null) {
                uriString += "?networkInterface=" + getDiscoveryNetworkInterface();
            }
            connector.setDiscoveryUri(new URI(uriString));
        }
        port = connector.getPublishableConnectURI().getPort();
        LOG.debug("Using amqp port: {}", port);

        if (isAddOpenWireConnector()) {
            if (portMap.containsKey("openwire")) {
                port = portMap.get("openwire");
            } else {
                port = 0;
            }

            connector = brokerService.addConnector("tcp://0.0.0.0:" + port);
            connector.setName("openwire");

            LOG.debug("Using openwire port: {}", port);
        }
    }

    public String getAmqpConnectionURIOptions() {
        return "";
    }

    public URI getBrokerAmqpConnectionURI() {
        try {
            String uri = "amqp://127.0.0.1:" +
                brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI().getPort();

            if (!getAmqpConnectionURIOptions().isEmpty()) {
                uri = uri + "?amqp.traceFrames=" + isFrameTracingEnabled() + "&" + getAmqpConnectionURIOptions();
            } else {
                uri = uri + "?amqp.traceFrames=" + isFrameTracingEnabled();
            }

            return new URI(uri);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public String getAmqpFailoverURI() throws Exception {
        StringBuilder uri = new StringBuilder();
        uri.append("failover://(");
        uri.append(brokerService.getTransportConnectorByName("amqp").getPublishableConnectString());

        for (BrokerService broker : brokers) {
            uri.append(",");
            uri.append(broker.getTransportConnectorByName("amqp").getPublishableConnectString());
        }

        uri.append(")");

        return uri.toString();
    }

    public Connection createAmqpConnection() throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI());
    }

    public Connection createAmqpConnection(String username, String password) throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
    }

    public Connection createAmqpConnection(URI brokerURI) throws Exception {
        return createAmqpConnection(brokerURI, null, null);
    }

    public Connection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
        ConnectionFactory factory = createAmqpConnectionFactory(brokerURI, username, password);
        return factory.createConnection();
    }

    public ConnectionFactory createAmqpConnectionFactory() throws Exception {
        return createAmqpConnectionFactory(getBrokerAmqpConnectionURI(), null, null);
    }

    public ConnectionFactory createAmqpConnectionFactory(URI brokerURI) throws Exception {
        return createAmqpConnectionFactory(brokerURI, null, null);
    }

    public ConnectionFactory createAmqpConnectionFactory(String username, String password) throws Exception {
        return createAmqpConnectionFactory(getBrokerAmqpConnectionURI(), username, password);
    }

    public ConnectionFactory createAmqpConnectionFactory(URI brokerURI, String username, String password) throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        factory.setForceAsyncSend(isForceAsyncSends());
        factory.setForceSyncSend(isForceSyncSends());
        factory.setForceAsyncAcks(isForceAsyncAcks());
        if (username != null) {
            factory.setUsername(username);
        }
        if (password != null) {
            factory.setPassword(password);
        }
        return factory;
    }

    public abstract class Testable implements Runnable {

        protected String failure;

        @Override
        public String toString() {
            return failure;
        }

        public synchronized boolean passed() {
            return failure == null;
        }
    }
}