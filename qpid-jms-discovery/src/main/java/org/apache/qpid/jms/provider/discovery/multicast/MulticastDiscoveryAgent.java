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
package org.apache.qpid.jms.provider.discovery.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.jms.provider.discovery.DiscoveryAgent;
import org.apache.qpid.jms.provider.discovery.DiscoveryEvent;
import org.apache.qpid.jms.provider.discovery.DiscoveryEvent.EventType;
import org.apache.qpid.jms.provider.discovery.DiscoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovery agent that listens on a multicast address for new Broker advisories.
 */
public class MulticastDiscoveryAgent implements DiscoveryAgent, Runnable {

    public static final String DEFAULT_DISCOVERY_URI_STRING = "multicast://239.255.2.3:6155";
    public static final String DEFAULT_HOST_STR = "default";
    public static final String DEFAULT_HOST_IP = System.getProperty("qpidjms.partition.discovery", "239.255.2.3");
    public static final int DEFAULT_PORT = 6155;

    private static final Logger LOG = LoggerFactory.getLogger(MulticastDiscoveryAgent.class);
    private static final int BUFF_SIZE = 8192;
    private static final int DEFAULT_IDLE_TIME = 500;
    private static final int HEARTBEAT_MISS_BEFORE_DEATH = 10;

    private DiscoveryListener listener;
    private URI discoveryURI;
    private int timeToLive = 1;
    private boolean loopBackMode;
    private final Map<String, RemoteBrokerData> brokersByService = new ConcurrentHashMap<String, RemoteBrokerData>();
    private String group = "default";
    private InetAddress inetAddress;
    private SocketAddress sockAddress;
    private MulticastSocket mcast;
    private Thread runner;
    private long keepAliveInterval = DEFAULT_IDLE_TIME;
    private String mcInterface;
    private String mcNetworkInterface;
    private String mcJoinNetworkInterface;
    private String service;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private PacketParser parser;

    public MulticastDiscoveryAgent(URI discoveryURI) {
        this.discoveryURI = discoveryURI;
    }

    @Override
    public void setDiscoveryListener(DiscoveryListener listener) {
        this.listener = listener;
    }

    public DiscoveryListener getDiscoveryListener() {
        return this.listener;
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        if (listener == null) {
            throw new IllegalStateException("No DiscoveryListener configured.");
        }

        if (started.compareAndSet(false, true)) {

            if (group == null || group.length() == 0) {
                throw new IOException("You must specify a group to discover");
            }

            if (discoveryURI == null) {
                try {
                    discoveryURI = new URI(DEFAULT_DISCOVERY_URI_STRING);
                } catch (URISyntaxException e) {
                    // Default is always valid.
                }
            }

            LOG.trace("mcast - discoveryURI = {}", discoveryURI);

            String myHost = discoveryURI.getHost();
            int myPort = discoveryURI.getPort();

            if (DEFAULT_HOST_STR.equals(myHost)) {
                myHost = DEFAULT_HOST_IP;
            }

            if (myPort < 0) {
                myPort = DEFAULT_PORT;
            }

            LOG.trace("mcast - myHost = {}", myHost);
            LOG.trace("mcast - myPort = {}", myPort);
            LOG.trace("mcast - group = {}", group);
            LOG.trace("mcast - interface = {}", mcInterface);
            LOG.trace("mcast - network interface = {}", mcNetworkInterface);
            LOG.trace("mcast - join network interface = {}", mcJoinNetworkInterface);

            this.inetAddress = InetAddress.getByName(myHost);
            this.sockAddress = new InetSocketAddress(this.inetAddress, myPort);
            mcast = new MulticastSocket(myPort);
            mcast.setLoopbackMode(loopBackMode);
            mcast.setTimeToLive(getTimeToLive());
            if (mcJoinNetworkInterface != null) {
                mcast.joinGroup(sockAddress, NetworkInterface.getByName(mcJoinNetworkInterface));
            } else {
                mcast.joinGroup(inetAddress);
            }
            mcast.setSoTimeout((int) keepAliveInterval);
            if (mcInterface != null) {
                mcast.setInterface(InetAddress.getByName(mcInterface));
            }
            if (mcNetworkInterface != null) {
                mcast.setNetworkInterface(NetworkInterface.getByName(mcNetworkInterface));
            }
            runner = new Thread(this);
            runner.setName(this.toString() + ":" + runner.getName());
            runner.setDaemon(true);
            runner.start();
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            if (mcast != null) {
                mcast.close();
            }
            if (runner != null) {
                runner.interrupt();
            }
        }
    }

    @Override
    public void suspend() {
        // We don't suspend multicast as it's mostly a passive listener.
    }

    @Override
    public void resume() {
        // We don't suspend multicast as it's mostly a passive listener.
    }

    @Override
    public void run() {
        byte[] buf = new byte[BUFF_SIZE];
        DatagramPacket packet = new DatagramPacket(buf, 0, buf.length);
        while (started.get()) {
            expireOldServices();
            try {
                mcast.receive(packet);
                if (packet.getLength() > 0) {
                    DiscoveryEvent event = parser.processPacket(packet.getData(), packet.getOffset(), packet.getLength());
                    if (event != null) {
                        if (event.getType() == EventType.ALIVE) {
                            processAlive(event);
                        } else {
                            processShutdown(event);
                        }
                    }
                }
            } catch (SocketTimeoutException se) {
                // ignore
            } catch (IOException e) {
                if (started.get()) {
                    LOG.error("failed to process packet: {}", e.getMessage());
                    LOG.trace(" packet processing failed by: {}", e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "MulticastDiscoveryAgent: listener:" + getDiscvoeryURI();
    }

    //---------- Internal Implementation -------------------------------------//

    private void processAlive(DiscoveryEvent event) {
        RemoteBrokerData data = brokersByService.get(event.getPeerUri());
        if (data == null) {
            String peerUri = event.getPeerUri();
            data = new RemoteBrokerData(event.getPeerUri());
            brokersByService.put(peerUri, data);
            fireServiceAddEvent(data);
        } else {
            data.updateHeartBeat();
        }
    }

    private void processShutdown(DiscoveryEvent event) {
        RemoteBrokerData data = brokersByService.remove(event.getPeerUri());
        if (data != null) {
            fireServiceRemovedEvent(data);
        }
    }

    private void expireOldServices() {
        long expireTime = System.currentTimeMillis() - (keepAliveInterval * HEARTBEAT_MISS_BEFORE_DEATH);
        for (Iterator<RemoteBrokerData> i = brokersByService.values().iterator(); i.hasNext();) {
            RemoteBrokerData data = i.next();
            if (data.getLastHeartBeat() < expireTime) {
                processShutdown(data.asShutdownEvent());
            }
        }
    }

    private void fireServiceRemovedEvent(final RemoteBrokerData data) {
        if (listener != null && started.get()) {
            listener.onServiceRemove(data);
        }
    }

    private void fireServiceAddEvent(final RemoteBrokerData data) {
        if (listener != null && started.get()) {
            listener.onServiceAdd(data);
        }
    }

    // ---------- Property Accessors ------------------------------------------//

    /**
     * @return the original URI used to create the Discovery Agent.
     */
    public URI getDiscvoeryURI() {
        return this.discoveryURI;
    }

    /**
     * @return Returns the loopBackMode.
     */
    public boolean isLoopBackMode() {
        return loopBackMode;
    }

    /**
     * @param loopBackMode
     *        The loopBackMode to set.
     */
    public void setLoopBackMode(boolean loopBackMode) {
        this.loopBackMode = loopBackMode;
    }

    /**
     * @return Returns the timeToLive.
     */
    public int getTimeToLive() {
        return timeToLive;
    }

    /**
     * @param timeToLive
     *        The timeToLive to set.
     */
    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    public long getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public void setInterface(String mcInterface) {
        this.mcInterface = mcInterface;
    }

    public void setNetworkInterface(String mcNetworkInterface) {
        this.mcNetworkInterface = mcNetworkInterface;
    }

    public void setJoinNetworkInterface(String mcJoinNetwrokInterface) {
        this.mcJoinNetworkInterface = mcJoinNetwrokInterface;
    }

    /**
     * @return the multicast group this agent is assigned to.
     */
    public String getGroup() {
        return this.group;
    }

    /**
     * Sets the multicast group this agent is assigned to.  The group can only be set
     * prior to starting the agent, once started the group change will never take effect.
     *
     * @param group
     *        the multicast group the agent is assigned to.
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Returns the name of the service that is providing the discovery data for this agent such
     * as ActiveMQ.
     *
     * @return the name of the service that is advertising remote peer data.
     */
    public String getService() {
        return this.service;
    }

    /**
     * Sets the name of the service that is providing the remote peer discovery data.
     *
     * @param name
     *        the name of the service that provides this agent with remote peer data.
     */
    public void setService(String name) {
        this.service = name;
    }

    /**
     * @return the currently configured datagram packet parser for this agent.
     */
    public PacketParser getParser() {
        return parser;
    }

    /**
     * Sets the datagram packet parser used to read the discovery data broadcast by the service
     * being monitored for remote peers.
     *
     * @param parser
     *        the datagram packet parser to use.
     */
    public void setParser(PacketParser parser) {
        this.parser = parser;
    }

    // ---------- Discovered Peer Bookkeeping Class ---------------------------//

    private class RemoteBrokerData extends DiscoveryEvent {

        long lastHeartBeat;

        public RemoteBrokerData(String peerUri) {
            super(peerUri, EventType.ALIVE);
            this.lastHeartBeat = System.currentTimeMillis();
        }

        /**
         * @return an event representing this remote peers shutdown event.
         */
        public DiscoveryEvent asShutdownEvent() {
            return new DiscoveryEvent(getPeerUri(), EventType.SHUTDOWN);
        }

        public synchronized void updateHeartBeat() {
            lastHeartBeat = System.currentTimeMillis();
        }

        public synchronized long getLastHeartBeat() {
            return lastHeartBeat;
        }
    }
}
