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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.qpid.jms.provider.discovery.multicast.MulticastDiscoveryAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MulticastTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MulticastTestSupport.class);

    public static MulticastSupportResult checkMulticastWorking() {
        String host = MulticastDiscoveryAgent.DEFAULT_HOST_IP;
        int myPort = MulticastDiscoveryAgent.DEFAULT_PORT;
        int timeToLive = 1;
        int soTimeout = 500;
        boolean success = false;
        String networkInterface = null;

        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            InetSocketAddress sockAddress = new InetSocketAddress(inetAddress, myPort);

            MulticastSocket mcastSend = new MulticastSocket(myPort);
            mcastSend.setTimeToLive(timeToLive);
            MulticastDiscoveryAgent.trySetNetworkInterface(mcastSend);
            mcastSend.joinGroup(inetAddress);
            mcastSend.setSoTimeout(soTimeout);

            MulticastSocket mcastRcv = new MulticastSocket(myPort);
            MulticastDiscoveryAgent.trySetNetworkInterface(mcastRcv);
            mcastRcv.joinGroup(inetAddress);
            mcastRcv.setSoTimeout(soTimeout);

            byte[] bytesOut = "verifyingMulticast".getBytes("UTF-8");
            DatagramPacket packetOut = new DatagramPacket(bytesOut, 0, bytesOut.length, sockAddress);

                mcastSend.send(packetOut);

            byte[] buf = new byte[1024];
            DatagramPacket packetIn = new DatagramPacket(buf, 0, buf.length);

            try {
                mcastRcv.receive(packetIn);

                if(packetIn.getLength() > 0) {
                    LOG.info("Received packet with content, multicast seems to be working!");
                    success = true;
                    networkInterface = mcastRcv.getNetworkInterface().getName();
                } else {
                    LOG.info("Received packet without content, lets assume multicast isnt working!");
                }
            } catch (SocketTimeoutException e) {
                LOG.info("Recieve timed out, assuming multicast isn't available");
            }
        } catch (UnknownHostException e) {
            LOG.info("Caught exception testing for multicast functionality", e);
        } catch (IOException e) {
            LOG.info("Caught exception testing for multicast functionality", e);
        }

        return new MulticastSupportResult(success, networkInterface);
    }

    public static class MulticastSupportResult {
        private boolean multicastWorking;
        private String networkInterface;

        public MulticastSupportResult(boolean multicastWorking, String networkInterface) {
            this.multicastWorking = multicastWorking;
            this.networkInterface = networkInterface;
        }

        public boolean isMulticastWorking() {
            return multicastWorking;
        }

        public String getNetworkInterface() {
            return networkInterface;
        }
    }
}
