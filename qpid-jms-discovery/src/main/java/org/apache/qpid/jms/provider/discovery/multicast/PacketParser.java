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

/**
 * Interface for a DatagramPacket parser object which is used by the
 * MulticastDiscoveryAget to parse incoming packets to determine the
 * discovered peer information.
 */
public interface PacketParser {

    /**
     * @return the multicast group assignment for this parser.
     */
    String getGroup();

    /**
     * Sets the multicast group that the parent agent is assigned to.  This can
     * be used in some cases to parse discovery messages.
     *
     * @param group
     *        the multicast group that this parser's parent agent resides in./
     */
    void setGroup(String group);

    /**
     * Process in incoming event packet and create a DiscoveryEvent from the data.
     *
     * @param data
     *        the new data packet to process.
     * @param offset
     *        the offset into the data buffer to start at.
     * @param length
     *        the length of the data packet contained in the buffer.
     *
     * @return a new DiscoveryEvent created from pocessing the incoming data.
     */
    DiscoveryEvent processPacket(byte[] data, int offset, int length);
}
