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
package org.apache.qpid.jms.provider.discovery.multicast.parsers;

import org.apache.qpid.jms.provider.discovery.DiscoveryEvent;
import org.apache.qpid.jms.provider.discovery.DiscoveryEvent.EventType;
import org.apache.qpid.jms.provider.discovery.multicast.PacketParser;

/**
 * Parser instance for ActiveMQ multicast discovery processing.
 */
public class ActiveMQPacketParser implements PacketParser {

    private static final String TYPE_SUFFIX = "ActiveMQ-4.";
    private static final String ALIVE = "alive.";
    private static final String DEAD = "dead.";
    private static final String DELIMITER = "%";

    private String group;

    @Override
    public String getGroup() {
        return this.group;
    }

    @Override
    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public DiscoveryEvent processPacket(byte[] packet, int offset, int length) {
        String str = new String(packet, offset, length);
        DiscoveryEvent event = null;
        if (str.startsWith(getType())) {
            String payload = str.substring(getType().length());
            if (payload.startsWith(ALIVE)) {
                String brokerName = getBrokerName(payload.substring(ALIVE.length()));
                String brokerUri = payload.substring(ALIVE.length() + brokerName.length() + 2);
                event = new DiscoveryEvent(brokerUri, EventType.ALIVE);
            } else {
                String brokerName = getBrokerName(payload.substring(DEAD.length()));
                String brokerUri = payload.substring(DEAD.length() + brokerName.length() + 2);
                event = new DiscoveryEvent(brokerUri, EventType.SHUTDOWN);
            }
        }
        return event;
    }

    private String getBrokerName(String str) {
        String result = null;
        int start = str.indexOf(DELIMITER);
        if (start >= 0) {
            int end = str.indexOf(DELIMITER, start + 1);
            result = str.substring(start + 1, end);
        }
        return result;
    }

    private String getType() {
        return group + "." + TYPE_SUFFIX;
    }
}
