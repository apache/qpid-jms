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
package org.apache.qpid.jms.meta;

public final class JmsProducerId extends JmsAbstractResourceId implements Comparable<JmsProducerId> {

    private String connectionId;
    private long sessionId;
    private long value;

    private transient String key;
    private transient JmsSessionId parentId;

    public JmsProducerId(JmsSessionId sessionId, long producerId) {
        if (sessionId == null) {
            throw new IllegalArgumentException("Session ID cannot be null");
        }

        this.connectionId = sessionId.getConnectionId();
        this.sessionId = sessionId.getValue();
        this.value = producerId;
        this.parentId = sessionId;
    }

    public JmsProducerId(JmsProducerId id) {
        if (id == null) {
            throw new IllegalArgumentException("Producer ID cannot be null");
        }

        this.connectionId = id.getConnectionId();
        this.sessionId = id.getSessionId();
        this.value = id.getValue();
        this.parentId = id.getParentId();
    }

    public JmsProducerId(String connectionId, long sessionId, long producerId) {
        if (connectionId == null || connectionId.isEmpty()) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.value = producerId;
    }

    public JmsProducerId(String producerKey) {
        if (producerKey == null || producerKey.isEmpty()) {
            throw new IllegalArgumentException("Producer Key cannot be null or empty");
        }

        // Parse off the producerId
        int p = producerKey.lastIndexOf(":");
        if (p >= 0) {
            value = Long.parseLong(producerKey.substring(p + 1));
            producerKey = producerKey.substring(0, p);
        }
        setProducerSessionKey(producerKey);
    }

    public JmsSessionId getParentId() {
        if (parentId == null) {
            parentId = new JmsSessionId(this);
        }
        return parentId;
    }

    private void setProducerSessionKey(String sessionKey) {
        // Parse off the value of the session Id
        int p = sessionKey.lastIndexOf(":");
        if (p >= 0) {
            sessionId = Long.parseLong(sessionKey.substring(p + 1));
            sessionKey = sessionKey.substring(0, p);
        }

        // The rest is the value of the connection Id.
        connectionId = sessionKey;

        parentId = new JmsSessionId(connectionId, sessionId);
    }

    public String getConnectionId() {
        return connectionId;
    }

    public long getValue() {
        return value;
    }

    public long getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        if (key == null) {
            key = connectionId + ":" + sessionId + ":" + value;
        }
        return key;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = 1;
            hashCode = 31 * hashCode + connectionId.hashCode();
            hashCode = 31 * hashCode + (int) (sessionId ^ (sessionId >>> 32));
            hashCode = 31 * hashCode + (int) (value ^ (value >>> 32));
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != JmsProducerId.class) {
            return false;
        }

        JmsProducerId id = (JmsProducerId)o;
        return sessionId == id.sessionId && value == id.value && connectionId.equals(id.connectionId);
    }

    @Override
    public int compareTo(JmsProducerId other) {
        return toString().compareTo(other.toString());
    }
}
