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

public final class JmsConsumerId extends JmsAbstractResourceId implements Comparable<JmsConsumerId> {

    private String connectionId;
    private long sessionId;
    private long value;

    private transient String key;
    private transient JmsSessionId parentId;

    public JmsConsumerId(String connectionId, long sessionId, long consumerId) {
        if (connectionId == null || connectionId.isEmpty()) {
            throw new IllegalArgumentException("Connection ID cannot be null or an empty string");
        }

        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.value = consumerId;
    }

    public JmsConsumerId(JmsSessionId sessionId, long consumerId) {
        if (sessionId == null) {
            throw new IllegalArgumentException("Session ID cannot be null");
        }

        this.connectionId = sessionId.getConnectionId();
        this.sessionId = sessionId.getValue();
        this.value = consumerId;
        this.parentId = sessionId;
    }

    public JmsConsumerId(JmsConsumerId consumerId) {
        if (consumerId == null) {
            throw new IllegalArgumentException("Consumer ID cannot be null");
        }

        this.connectionId = consumerId.getConnectionId();
        this.sessionId = consumerId.getSessionId();
        this.value = consumerId.getValue();
        this.parentId = consumerId.getParentId();
    }

    public JmsConsumerId(String consumerKey) throws IllegalArgumentException {
        if (consumerKey == null || consumerKey.isEmpty()) {
            throw new IllegalArgumentException("Consumer Key cannot be null or empty");
        }

        // Parse off the consumer Id value
        int p = consumerKey.lastIndexOf(":");
        if (p >= 0) {
            value = Long.parseLong(consumerKey.substring(p + 1));
            consumerKey = consumerKey.substring(0, p);
        }
        setConsumerSessionKey(consumerKey);
    }

    public JmsSessionId getParentId() {
        if (parentId == null) {
            parentId = new JmsSessionId(this);
        }
        return parentId;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getValue() {
        return value;
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
        if (o == null || o.getClass() != JmsConsumerId.class) {
            return false;
        }

        JmsConsumerId id = (JmsConsumerId) o;
        return sessionId == id.sessionId && value == id.value && connectionId.equals(id.connectionId);
    }

    @Override
    public String toString() {
        if (key == null) {
            key = connectionId + ":" + sessionId + ":" + value;
        }
        return key;
    }

    @Override
    public int compareTo(JmsConsumerId other) {
        return toString().compareTo(other.toString());
    }

    private void setConsumerSessionKey(String sessionKey) {
        // Parse off the value of the session Id
        int p = sessionKey.lastIndexOf(":");
        if (p >= 0) {
            sessionId = Long.parseLong(sessionKey.substring(p + 1));
            sessionKey = sessionKey.substring(0, p);
        }

        // The rest is the value of the connection Id.
        connectionId = sessionKey;
    }
}
