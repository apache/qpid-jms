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

public final class JmsSessionId extends JmsAbstractResourceId implements Comparable<JmsSessionId> {

    private final String connectionId;
    private final long value;

    private transient String key;
    private transient JmsConnectionId parentId;

    public JmsSessionId(String connectionId, long value) {
        if (connectionId == null || connectionId.isEmpty()) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        this.connectionId = connectionId;
        this.value = value;
    }

    public JmsSessionId(JmsConnectionId connectionId, long sessionId) {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        this.connectionId = connectionId.getValue();
        this.value = sessionId;
        this.parentId = connectionId;
    }

    public JmsSessionId(JmsSessionId id) {
        if (id == null) {
            throw new IllegalArgumentException("Session ID cannot be null");
        }

        this.connectionId = id.getConnectionId();
        this.value = id.getValue();
    }

    public JmsSessionId(JmsProducerId id) {
        if (id == null) {
            throw new IllegalArgumentException("Producer ID cannot be null");
        }

        this.connectionId = id.getConnectionId();
        this.value = id.getSessionId();
    }

    public JmsSessionId(JmsConsumerId id) {
        if (id == null) {
            throw new IllegalArgumentException("Consumer ID cannot be null");
        }

        this.connectionId = id.getConnectionId();
        this.value = id.getSessionId();
    }

    public JmsConnectionId getParentId() {
        if (parentId == null) {
            parentId = new JmsConnectionId(this);
        }
        return parentId;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = 1;
            hashCode = 31 * hashCode + connectionId.hashCode();
            hashCode = 31 * hashCode + (int) (value ^ (value >>> 32));
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != JmsSessionId.class) {
            return false;
        }

        JmsSessionId id = (JmsSessionId)o;
        return value == id.value && connectionId.equals(id.connectionId);
    }

    public String getConnectionId() {
        return connectionId;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        if (key == null) {
            key = connectionId + ":" + value;
        }
        return key;
    }

    @Override
    public int compareTo(JmsSessionId other) {
        return toString().compareTo(other.toString());
    }
}
