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

public final class JmsConnectionId extends JmsAbstractResourceId implements Comparable<JmsConnectionId> {

    private final String value;

    public JmsConnectionId(String connectionId) {
        if (connectionId == null || connectionId.isEmpty()) {
            throw new IllegalArgumentException("Connection ID cannot be null or empty");
        }

        this.value = connectionId;
    }

    public JmsConnectionId(JmsConnectionId id) {
        if (id == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        this.value = id.getValue();
    }

    public JmsConnectionId(JmsSessionId id) {
        if (id == null) {
            throw new IllegalArgumentException("Session ID cannot be null");
        }

        this.value = id.getConnectionId();
    }

    public JmsConnectionId(JmsProducerId id) {
        if (id == null) {
            throw new IllegalArgumentException("Producer ID cannot be null");
        }

        this.value = id.getConnectionId();
    }

    public JmsConnectionId(JmsConsumerId id) {
        if (id == null) {
            throw new IllegalArgumentException("Consumer ID cannot be null");
        }

        this.value = id.getConnectionId();
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = 1;
            hashCode = 31 * hashCode + value.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != JmsConnectionId.class) {
            return false;
        }
        JmsConnectionId id = (JmsConnectionId) o;
        return value.equals(id.value);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(JmsConnectionId o) {
        return value.compareTo(o.value);
    }
}
