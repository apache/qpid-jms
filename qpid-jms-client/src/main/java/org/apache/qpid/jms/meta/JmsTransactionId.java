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

public final class JmsTransactionId extends JmsAbstractResourceId implements Comparable<JmsTransactionId> {

    private final JmsConnectionId connectionId;
    private final long value;

    public JmsTransactionId(JmsConnectionId connectionId, long transactionId) {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        this.connectionId = connectionId;
        this.value = transactionId;
    }

    @Override
    public String toString() {
        return "TX:" + connectionId + ":" + value;
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
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other.getClass() != JmsTransactionId.class) {
            return false;
        }

        JmsTransactionId tx = (JmsTransactionId) other;
        return value == tx.value && connectionId.equals(tx.connectionId);
    }

    @Override
    public int compareTo(JmsTransactionId o) {
        int result = connectionId.compareTo(o.connectionId);
        if (result == 0) {
            result = (int)(value - o.value);
        }
        return result;
    }

    public Object getProviderTxId() {
        return getProviderHint();
    }

    public long getValue() {
        return value;
    }

    public JmsConnectionId getConnectionId() {
        return connectionId;
    }
}
