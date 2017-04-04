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

public final class JmsTransactionInfo extends JmsAbstractResource implements Comparable<JmsTransactionInfo> {

    private final JmsSessionId sessionId;
    private final JmsTransactionId transactionId;
    private volatile boolean inDoubt;

    public JmsTransactionInfo(JmsSessionId sessionId, JmsTransactionId transactionId) {
        if (sessionId == null) {
            throw new IllegalArgumentException("Session ID cannot be null");
        }
        if (transactionId == null) {
            throw new IllegalArgumentException("Transaction ID cannot be null");
        }

        this.sessionId = sessionId;
        this.transactionId = transactionId;
    }

    public JmsTransactionInfo copy() {
        return new JmsTransactionInfo(sessionId, transactionId);
    }

    @Override
    public JmsTransactionId getId() {
        return transactionId;
    }

    /**
     * @return the JmsSessionId of the Session that this transaction is owned by.
     */
    public JmsSessionId getSessionId() {
        return this.sessionId;
    }

    /**
     * @return the inDoubt
     */
    public boolean isInDoubt() {
        return inDoubt;
    }

    /**
     * @param inDoubt the inDoubt to set
     */
    public void setInDoubt(boolean inDoubt) {
        this.inDoubt = inDoubt;
    }

    @Override
    public int hashCode() {
        return transactionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        JmsTransactionInfo other = (JmsTransactionInfo) obj;
        return transactionId.equals(other.transactionId);
    }

    @Override
    public int compareTo(JmsTransactionInfo other) {
        return transactionId.compareTo(other.transactionId);
    }

    @Override
    public String toString() {
        return "JmsTransactionInfo { " + this.transactionId + " }";
    }

    @Override
    public void visit(JmsResourceVistor visitor) throws Exception {
        visitor.processTransactionInfo(this);
    }
}
