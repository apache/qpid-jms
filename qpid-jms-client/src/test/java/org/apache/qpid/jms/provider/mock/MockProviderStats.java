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
package org.apache.qpid.jms.provider.mock;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Statistics gathering class used to track activity in all MockProvider
 * instances created from the MockProviderFactory.
 */
public class MockProviderStats {

    private final MockProviderStats parent;

    private final AtomicInteger providersCreated = new AtomicInteger();
    private final AtomicInteger connectionAttempts = new AtomicInteger();
    private final AtomicInteger createResourceCalls = new AtomicInteger();
    private final AtomicInteger startResourceCalls = new AtomicInteger();
    private final AtomicInteger stopResourceCalls = new AtomicInteger();
    private final AtomicInteger destroyResourceCalls = new AtomicInteger();
    private final AtomicInteger sendCalls = new AtomicInteger();
    private final AtomicInteger acknowledgeCalls = new AtomicInteger();
    private final AtomicInteger sessionAcknowledgeCalls = new AtomicInteger();
    private final AtomicInteger commitCalls = new AtomicInteger();
    private final AtomicInteger rollbackCalls = new AtomicInteger();
    private final AtomicInteger recoverCalls = new AtomicInteger();
    private final AtomicInteger unsubscribeCalls = new AtomicInteger();
    private final AtomicInteger pullCalls = new AtomicInteger();

    public MockProviderStats() {
        this(null);
    }

    public MockProviderStats(MockProviderStats parent) {
        this.parent = parent;
    }

    public int getProvidersCreated() {
        return providersCreated.get();
    }

    public void recordProviderCreated() {
        if (parent != null) {
            parent.recordProviderCreated();
        }

        providersCreated.incrementAndGet();
    }

    public int getConnectionAttempts() {
        return connectionAttempts.get();
    }

    public void recordConnectAttempt() {
        if (parent != null) {
            parent.recordConnectAttempt();
        }

        connectionAttempts.incrementAndGet();
    }

    public int getCreateResourceCalls() {
        return createResourceCalls.get();
    }

    public void recordCreateResourceCall() {
        if (parent != null) {
            parent.recordCreateResourceCall();
        }

        createResourceCalls.incrementAndGet();
    }

    public int getStartResourceCalls() {
        return startResourceCalls.get();
    }

    public void recordStartResourceCall() {
        if (parent != null) {
            parent.recordStartResourceCall();
        }

        startResourceCalls.incrementAndGet();
    }

    public int getStopResourceCalls() {
        return stopResourceCalls.get();
    }

    public void recordStopResourceCall() {
        if (parent != null) {
            parent.recordStopResourceCall();
        }

        stopResourceCalls.incrementAndGet();
    }

    public int getDestroyResourceCalls() {
        return destroyResourceCalls.get();
    }

    public void recordDetroyResourceCall() {
        if (parent != null) {
            parent.recordDetroyResourceCall();
        }

        destroyResourceCalls.incrementAndGet();
    }

    public int getSendCalls() {
        return sendCalls.get();
    }

    public void recordSendCall() {
        if (parent != null) {
            parent.recordSendCall();
        }

        sendCalls.incrementAndGet();
    }

    public int getAcnkowledgeCalls() {
        return acknowledgeCalls.get();
    }

    public void recoordAcknowledgeCall() {
        if (parent != null) {
            parent.recoordAcknowledgeCall();
        }

        acknowledgeCalls.incrementAndGet();
    }

    public int getSessopmAcknowledgeCalls() {
        return sessionAcknowledgeCalls.get();
    }

    public void recoordSessionAcknowledgeCall() {
        if (parent != null) {
            parent.recoordSessionAcknowledgeCall();
        }

        sessionAcknowledgeCalls.incrementAndGet();
    }

    public int getCommitCalls() {
        return commitCalls.get();
    }

    public void recordCommitCall() {
        if (parent != null) {
            parent.recordCommitCall();
        }

        commitCalls.incrementAndGet();
    }

    public int getRollbackCalls() {
        return rollbackCalls.get();
    }

    public void recordRollbackCall() {
        if (parent != null) {
            parent.recordRollbackCall();
        }

        commitCalls.incrementAndGet();
    }

    public int getRecoverCalls() {
        return recoverCalls.get();
    }

    public void recordRecoverCall() {
        if (parent != null) {
            parent.recordRecoverCall();
        }

        recoverCalls.incrementAndGet();
    }

    public int getUnsubscribeCalls() {
        return unsubscribeCalls.get();
    }

    public void recordUnsubscribeCall() {
        if (parent != null) {
            parent.recordUnsubscribeCall();
        }

        unsubscribeCalls.incrementAndGet();
    }

    public int getPullCalls() {
        return pullCalls.get();
    }

    public void recordPullCall() {
        if (parent != null) {
            parent.recordPullCall();
        }

        pullCalls.incrementAndGet();
    }

    public void reset() {
        providersCreated.set(0);
        connectionAttempts.set(0);
        createResourceCalls.set(0);
        startResourceCalls.set(0);
        stopResourceCalls.set(0);
        destroyResourceCalls.set(0);
        sendCalls.set(0);
        acknowledgeCalls.set(0);
        sessionAcknowledgeCalls.set(0);
        commitCalls.set(0);
        rollbackCalls.set(0);
        recoverCalls.set(0);
        unsubscribeCalls.set(0);
        pullCalls.set(0);
    }
}
