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
package org.apache.qpid.jms.provider.mock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.jms.meta.JmsResource;

/**
 * Statistics gathering class used to track activity in all MockProvider
 * instances created from the MockProviderFactory.
 */
public class MockProviderStats {

    private final MockProviderStats parent;

    private final AtomicInteger providersCreated = new AtomicInteger();
    private final AtomicInteger connectionAttempts = new AtomicInteger();
    private final AtomicInteger closeAttempts = new AtomicInteger();
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

    private final ConcurrentMap<Class<? extends JmsResource>, AtomicInteger> resourceCreateCalls = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<? extends JmsResource>, AtomicInteger> resourceStartCalls = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<? extends JmsResource>, AtomicInteger> resourceStopCalls = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<? extends JmsResource>, AtomicInteger> resourceDestroyCalls = new ConcurrentHashMap<>();

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

    public int getCloseAttempts() {
        return closeAttempts.get();
    }

    public void recordCloseAttempt() {
        if (parent != null) {
            parent.recordCloseAttempt();
        }

        closeAttempts.incrementAndGet();
    }

    public int getCreateResourceCalls() {
        return createResourceCalls.get();
    }

    public int getCreateResourceCalls(Class<? extends JmsResource> resourceType) {
        AtomicInteger count = resourceCreateCalls.get(resourceType);
        if (count != null) {
            return count.get();
        }

        return 0;
    }

    public void recordCreateResourceCall(JmsResource resource) {
        if (parent != null) {
            parent.recordCreateResourceCall(resource);
        }

        createResourceCalls.incrementAndGet();
        AtomicInteger count = resourceCreateCalls.get(resource.getClass());
        if (count != null) {
            count.incrementAndGet();
        } else {
            resourceCreateCalls.putIfAbsent(resource.getClass(), new AtomicInteger(1));
        }
    }

    public int getStartResourceCalls() {
        return startResourceCalls.get();
    }

    public int getStartResourceCalls(Class<? extends JmsResource> resourceType) {
        AtomicInteger count = resourceStartCalls.get(resourceType);
        if (count != null) {
            return count.get();
        }

        return 0;
    }

    public void recordStartResourceCall(JmsResource resource) {
        if (parent != null) {
            parent.recordStartResourceCall(resource);
        }

        startResourceCalls.incrementAndGet();
        AtomicInteger count = resourceStartCalls.get(resource.getClass());
        if (count != null) {
            count.incrementAndGet();
        } else {
            resourceStartCalls.putIfAbsent(resource.getClass(), new AtomicInteger(1));
        }
    }

    public int getStopResourceCalls() {
        return stopResourceCalls.get();
    }

    public int getStopResourceCalls(Class<? extends JmsResource> resourceType) {
        AtomicInteger count = resourceStopCalls.get(resourceType);
        if (count != null) {
            return count.get();
        }

        return 0;
    }

    public void recordStopResourceCall(JmsResource resource) {
        if (parent != null) {
            parent.recordStopResourceCall(resource);
        }

        stopResourceCalls.incrementAndGet();
        AtomicInteger count = resourceStopCalls.get(resource.getClass());
        if (count != null) {
            count.incrementAndGet();
        } else {
            resourceStopCalls.putIfAbsent(resource.getClass(), new AtomicInteger(1));
        }
    }

    public int getDestroyResourceCalls() {
        return destroyResourceCalls.get();
    }

    public int getDestroyResourceCalls(Class<? extends JmsResource> resourceType) {
        AtomicInteger count = resourceDestroyCalls.get(resourceType);
        if (count != null) {
            return count.get();
        }

        return 0;
    }

    public void recordDestroyResourceCall(JmsResource resource) {
        if (parent != null) {
            parent.recordDestroyResourceCall(resource);
        }

        destroyResourceCalls.incrementAndGet();
        AtomicInteger count = resourceDestroyCalls.get(resource.getClass());
        if (count != null) {
            count.incrementAndGet();
        } else {
            resourceDestroyCalls.putIfAbsent(resource.getClass(), new AtomicInteger(1));
        }
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
        closeAttempts.set(0);
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

        resourceCreateCalls.clear();
        resourceStartCalls.clear();
        resourceStopCalls.clear();
        resourceDestroyCalls.clear();
    }
}
