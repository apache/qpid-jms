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
package org.apache.qpid.jms.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Simple first in / first out Message Queue.
 */
public final class FifoMessageQueue implements MessageQueue {

    protected static final AtomicIntegerFieldUpdater<FifoMessageQueue> STATE_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(FifoMessageQueue.class, "state");

    protected static final int CLOSED = 0;
    protected static final int STOPPED = 1;
    protected static final int RUNNING = 2;

    private volatile int state = STOPPED;

    protected final ReentrantLock lock = new ReentrantLock();
    protected final Condition condition = lock.newCondition();

    protected final Deque<JmsInboundMessageDispatch> queue;

    public FifoMessageQueue(int prefetchSize) {
        this.queue = new ArrayDeque<JmsInboundMessageDispatch>(Math.max(1, prefetchSize));
    }

    @Override
    public void enqueueFirst(JmsInboundMessageDispatch envelope) {
        lock.lock();
        try {
            queue.addFirst(envelope);
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void enqueue(JmsInboundMessageDispatch envelope) {
        lock.lock();
        try {
            queue.addLast(envelope);
            condition.signal();
        } finally {
            lock.unlock();
        }
    }


    @Override
    public JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        lock.lock();
        try {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && isRunning() && queue.isEmpty()) {
                if (timeout == -1) {
                    condition.await();
                } else {
                    long start = System.currentTimeMillis();
                    condition.await(timeout, TimeUnit.MILLISECONDS);
                    timeout = Math.max(timeout + start - System.currentTimeMillis(), 0);
                }
            }

            if (!isRunning()) {
                return null;
            }

            return queue.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final JmsInboundMessageDispatch dequeueNoWait() {
        lock.lock();
        try {
            if (!isRunning()) {
                return null;
            }

            return queue.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void start() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, STOPPED, RUNNING)) {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public final void stop() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, RUNNING, STOPPED)) {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public final void close() {
        if (STATE_FIELD_UPDATER.getAndSet(this, CLOSED) > CLOSED) {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public final boolean isRunning() {
        return state == RUNNING;
    }

    @Override
    public final boolean isClosed() {
        return state == CLOSED;
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            return queue.toString();
        } finally {
            lock.unlock();
        }
    }
}
