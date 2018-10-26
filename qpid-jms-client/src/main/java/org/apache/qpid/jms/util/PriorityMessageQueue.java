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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Simple Message Priority ordered Queue.  Message envelopes are stored in the
 * Queue based on their priority value, except where {@link #enqueueFirst} is
 * used.
 */
public final class PriorityMessageQueue implements MessageQueue {

    protected static final AtomicIntegerFieldUpdater<PriorityMessageQueue> STATE_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PriorityMessageQueue.class, "state");

    protected static final int CLOSED = 0;
    protected static final int STOPPED = 1;
    protected static final int RUNNING = 2;

    private volatile int state = STOPPED;

    protected final Object lock = new Object();

    // There are 10 priorities, values 0-9
    private static final Integer MAX_PRIORITY = 9;

    private final LinkedList<JmsInboundMessageDispatch>[] lists;
    private int size = 0;

    @SuppressWarnings("unchecked")
    public PriorityMessageQueue() {
        this.lists = new LinkedList[MAX_PRIORITY + 1];
        for (int i = 0; i <= MAX_PRIORITY; i++) {
            lists[i] = new LinkedList<JmsInboundMessageDispatch>();
        }
    }

    @Override
    public void enqueue(JmsInboundMessageDispatch envelope) {
        synchronized (lock) {
            getList(envelope).addLast(envelope);
            this.size++;
            lock.notify();
        }
    }

    @Override
    public void enqueueFirst(JmsInboundMessageDispatch envelope) {
        synchronized (lock) {
            getList(MAX_PRIORITY).addFirst(envelope);
            this.size++;
            lock.notify();
        }
    }

    @Override
    public final JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (lock) {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && isRunning() && isEmpty()) {
                if (timeout == -1) {
                    lock.wait();
                } else {
                    long start = System.currentTimeMillis();
                    lock.wait(timeout);
                    timeout = Math.max(timeout + start - System.currentTimeMillis(), 0);
                }
            }

            if (!isRunning() || isEmpty()) {
                return null;
            }

            return removeFirst();
        }
    }

    @Override
    public final JmsInboundMessageDispatch dequeueNoWait() {
        synchronized (lock) {
            if (!isRunning() || isEmpty()) {
                return null;
            }
            return removeFirst();
        }
    }

    @Override
    public final void start() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, STOPPED, RUNNING)) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    @Override
    public final void stop() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, RUNNING, STOPPED)) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    @Override
    public final void close() {
        if (STATE_FIELD_UPDATER.getAndSet(this, CLOSED) > CLOSED) {
            synchronized (lock) {
                lock.notifyAll();
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
        synchronized (lock) {
            return size == 0;
        }
    }

    @Override
    public int size() {
        synchronized (lock) {
            return size;
        }
    }

    @Override
    public void clear() {
        synchronized (lock) {
            for (int i = 0; i <= MAX_PRIORITY; i++) {
                lists[i].clear();
            }
            this.size = 0;
        }
    }

    private JmsInboundMessageDispatch removeFirst() {
        if (this.size > 0) {
            for (int i = MAX_PRIORITY; i >= 0; i--) {
                LinkedList<JmsInboundMessageDispatch> list = lists[i];
                if (!list.isEmpty()) {
                    this.size--;
                    return list.removeFirst();
                }
            }
        }
        return null;
    }

    private int getPriority(JmsInboundMessageDispatch envelope) {
        int priority = javax.jms.Message.DEFAULT_PRIORITY;
        if (envelope.getMessage() != null) {
            try {
                priority = Math.max(envelope.getMessage().getJMSPriority(), 0);
            } catch (JMSException e) {
            }
            priority = Math.min(priority, MAX_PRIORITY);
        }
        return priority;
    }

    private LinkedList<JmsInboundMessageDispatch> getList(JmsInboundMessageDispatch envelope) {
        return getList(getPriority(envelope));
    }

    private LinkedList<JmsInboundMessageDispatch> getList(int priority) {
        return lists[priority];
    }
}
