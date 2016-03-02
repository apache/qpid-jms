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

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Abstract Message Queue class used to implement the common functions of a Message Queue
 * instance.
 */
public abstract class AbstractMessageQueue implements MessageQueue {

    private volatile boolean closed;
    private volatile boolean running;
    private final Object lock = new Object();

    @Override
    public final JmsInboundMessageDispatch peek() {
        synchronized (lock) {
            return peekFirst();
        }
    }

    @Override
    public final JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (lock) {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && !closed && isEmpty() && running) {
                if (timeout == -1) {
                    lock.wait();
                } else {
                    long start = System.currentTimeMillis();
                    lock.wait(timeout);
                    timeout = Math.max(timeout + start - System.currentTimeMillis(), 0);
                }
            }

            if (closed || !running || isEmpty()) {
                return null;
            }

            return removeFirst();
        }
    }

    @Override
    public final JmsInboundMessageDispatch dequeueNoWait() {
        synchronized (lock) {
            if (closed || !running || isEmpty()) {
                return null;
            }
            return removeFirst();
        }
    }

    @Override
    public final void start() {
        synchronized (lock) {
            if (!closed) {
                running = true;
            }
            lock.notifyAll();
        }
    }

    @Override
    public final void stop() {
        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }
    }

    @Override
    public final boolean isRunning() {
        return running;
    }

    @Override
    public final void close() {
        synchronized (lock) {
            running = false;
            closed = true;
            lock.notifyAll();
        }
    }

    @Override
    public final boolean isClosed() {
        return closed;
    }

    @Override
    public final Object getLock() {
        return lock;
    }

    /**
     * Removes and returns the first entry in the implementation queue.  This method
     * is always called under lock and does not need to protect itself or check running
     * state etc.
     *
     * @return the first message queued in the implemented queue.
     */
    protected abstract JmsInboundMessageDispatch removeFirst();

    /**
     * Returns but does not remove the first entry in the implementation queue.  This method
     * is always called under lock and does not need to protect itself or check running
     * state etc.
     *
     * @return the first message queued in the implemented queue.
     */
    protected abstract JmsInboundMessageDispatch peekFirst();

}
