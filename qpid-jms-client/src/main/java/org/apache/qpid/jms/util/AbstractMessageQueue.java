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
package org.apache.qpid.jms.util;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Abstract Message Queue class used to implement the common functions of a Message Queue
 * instance.
 */
public abstract class AbstractMessageQueue implements MessageQueue {

    protected boolean closed;
    protected boolean running;
    protected Object lock = new Object();

    @Override
    public JmsInboundMessageDispatch peek() {
        synchronized (lock) {
            return peekFirst();
        }
    }

    @Override
    public JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (lock) {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && !closed && (isEmpty() || !running)) {
                if (timeout == -1) {
                    lock.wait();
                } else {
                    lock.wait(timeout);
                    break;
                }
            }

            if (closed || !running || isEmpty()) {
                return null;
            }

            return removeFirst();
        }
    }

    @Override
    public JmsInboundMessageDispatch dequeueNoWait() {
        synchronized (lock) {
            if (closed || !running || isEmpty()) {
                return null;
            }
            return removeFirst();
        }
    }

    @Override
    public void start() {
        synchronized (lock) {
            running = true;
            lock.notifyAll();
        }
    }

    @Override
    public void stop() {
        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (!closed) {
                running = false;
                closed = true;
            }
            lock.notifyAll();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Object getLock() {
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
