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
 * Queue based storage interface for inbound Messages.
 */
public interface MessageQueue {

    /**
     * Adds the given message envelope to the end of the Message queue.
     *
     * @param envelope
     *        The in-bound Message envelope to enqueue.
     */
    void enqueue(JmsInboundMessageDispatch envelope);

    /**
     * Adds the given message envelope to the front of the Message queue.
     *
     * @param envelope
     *        The in-bound Message envelope to enqueue.
     */
    void enqueueFirst(JmsInboundMessageDispatch envelope);

    /**
     * Used to get an enqueued message. The amount of time this method blocks is
     * based on the timeout value. - if timeout==-1 then it blocks until a
     * message is received. - if timeout==0 then it it tries to not block at
     * all, it returns a message if it is available - if {@literal timeout > 0} then it
     * blocks up to timeout amount of time. Expired messages will consumed by
     * this method.
     *
     * @param timeout
     *      The amount of time to wait for an entry to be added before returning null.
     *
     * @return null if we timeout or if the consumer is closed.
     *
     * @throws InterruptedException if the wait is interrupted.
     */
    JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException;

    /**
     * Used to get an enqueued Message if on exists, otherwise returns null.
     *
     * @return the next Message in the Queue if one exists, otherwise null.
     */
    JmsInboundMessageDispatch dequeueNoWait();

    /**
     * Starts the Message Queue.  An non-started Queue will always return null for
     * any of the Queue methods.
     */
    void start();

    /**
     * Stops the Message Queue.  Messages cannot be read from the Queue when it is in
     * the stopped state and any waiters will be woken.
     */
    void stop();

    /**
     * Closes the Message Queue.  No messages can be added or removed from the Queue
     * once it has entered the closed state.
     */
    void close();

    /**
     * @return true if the Queue is not in the stopped or closed state.
     */
    boolean isRunning();

    /**
     * @return true if the Queue has been closed.
     */
    boolean isClosed();

    /**
     * @return true if there are no messages in the queue.
     */
    boolean isEmpty();

    /**
     * Returns the number of Messages currently in the Queue.  This value is only
     * meaningful at the time of the call as the size of the Queue changes rapidly
     * as Messages arrive and are consumed.
     *
     * @return the current number of Messages in the Queue.
     */
    int size();

    /**
     * Clears the Queue of any Messages.
     */
    void clear();

}