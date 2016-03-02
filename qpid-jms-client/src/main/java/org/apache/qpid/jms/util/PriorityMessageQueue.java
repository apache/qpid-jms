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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Simple Message Priority ordered Queue.  Message envelopes are stored in the
 * Queue based on their priority value, except where {@link #enqueueFirst} is
 * used.
 */
public final class PriorityMessageQueue extends AbstractMessageQueue {

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
        synchronized (getLock()) {
            getList(envelope).addLast(envelope);
            this.size++;
            getLock().notify();
        }
    }

    @Override
    public void enqueueFirst(JmsInboundMessageDispatch envelope) {
        synchronized (getLock()) {
            getList(MAX_PRIORITY).addFirst(envelope);
            this.size++;
            getLock().notify();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (getLock()) {
            return size == 0;
        }
    }

    @Override
    public int size() {
        synchronized (getLock()) {
            return size;
        }
    }

    @Override
    public void clear() {
        synchronized (getLock()) {
            for (int i = 0; i <= MAX_PRIORITY; i++) {
                lists[i].clear();
            }
            this.size = 0;
        }
    }

    @Override
    public List<JmsInboundMessageDispatch> removeAll() {
        synchronized (getLock()) {
            ArrayList<JmsInboundMessageDispatch> result = new ArrayList<JmsInboundMessageDispatch>(size());
            for (int i = MAX_PRIORITY; i >= 0; i--) {
                List<JmsInboundMessageDispatch> list = lists[i];
                result.addAll(list);
                size -= list.size();
                list.clear();
            }
            return result;
        }
    }

    @Override
    protected JmsInboundMessageDispatch removeFirst() {
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

    @Override
    protected JmsInboundMessageDispatch peekFirst() {
        if (this.size > 0) {
            for (int i = MAX_PRIORITY; i >= 0; i--) {
                LinkedList<JmsInboundMessageDispatch> list = lists[i];
                if (!list.isEmpty()) {
                    return list.peekFirst();
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
