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

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Simple first in / first out Message Queue.
 */
public final class FifoMessageQueue extends AbstractMessageQueue {

    protected final LinkedList<JmsInboundMessageDispatch> list = new LinkedList<JmsInboundMessageDispatch>();

    @Override
    public void enqueueFirst(JmsInboundMessageDispatch envelope) {
        synchronized (getLock()) {
            list.addFirst(envelope);
            getLock().notify();
        }
    }

    @Override
    public void enqueue(JmsInboundMessageDispatch envelope) {
        synchronized (getLock()) {
            list.addLast(envelope);
            getLock().notify();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (getLock()) {
            return list.isEmpty();
        }
    }

    @Override
    public int size() {
        synchronized (getLock()) {
            return list.size();
        }
    }

    @Override
    public void clear() {
        synchronized (getLock()) {
            list.clear();
        }
    }

    @Override
    public List<JmsInboundMessageDispatch> removeAll() {
        synchronized (getLock()) {
            ArrayList<JmsInboundMessageDispatch> rc = new ArrayList<JmsInboundMessageDispatch>(list.size());
            for (JmsInboundMessageDispatch entry : list) {
                rc.add(entry);
            }
            list.clear();
            return rc;
        }
    }

    @Override
    public String toString() {
        synchronized (getLock()) {
            return list.toString();
        }
    }

    @Override
    protected JmsInboundMessageDispatch removeFirst() {
        return list.removeFirst();
    }

    @Override
    protected JmsInboundMessageDispatch peekFirst() {
        return list.peekFirst();
    }
}
