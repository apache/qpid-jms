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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

abstract class FifoMessageQueuePad0 {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class FifoMessageQueueProducerFields extends FifoMessageQueuePad0 {
    private static final AtomicLongFieldUpdater<FifoMessageQueueProducerFields> TAIL_FIELD_UPDATER =
            AtomicLongFieldUpdater.newUpdater(FifoMessageQueueProducerFields.class, "tail");

    private volatile long tail;
    protected long producerLimit;

    protected final long lvTail() {
        return tail;
    }

    protected final void soTail(long value) {
        TAIL_FIELD_UPDATER.lazySet(this, value);
    }

    protected final void svTail(long value) {
        tail = value;
    }
}

abstract class FifoMessageQueuePad1 extends FifoMessageQueueProducerFields {
    long p01, p02, p03, p04, p05, p06, p07, p08;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class FifoMessageQueueConsumerFields extends FifoMessageQueuePad1 {
    protected static final AtomicLongFieldUpdater<FifoMessageQueueConsumerFields> HEAD_FIELD_UPDATER =
            AtomicLongFieldUpdater.newUpdater(FifoMessageQueueConsumerFields.class, "head");

    protected static final AtomicLongFieldUpdater<FifoMessageQueueConsumerFields> HEAD_LOCK_FIELD_UPDATER =
            AtomicLongFieldUpdater.newUpdater(FifoMessageQueueConsumerFields.class, "headLock");
    protected static final AtomicIntegerFieldUpdater<FifoMessageQueueConsumerFields> STATE_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(FifoMessageQueueConsumerFields.class, "state");
    protected static final int CLOSED = 0;
    protected static final int STOPPED = 1;
    protected static final int RUNNING = 2;
    private volatile long head;
    private volatile long headLock;
    protected volatile int state = STOPPED;

    protected final boolean tryLockHead() {
        return HEAD_LOCK_FIELD_UPDATER.getAndIncrement(this) == 0;
    }

    protected final void unlockLockHead() {
        HEAD_LOCK_FIELD_UPDATER.lazySet(this, 0);
    }

    protected final long lvHead() {
        return head;
    }

    protected final void soHead(long value) {
        HEAD_FIELD_UPDATER.lazySet(this, value);
    }

    protected final void svHead(long value) {
        head = value;
    }
}

abstract class FifoMessageQueuePad2 extends FifoMessageQueueConsumerFields {
    long p01, p02, p03, p04, p05, p06, p07, p08;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class FifoMessageQueueSharedFields extends FifoMessageQueuePad2 {
    protected static final AtomicLongFieldUpdater<FifoMessageQueueSharedFields> WAITING_THREADS_FIELD_UPDATER =
            AtomicLongFieldUpdater.newUpdater(FifoMessageQueueSharedFields.class, "waitingThreads");
    private volatile long waitingThreads = 0;
    protected final ReentrantLock notEmptyLock = new ReentrantLock();
    protected final Condition notEmptyCondition = notEmptyLock.newCondition();

    public void signalNotEmpty() {
        if (isWaiting()) {
            notEmptyLock.lock();
            try {
                if (isWaiting()) {
                    notEmptyCondition.signalAll();
                }
            } finally {
                notEmptyLock.unlock();
            }
        }
    }

    private final boolean isWaiting() {
        return WAITING_THREADS_FIELD_UPDATER.get(this) > 0;
    }

    protected final void startWaiting() {
        WAITING_THREADS_FIELD_UPDATER.incrementAndGet(this);
    }

    protected final void stopWaiting() {
        WAITING_THREADS_FIELD_UPDATER.decrementAndGet(this);
    }
}

abstract class FifoMessageQueuePad3 extends FifoMessageQueueSharedFields {
    long p01, p02, p03, p04, p05, p06, p07, p08;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

/**
 * Simple lock-free first in / first out Message Queue.<br>
 * It is single producer on {@link #enqueue(JmsInboundMessageDispatch)} and {@link #enqueueFirst(JmsInboundMessageDispatch)} while
 * multi consumer on any dequeue operation although it will suffer contention in that case.
 */
public final class FifoMessageQueue extends FifoMessageQueuePad3 implements MessageQueue {

    /**
     * Note on concurrent fields accessors notation:
     * <p>
     * lvXXX:   load volatile of XXX
     * soXXX:   store ordered of XXX
     * svXXX:   store volatile of XXX
     */

    private final AtomicReferenceArray<JmsInboundMessageDispatch> messages;
    private final long mask;
    //this pad is needed to avoid false sharing with other regions in the JVM heap
    private static final int ARRAY_PAD = 128 / Integer.BYTES;
    private final int lookAheadStep;

    public FifoMessageQueue(int prefetchSize) {
        final int size = Math.max(4, prefetchSize);
        final int nextPow2Size = 1 << 32 - Integer.numberOfLeadingZeros(size - 1);
        messages = new AtomicReferenceArray<>(nextPow2Size + ARRAY_PAD * 2);
        soHead(0);
        soTail(0);
        mask = nextPow2Size - 1;
        lookAheadStep = nextPow2Size / 4;
        producerLimit = nextPow2Size;
    }

    private static int elementIndex(long sequence, long mask) {
        return ARRAY_PAD + (int) (sequence & mask);
    }

    private static void soElement(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask, long sequence, JmsInboundMessageDispatch dispatch) {
        messages.lazySet(elementIndex(sequence, mask), dispatch);
    }

    private static JmsInboundMessageDispatch lvElement(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask, long sequence) {
        return messages.get(elementIndex(sequence, mask));
    }

    private boolean exclusiveOffer(final AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask, final JmsInboundMessageDispatch e) {
        final long currentTail = lvTail();
        if (currentTail >= producerLimit && !exclusiveOfferSlowPath(messages, mask, currentTail)) {
            return false;
        }
        soElement(messages, mask, currentTail, e);
        //using a full barrier to avoid signalNotEmpty to happen before a completed offer
        svTail(currentTail + 1);
        return true;
    }

    private boolean exclusiveOffer(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask, JmsInboundMessageDispatch e, long currentTail) {
        if (currentTail >= producerLimit && !exclusiveOfferSlowPath(messages, mask, currentTail)) {
            return false;
        }
        soElement(messages, mask, currentTail, e);
        //using a full barrier to avoid signalNotEmpty to happen before a completed offer
        svTail(currentTail + 1);
        return true;
    }

    private boolean exclusiveOfferSlowPath(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask, long currentTail) {
        final int lookAheadStep = this.lookAheadStep;
        final long lookAheadTail = currentTail + lookAheadStep;

        if (lvElement(messages, mask, lookAheadTail) == null) {
            producerLimit = lookAheadTail;
        } else {
            if (lvElement(messages, mask, currentTail) != null) {
                return false;
            }
        }
        return true;
    }

    private boolean exclusiveOfferFirst(final JmsInboundMessageDispatch e) {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        final long currentTail = lvTail();
        if (currentTail == lvHead()) {
            return exclusiveOffer(messages, mask, e, currentTail);
        }
        while (!tryLockHead()) {
            Thread.yield();
        }
        final long currentHead = lvHead();
        //both head and tail cannot change here
        if (currentTail == currentHead) {
            //i can unlock it because the queue is empty for real: any consumer is waiting
            unlockLockHead();
            return exclusiveOffer(messages, mask, e, currentTail);
        }
        try {
            final long previousHead = currentHead - 1;
            //full
            if (lvElement(messages, mask, previousHead) != null) {
                return false;
            }
            //adjust the producer limit to be right before the consumer
            producerLimit = previousHead + mask + 1;
            soElement(messages, mask, previousHead, e);
            //using a full barrier to avoid signalNotEmpty to happen before a completed offerFirst
            svHead(previousHead);
            return true;
        } finally {
            unlockLockHead();
        }
    }

    private JmsInboundMessageDispatch sharedPoll(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask) {
        while (!tryLockHead()) {
            Thread.yield();
        }
        try {
            final long currentHead = lvHead();
            final JmsInboundMessageDispatch e = lvElement(messages, mask, currentHead);
            if (e == null) {
                return null;
            }
            soElement(messages, mask, currentHead, null);
            soHead(currentHead + 1);
            return e;
        } finally {
            unlockLockHead();
        }
    }

    private void sharedClear(AtomicReferenceArray<JmsInboundMessageDispatch> messages, long mask) {
        while (!tryLockHead()) {
            Thread.yield();
        }
        try {
            long currentHead = lvHead();
            while (true) {
                final JmsInboundMessageDispatch e = lvElement(messages, mask, currentHead);
                if (e == null) {
                    return;
                }
                soElement(messages, mask, currentHead, null);
                currentHead++;
                soHead(currentHead);
            }
        } finally {
            unlockLockHead();
        }
    }

    @Override
    public void enqueueFirst(JmsInboundMessageDispatch envelope) {
        while (!exclusiveOfferFirst(envelope)) {
            Thread.yield();
        }
        signalNotEmpty();
    }

    public boolean tryEnqueue(JmsInboundMessageDispatch envelope) {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        if (exclusiveOffer(messages, mask, envelope)) {
            signalNotEmpty();
            return true;
        }
        return false;
    }

    @Override
    public void enqueue(JmsInboundMessageDispatch envelope) {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        while (!exclusiveOffer(messages, mask, envelope)) {
            Thread.yield();
        }
        signalNotEmpty();
    }


    private JmsInboundMessageDispatch blockingDequeue() throws InterruptedException {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        JmsInboundMessageDispatch e = null;
        while (isRunning() && (e = sharedPoll(messages, mask)) == null) {
            startWaiting();
            //startWaiting must contain a full memory barrier to avoid
            //a producer may think the consumer is still active
            //when in fact it has decided to go to sleep.
            notEmptyLock.lock();
            try {
                final JmsInboundMessageDispatch lastChancePoll = sharedPoll(messages, mask);
                if (lastChancePoll != null) {
                    return lastChancePoll;
                }
                notEmptyCondition.await();
            } finally {
                notEmptyLock.unlock();
                stopWaiting();
            }
        }
        return e;
    }

    private JmsInboundMessageDispatch blockingDequeue(long timeoutInNanos) throws InterruptedException {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        long nanosLeftToWait = timeoutInNanos;
        JmsInboundMessageDispatch e = null;
        while (isRunning() && (e = sharedPoll(messages, mask)) == null && nanosLeftToWait > 0) {
            startWaiting();
            //startWaiting must contain a full memory barrier to avoid
            //a producer may think the consumer is still active
            //when in fact it has decided to go to sleep.
            notEmptyLock.lock();
            try {
                final JmsInboundMessageDispatch lastChancePoll = sharedPoll(messages, mask);
                if (lastChancePoll != null) {
                    return lastChancePoll;
                }
                nanosLeftToWait = notEmptyCondition.awaitNanos(nanosLeftToWait);
            } finally {
                notEmptyLock.unlock();
                stopWaiting();
            }
        }
        return e;
    }

    @Override
    public JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        if (timeout == -1) {
            return blockingDequeue();
        }
        if (timeout == 0) {
            return dequeueNoWait();
        }
        return blockingDequeue(TimeUnit.MILLISECONDS.toNanos(timeout));
    }

    @Override
    public final JmsInboundMessageDispatch dequeueNoWait() {
        if (!isRunning()) {
            return null;
        }
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        return sharedPoll(messages, mask);
    }

    @Override
    public final void start() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, STOPPED, RUNNING)) {
            signalNotEmpty();
        }
    }

    @Override
    public final void stop() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, RUNNING, STOPPED)) {
            signalNotEmpty();
        }
    }

    @Override
    public final void close() {
        if (STATE_FIELD_UPDATER.getAndSet(this, CLOSED) > CLOSED) {
            signalNotEmpty();
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

    public int capacity() {
        return (int) mask + 1;
    }

    @Override
    public int size() {
        /*
         * It is possible for a thread to be interrupted or reschedule between the read of the producer and
         * consumer indices, therefore protection is required to ensure size is within valid range. In the
         * event of concurrent polls/offers to this method the size is OVER estimated as we read consumer
         * index BEFORE the producer index.
         */
        long after = lvHead();
        long size;
        while (true) {
            final long before = after;
            final long currentProducerIndex = lvTail();
            after = lvHead();
            if (before == after) {
                size = (currentProducerIndex - after);
                //protection against offerFirst
                if (size >= 0) {
                    break;
                }
            }
        }
        //both long and int overflow are impossible (it is a bounded q)
        return (int) size;
    }

    @Override
    public boolean isEmpty() {
        // Order matters!
        // Loading consumer before producer allows for producer increments after consumer index is read.
        // This ensures this method is conservative in it's estimate.
        return (lvHead() == lvTail());
    }

    @Override
    public void clear() {
        final AtomicReferenceArray<JmsInboundMessageDispatch> messages = this.messages;
        final long mask = this.mask;
        sharedClear(messages, mask);
    }

    @Override
    public String toString() {
        return "FifoMessageQueue size = " + size();
    }
}
