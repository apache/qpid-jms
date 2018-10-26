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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFacade;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the FIFO based message queue.
 */
public class FifoMessageQueueTest {

    private FifoMessageQueue queue;
    private final IdGenerator messageId = new IdGenerator();
    private long sequence;

    @Before
    public void setUp() {
        queue = new FifoMessageQueue(1000);
        queue.start();
    }

    @Test
    public void testToString() {
        assertNotNull(queue.toString());
    }

    @Test
    public void testCreate() {
        FifoMessageQueue queue = new FifoMessageQueue(1000);

        assertFalse(queue.isClosed());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isRunning());

        assertEquals(0, queue.size());
    }

    @Test
    public void testClose() {
        assertFalse(queue.isClosed());
        assertTrue(queue.isRunning());
        queue.close();
        assertTrue(queue.isClosed());
        assertFalse(queue.isRunning());
        queue.close();
    }

    @Test
    public void testDequeueNoWaitWhenQueueIsClosed() {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);

        assertFalse(queue.isEmpty());
        queue.close();
        assertSame(null, queue.dequeueNoWait());
    }

    @Test
    public void testDequeueWhenQueueIsClosed() throws InterruptedException {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);

        assertFalse(queue.isEmpty());
        queue.close();
        assertSame(null, queue.dequeue(1L));
    }

    @Test
    public void testDequeueWhenQueueIsStopped() throws InterruptedException {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);

        assertFalse(queue.isEmpty());
        queue.stop();
        assertFalse(queue.isRunning());
        assertSame(null, queue.dequeue(1L));
        queue.start();
        assertTrue(queue.isRunning());
        assertSame(message, queue.dequeue(1L));
    }

    @Test
    public void testDequeueNoWaitWhenQueueIsStopped() {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);

        assertFalse(queue.isEmpty());
        queue.stop();
        assertFalse(queue.isRunning());
        assertSame(null, queue.dequeueNoWait());
        queue.start();
        assertTrue(queue.isRunning());
        assertSame(message, queue.dequeueNoWait());
    }

    @Test
    public void testEnqueueFirst() {
        JmsInboundMessageDispatch message1 = createEnvelope();
        JmsInboundMessageDispatch message2 = createEnvelope();
        JmsInboundMessageDispatch message3 = createEnvelope();

        queue.enqueueFirst(message1);
        queue.enqueueFirst(message2);
        queue.enqueueFirst(message3);

        assertSame(message3, queue.dequeueNoWait());
        assertSame(message2, queue.dequeueNoWait());
        assertSame(message1, queue.dequeueNoWait());
    }

    @Test
    public void testClear() {
        List<JmsInboundMessageDispatch> messages = createFullRangePrioritySet();

        for (JmsInboundMessageDispatch envelope: messages) {
            queue.enqueue(envelope);
        }

        assertFalse(queue.isEmpty());
        queue.clear();
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testRemoveFirstOnEmptyQueue() {
        assertNull(queue.dequeueNoWait());
    }

    @Test
    public void testRemoveFirst() throws JMSException {
        List<JmsInboundMessageDispatch> messages = createFullRangePrioritySet();
        Collections.shuffle(messages);

        for (JmsInboundMessageDispatch envelope: messages) {
            queue.enqueue(envelope);
        }

        for (byte i = 0; i < 10; ++i) {
            JmsInboundMessageDispatch first = queue.dequeueNoWait();
            assertEquals(first, messages.get(i));
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testRemoveFirstSparse() throws JMSException {
        queue.enqueue(createEnvelope(9));
        queue.enqueue(createEnvelope(4));
        queue.enqueue(createEnvelope(1));

        JmsInboundMessageDispatch envelope = queue.dequeueNoWait();
        assertEquals(9, envelope.getMessage().getJMSPriority());
        envelope = queue.dequeueNoWait();
        assertEquals(4, envelope.getMessage().getJMSPriority());
        envelope = queue.dequeueNoWait();
        assertEquals(1, envelope.getMessage().getJMSPriority());

        assertTrue(queue.isEmpty());
    }

    @Test(timeout = 10000)
    public void testDequeueWaitsUntilMessageArrives() throws InterruptedException {
        final JmsInboundMessageDispatch message = createEnvelope();
        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                }
                queue.enqueueFirst(message);
            }
        });
        runner.start();

        assertSame(message, queue.dequeue(-1));
    }

    @Test(timeout = 10000)
    public void testDequeueWaitsUntilMessageArrivesWhenLockNotified() throws Exception {
        doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(-1);
    }

    @Test(timeout = 10000)
    public void testTimedDequeueWaitsUntilMessageArrivesWhenLockNotified() throws Exception {
        doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(100000);
    }

    private void doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(int timeout) throws Exception {
        final JmsInboundMessageDispatch message = createEnvelope();
        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                }

                try {
                    singalQueue(queue);
                } catch (Exception e1) {
                    return;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                }
                queue.enqueueFirst(message);
            }
        });
        runner.start();

        assertSame(message, queue.dequeue(timeout));
    }

    @Test(timeout = 10000)
    public void testDequeueReturnsWhenQueueIsStopped() throws InterruptedException {
        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                }
                queue.stop();
            }
        });
        runner.start();

        assertNull(queue.dequeue(-1));
    }

    @Test
    public void testRestartingClosedQueueHasNoEffect() throws InterruptedException {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);

        assertTrue(queue.isRunning());
        assertFalse(queue.isClosed());

        queue.stop();

        assertFalse(queue.isRunning());
        assertFalse(queue.isClosed());
        assertNull(queue.dequeue(1L));

        queue.close();

        assertTrue(queue.isClosed());
        assertFalse(queue.isRunning());

        queue.start();

        assertTrue(queue.isClosed());
        assertFalse(queue.isRunning());
        assertNull(queue.dequeue(1L));
    }

    private List<JmsInboundMessageDispatch> createFullRangePrioritySet() {
        List<JmsInboundMessageDispatch> messages = new ArrayList<JmsInboundMessageDispatch>();
        for (int i = 0; i < 10; ++i) {
            messages.add(createEnvelope(i));
        }
        return messages;
    }

    private JmsInboundMessageDispatch createEnvelope() {
        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(sequence++);
        envelope.setMessage(createMessage());
        return envelope;
    }

    private JmsInboundMessageDispatch createEnvelope(int priority) {
        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(sequence++);
        envelope.setMessage(createMessage(priority));
        return envelope;
    }

    private JmsMessage createMessage() {
        return createMessage(4);
    }

    private JmsMessage createMessage(int priority) {
        JmsTestMessageFacade facade = new JmsTestMessageFacade();
        facade.setMessageId(messageId.generateId());
        facade.setPriority((byte) priority);
        JmsMessage message = new JmsMessage(facade);

        return message;
    }

    private void singalQueue(FifoMessageQueue queue) throws Exception {
        Field lock = null;
        Field condition = null;
        Class<?> queueType = queue.getClass();

        while (queueType != null && lock == null) {
            try {
                lock = queueType.getDeclaredField("lock");
                condition = queueType.getDeclaredField("condition");
            } catch (NoSuchFieldException error) {
                queueType = queueType.getSuperclass();
                if (Object.class.equals(queueType)) {
                    queueType = null;
                }
            }
        }

        assertNotNull("MessageQueue implementation unknown", lock);
        lock.setAccessible(true);
        condition.setAccessible(true);

        ReentrantLock lockView = (ReentrantLock) lock.get(queue);
        Condition conditionView = (Condition) condition.get(queue);

        lockView.lock();
        try {
            conditionView.signal();
        } finally {
            lockView.unlock();
        }
    }
}
