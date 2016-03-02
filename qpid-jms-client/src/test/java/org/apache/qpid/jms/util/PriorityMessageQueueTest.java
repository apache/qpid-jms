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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFacade;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the priority based message Queue
 */
public class PriorityMessageQueueTest {

    private MessageQueue queue;
    private final IdGenerator messageId = new IdGenerator();
    private long sequence;

    @Before
    public void setUp() {
        queue = new PriorityMessageQueue();
        queue.start();
    }

    @Test
    public void testGetLock() {
        assertNotNull(queue.getLock());
    }

    @Test
    public void testCreate() {
        PriorityMessageQueue queue = new PriorityMessageQueue();

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
    public void testDequeueWhenQueueIsNotEmpty() throws InterruptedException {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);
        assertFalse(queue.isEmpty());
        assertSame(message, queue.dequeue(1L));
    }

    @Test
    public void testDequeueZeroWhenQueueIsNotEmpty() throws InterruptedException {
        JmsInboundMessageDispatch message = createEnvelope();
        queue.enqueueFirst(message);
        assertFalse(queue.isEmpty());
        assertSame(message, queue.dequeue(0));
    }

    @Test
    public void testDequeueZeroWhenQueueIsEmpty() throws InterruptedException {
        assertTrue(queue.isEmpty());
        assertSame(null, queue.dequeue(0));
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
    public void testEnqueueFirstOverridesPriority() {
        // Add a higher priority message
        JmsInboundMessageDispatch message1 = createEnvelope(7);

        queue.enqueue(message1);

        // Add other lower priority messages 'first'.
        JmsInboundMessageDispatch message2 = createEnvelope(4);
        JmsInboundMessageDispatch message3 = createEnvelope(3);
        JmsInboundMessageDispatch message4 = createEnvelope(2);

        queue.enqueueFirst(message2);
        queue.enqueueFirst(message3);
        queue.enqueueFirst(message4);

        // Verify they dequeue in the reverse of the order
        // they were added, and not priority order.
        assertSame(message4, queue.dequeueNoWait());
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
    public void testRemoveAll() throws JMSException {
        List<JmsInboundMessageDispatch> messages = createFullRangePrioritySet();
        Collections.shuffle(messages);

        for (JmsInboundMessageDispatch envelope: messages) {
            queue.enqueue(envelope);
        }

        assertFalse(queue.isEmpty());
        List<JmsInboundMessageDispatch> result = queue.removeAll();
        assertTrue(queue.isEmpty());

        assertEquals(10, result.size());

        for (byte i = 9; i >= 0; --i) {
            JmsInboundMessageDispatch envelope = result.remove(0);
            assertEquals(i, envelope.getMessage().getJMSPriority());
        }
    }

    @Test
    public void testRemoveFirstOnEmptyQueue() {
        assertNull(queue.dequeueNoWait());
    }

    @Test
    public void testRemoveFirst() throws JMSException {
        List<JmsInboundMessageDispatch> messages = createFullRangePrioritySet();

        for (JmsInboundMessageDispatch envelope: messages) {
            queue.enqueue(envelope);
        }

        for (byte i = 9; i >= 0; --i) {
            JmsInboundMessageDispatch first = queue.dequeueNoWait();
            assertEquals(i, first.getMessage().getJMSPriority());
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

    @Test
    public void testPeekOnEmptyQueue() {
        assertNull(queue.peek());
    }

    @Test
    public void testPeekFirst() throws JMSException {
        List<JmsInboundMessageDispatch> messages = createFullRangePrioritySet();

        for (JmsInboundMessageDispatch envelope: messages) {
            queue.enqueue(envelope);
        }

        for (byte i = 9; i >= 0; --i) {
            JmsInboundMessageDispatch first = queue.peek();
            assertEquals(i, first.getMessage().getJMSPriority());
            queue.dequeueNoWait();
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPeekFirstSparse() throws JMSException {
        queue.enqueue(createEnvelope(9));
        queue.enqueue(createEnvelope(4));
        queue.enqueue(createEnvelope(1));

        JmsInboundMessageDispatch envelope = queue.peek();
        assertEquals(9, envelope.getMessage().getJMSPriority());
        queue.dequeueNoWait();
        envelope = queue.peek();
        assertEquals(4, envelope.getMessage().getJMSPriority());
        queue.dequeueNoWait();
        envelope = queue.peek();
        assertEquals(1, envelope.getMessage().getJMSPriority());
        queue.dequeueNoWait();

        assertTrue(queue.isEmpty());
    }

    @Test(timeout = 10000)
    public void testDequeueWaitsUntilMessageArrives() throws InterruptedException {
        doDequeueWaitsUntilMessageArrivesTestImpl(-1);
    }

    @Test(timeout = 10000)
    public void testDequeueTimedWaitsUntilMessageArrives() throws InterruptedException {
        doDequeueWaitsUntilMessageArrivesTestImpl(5000);
    }

    private void doDequeueWaitsUntilMessageArrivesTestImpl(int timeout) throws InterruptedException {
        final JmsInboundMessageDispatch message = createEnvelope();
        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
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
    public void testDequeueWaitsUntilMessageArrivesWhenLockNotified() throws InterruptedException {
        doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(-1);
    }

    @Test(timeout = 10000)
    public void testTimedDequeueWaitsUntilMessageArrivesWhenLockNotified() throws InterruptedException {
        doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(100000);
    }

    private void doDequeueWaitsUntilMessageArrivesWhenLockNotifiedTestImpl(int timeout) throws InterruptedException {
        final JmsInboundMessageDispatch message = createEnvelope();
        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                }
                synchronized (queue.getLock()) {
                    queue.getLock().notify();
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
}
