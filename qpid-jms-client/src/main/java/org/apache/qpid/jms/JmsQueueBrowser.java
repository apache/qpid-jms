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
package org.apache.qpid.jms;

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client uses a <CODE>QueueBrowser</CODE> object to look at messages on a queue without
 * removing them.
 * <p/>
 * <p/>
 * The <CODE>getEnumeration</CODE> method returns a <CODE>
 * java.util.Enumeration</CODE> that is used to scan the queue's messages. It may be an
 * enumeration of the entire content of a queue, or it may contain only the messages matching a
 * message selector.
 * <p/>
 * <p/>
 * Messages may be arriving and expiring while the scan is done. The JMS API does not require
 * the content of an enumeration to be a static snapshot of queue content. Whether these changes
 * are visible or not depends on the JMS provider.
 * <p/>
 * <p/>
 * A <CODE>QueueBrowser</CODE> can be created from either a <CODE>Session
 * </CODE> or a <CODE>QueueSession</CODE>.
 *
 * @see javax.jms.Session#createBrowser
 * @see javax.jms.QueueSession#createBrowser
 * @see javax.jms.QueueBrowser
 * @see javax.jms.QueueReceiver
 */
public class JmsQueueBrowser implements QueueBrowser, Enumeration<Message> {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsQueueBrowser.class);

    private final JmsSession session;
    private final JmsDestination destination;
    private final String selector;

    private JmsMessageConsumer consumer;
    private final AtomicBoolean browseDone = new AtomicBoolean(false);

    private Message next;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object semaphore = new Object();

    /**
     * Constructor for an JmsQueueBrowser - used internally
     *
     * @param session
     * @param id
     * @param destination
     * @param selector
     * @throws javax.jms.JMSException
     */
    protected JmsQueueBrowser(JmsSession session, JmsDestination destination, String selector) throws JMSException {
        this.session = session;
        this.destination = destination;
        this.selector = selector;
    }

    private void destroyConsumer() {
        if (consumer == null) {
            return;
        }
        try {
            if (session.getTransacted()) {
                session.commit();
            }
            consumer.close();
            consumer = null;
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets an enumeration for browsing the current queue messages in the order they would be
     * received.
     *
     * @return an enumeration for browsing the messages
     * @throws javax.jms.JMSException
     *         if the JMS provider fails to get the enumeration for this browser due to some
     *         internal error.
     */
    @Override
    public Enumeration<Message> getEnumeration() throws JMSException {
        checkClosed();
        if (consumer == null) {
            consumer = createConsumer();
        }
        return this;
    }

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The Consumer is closed");
        }
    }

    /**
     * @return true if more messages to process
     */
    @Override
    public boolean hasMoreElements() {
        while (true) {
            synchronized (this) {
                if (consumer == null) {
                    return false;
                }
            }

            if (next == null) {
                try {
                    next = consumer.receiveNoWait();
                } catch (JMSException e) {
                    LOG.warn("Error while receive the next message: {}", e.getMessage());
                    // TODO - Add client internal error listener.
                    // this.session.connection.onClientInternalException(e);
                }

                if (next != null) {
                    return true;
                }
            } else {
                return true;
            }

            if (browseDone.get() || !session.isStarted()) {
                destroyConsumer();
                return false;
            }

            waitForMessage();
        }
    }

    /**
     * @return the next message if one exists
     *
     * @throws NoSuchElementException if no more elements are available.
     */
    @Override
    public Message nextElement() {
        synchronized (this) {
            if (consumer == null) {
                return null;
            }
        }

        if (hasMoreElements()) {
            Message message = next;
            next = null;
            return message;
        }

        if (browseDone.get() || !session.isStarted()) {
            destroyConsumer();
            return null;
        }

        throw new NoSuchElementException();
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
            browseDone.set(true);
            destroyConsumer();
        }
    }

    /**
     * Gets the queue associated with this queue browser.
     *
     * @return the queue
     * @throws javax.jms.JMSException
     *         if the JMS provider fails to get the queue associated with this browser due to
     *         some internal error.
     */

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) destination;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return selector;
    }

    /**
     * Wait on a semaphore for a fixed amount of time for a message to come in.
     */
    protected void waitForMessage() {
        try {
            synchronized (semaphore) {
                semaphore.wait(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void notifyMessageAvailable() {
        synchronized (semaphore) {
            semaphore.notifyAll();
        }
    }

    @Override
    public String toString() {
        JmsMessageConsumer consumer = this.consumer;
        return "JmsQueueBrowser { value=" + (consumer != null ? consumer.getConsumerId() : "null") + " }";
    }

    private JmsMessageConsumer createConsumer() throws JMSException {
        browseDone.set(false);
        JmsMessageConsumer rc = new JmsMessageConsumer(session.getNextConsumerId(), session, destination, selector, false) {

            @Override
            public boolean isBrowser() {
                return true;
            }

            @Override
            public void onMessage(JmsInboundMessageDispatch envelope) {
                if (envelope.getMessage() == null) {
                    browseDone.set(true);
                } else {
                    super.onMessage(envelope);
                }
                notifyMessageAvailable();
            }
        };
        rc.init();
        return rc;
    }
}
