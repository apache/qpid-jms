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
package org.apache.qpid.jms;

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client uses a <CODE>QueueBrowser</CODE> object to look at messages on a queue without
 * removing them.
 * <p>
 * <p>
 * The <CODE>getEnumeration</CODE> method returns a <CODE>
 * java.util.Enumeration</CODE> that is used to scan the queue's messages. It may be an
 * enumeration of the entire content of a queue, or it may contain only the messages matching a
 * message selector.
 * <p>
 * <p>
 * Messages may be arriving and expiring while the scan is done. The JMS API does not require
 * the content of an enumeration to be a static snapshot of queue content. Whether these changes
 * are visible or not depends on the JMS provider.
 * <p>
 * <p>
 * A <CODE>QueueBrowser</CODE> can be created from either a <CODE>Session
 * </CODE> or a <CODE>QueueSession</CODE>.
 *
 * @see javax.jms.Session#createBrowser
 * @see javax.jms.QueueSession#createBrowser
 * @see javax.jms.QueueBrowser
 * @see javax.jms.QueueReceiver
 */
public class JmsQueueBrowser implements AutoCloseable, QueueBrowser, Enumeration<Message> {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsQueueBrowser.class);

    private final JmsSession session;
    private final JmsDestination destination;
    private final String selector;

    private volatile JmsMessageConsumer consumer;

    private Message next;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Constructor for an JmsQueueBrowser - used internally
     *
     * @param session
     *      The Session that owns this instance.
     * @param destination
     *      The Destination that will be browsed.
     * @param selector
     *      The selector string used to filter the browsed message.
     *
     * @throws javax.jms.JMSException if an error occurs while creating this instance.
     */
    protected JmsQueueBrowser(JmsSession session, JmsDestination destination, String selector) throws JMSException {
        this.session = session;
        this.destination = destination;
        this.selector = selector;
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
        createConsumer();

        return this;
    }

    /**
     * @return true if more messages to process
     */
    @Override
    public boolean hasMoreElements() {
        while (true) {
            MessageConsumer consumer = this.consumer;
            if (consumer == null) {
                return false;
            }

            if (next == null) {
                try {
                    next = consumer.receiveNoWait();
                } catch (JMSException e) {
                    LOG.warn("Error while receive the next message: {}", e.getMessage());
                }

                if (next != null) {
                    return true;
                } else {
                    destroyConsumer();
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    /**
     * @return the next message if one exists
     *
     * @throws NoSuchElementException if no more elements are available.
     */
    @Override
    public Message nextElement() {

        if (hasMoreElements()) {
            Message message = next;
            next = null;
            return message;
        }

        if (!session.isStarted()) {
            destroyConsumer();
            return null;
        }

        throw new NoSuchElementException();
    }

    @Override
    public void close() throws JMSException {
        if (closed.compareAndSet(false, true)) {
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

    @Override
    public String toString() {
        JmsMessageConsumer consumer = this.consumer;
        return "JmsQueueBrowser { value=" + (consumer != null ? consumer.getConsumerId() : "null") + " }";
    }

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The Consumer is closed");
        }
    }

    private synchronized void destroyConsumer() {
        synchronized (this) {
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (JMSException e) {
                LOG.trace("Error closing down internal consumer: ", e);
            } finally {
                consumer = null;
            }
        }
    }

    private synchronized void createConsumer() throws JMSException {
        if (consumer == null) {
            JmsMessageConsumer result = new JmsMessageConsumer(session.getNextConsumerId(), session, destination, selector, false) {

                @Override
                public boolean isBrowser() {
                    return true;
                }
            };
            result.init();

            // Assign only after fully created and initialized.
            consumer = result;
        }
    }
}
