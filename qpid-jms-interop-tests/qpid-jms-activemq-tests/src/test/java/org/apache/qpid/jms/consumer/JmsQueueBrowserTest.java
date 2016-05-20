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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Basic Queue Browser implementation.
 */
@SuppressWarnings("rawtypes")
public class JmsQueueBrowserTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsQueueBrowserTest.class);

    @Test(timeout = 40000)
    public void testCreateQueueBrowser() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        session.createConsumer(queue).close();

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 40000)
    public void testNoMessagesBrowserHasNoElements() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        session.createConsumer(queue).close();

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(0, proxy.getQueueSize());

        Enumeration enumeration = browser.getEnumeration();
        assertFalse(enumeration.hasMoreElements());
    }

    @Test(timeout=30000)
    public void testBroseOneInQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("hello"));
        producer.close();

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            assertTrue(m instanceof TextMessage);
            LOG.debug("Browsed message {} from Queue {}", m, queue);
        }

        browser.close();

        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(5000);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
    }

    @Test(timeout = 40000)
    public void testBrowseAllInQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(5, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
            TimeUnit.MILLISECONDS.sleep(50);
        }
        assertFalse(enumeration.hasMoreElements());
        assertEquals(5, count);
    }

    @Test(timeout = 40000)
    public void testBrowseAllInQueuePrefetchOne() throws Exception {
        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        ((JmsDefaultPrefetchPolicy) jmsConnection.getPrefetchPolicy()).setQueueBrowserPrefetch(1);

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(5, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
        }
        assertFalse(enumeration.hasMoreElements());
        assertEquals(5, count);
    }

    @Test(timeout = 40000)
    public void testBrowseAllInQueueZeroPrefetch() throws Exception {
        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        ((JmsDefaultPrefetchPolicy) jmsConnection.getPrefetchPolicy()).setQueueBrowserPrefetch(0);

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(5, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
        }
        assertFalse(enumeration.hasMoreElements());
        assertEquals(5, count);
    }

    @Test(timeout = 40000)
    public void testBrowseAllInQueueTxSession() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(5, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
        }
        assertFalse(enumeration.hasMoreElements());
        assertEquals(5, count);
    }

    @Test(timeout = 40000)
    public void testQueueBrowserInTxSessionLeavesOtherWorkUnaffected() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(5, proxy.getQueueSize());

        // Send some TX work but don't commit.
        MessageProducer txProducer = session.createProducer(queue);
        for (int i = 0; i < 5; ++i) {
            txProducer.send(session.createMessage());
        }

        assertEquals(5, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
        }

        assertFalse(enumeration.hasMoreElements());
        assertEquals(5, count);

        browser.close();

        // Now check that all browser work did not affect the session transaction.
        assertEquals(5, proxy.getQueueSize());
        session.commit();
        assertEquals(10, proxy.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testBrowseAllInQueueSmallPrefetch() throws Exception {
        connection = createAmqpConnection();
        ((JmsDefaultPrefetchPolicy) ((JmsConnection) connection).getPrefetchPolicy()).setQueueBrowserPrefetch(1);
        connection.start();

        final int MSG_COUNT = 30;

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(getDestinationName());
        sendToAmqQueue(MSG_COUNT);

        QueueViewMBean proxy = getProxyToQueue(getDestinationName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);
        Enumeration enumeration = browser.getEnumeration();
        int count = 0;
        while (enumeration.hasMoreElements()) {
            Message msg = (Message) enumeration.nextElement();
            assertNotNull(msg);
            LOG.debug("Recv: {}", msg);
            count++;
        }
        assertFalse(enumeration.hasMoreElements());
        assertEquals(MSG_COUNT, count);
    }
}
