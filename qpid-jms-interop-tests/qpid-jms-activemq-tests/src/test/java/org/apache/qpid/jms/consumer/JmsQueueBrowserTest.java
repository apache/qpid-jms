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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Enumeration;

import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Basic Queue Browser implementation.
 */
public class JmsQueueBrowserTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsQueueBrowserTest.class);

    @Test(timeout = 60000)
    public void testCreateQueueBrowser() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue).close();

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
    }

    @SuppressWarnings("rawtypes")
    @Test(timeout = 60000)
    public void testNoMessagesBrowserHasNoElements() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue).close();

        QueueBrowser browser = session.createBrowser(queue);
        assertNotNull(browser);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        Enumeration enumeration = browser.getEnumeration();
        assertFalse(enumeration.hasMoreElements());
    }

    @SuppressWarnings("rawtypes")
    @Test(timeout = 60000)
    public void testBrowseAllInQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        sendToAmqQueue(5);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
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

    @SuppressWarnings("rawtypes")
    @Test(timeout = 90000)
    public void testBrowseAllInQueueSmallPrefetch() throws Exception {
        connection = createAmqpConnection();
        ((JmsConnection) connection).getPrefetchPolicy().setQueueBrowserPrefetch(10);
        connection.start();

        final int MSG_COUNT = 30;

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        sendToAmqQueue(MSG_COUNT);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
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
