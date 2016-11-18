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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Enumeration;

import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Test basic contract of QueueBroser implementation.
 */
public class JmsQueueBrowserTest extends JmsConnectionTestSupport {

    protected Session session;
    protected Queue queue;
    protected QueueBrowser browser;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockProvider();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue(_testName.getMethodName());
        browser = session.createBrowser(queue);
    }

    @Test(timeout = 30000)
    public void testCreateBrowser() throws Exception {
        QueueBrowser browser = session.createBrowser(queue);
        assertEquals(queue, browser.getQueue());
        assertNull(browser.getMessageSelector());
    }

    @Test(timeout = 30000)
    public void testCreateBrowserAndCloseTwice() throws Exception {
        browser = session.createBrowser(queue);
        browser.close();
        browser.close();  // Should not throw on multiple close.
    }

    @Test(timeout = 30000)
    public void testHasMoreElementsOnClosedBrowser() throws Exception {
        browser = session.createBrowser(queue);

        @SuppressWarnings("unchecked")
        Enumeration<Message> browse = browser.getEnumeration();

        assertFalse(browse.hasMoreElements());
        browser.close();
        assertFalse(browse.hasMoreElements());
    }

    @Test(timeout = 30000)
    public void testGetEnumerationClosedBrowser() throws Exception {
        browser = session.createBrowser(queue);

        browser.close();

        try {
            browser.getEnumeration();
            fail("Should throw an IllegalStateException");
        } catch (IllegalStateException ise) {}
    }
}
