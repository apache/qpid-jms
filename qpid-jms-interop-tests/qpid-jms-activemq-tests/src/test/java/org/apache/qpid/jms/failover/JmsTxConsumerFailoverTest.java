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
package org.apache.qpid.jms.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;

/**
 * Test MessageConsumer behavior when in a TX and failover occurs.
 */
public class JmsTxConsumerFailoverTest extends AmqpTestSupport {

    @Override
    protected boolean isPersistent() {
        return true;
    }

    /*
     * Test that the TX doesn't start until the first ack so a failover
     * before that should allow Commit to work as expected.
     */
    @Test(timeout=60000)
    public void testTxConsumerReceiveAfterFailoverCommits() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, MSG_COUNT);
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        assertTrue("Should have a new connection.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        assertTrue("Should have a recovered consumer.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getQueueSubscribers().length == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(50)));

        for (int i = 0; i < MSG_COUNT; ++i) {
            Message received = consumer.receive(3000);
            assertNotNull("Mesage was not expected but not received", received);
        }

        try {
            session.commit();
            LOG.info("Transacted commit ok after failover.");
        } catch (TransactionRolledBackException rb) {
            fail("Session commit should not have failed with TX rolled back.");
        }

        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout=60000)
    public void testTxConsumerReceiveThenFailoverCommitFails() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, MSG_COUNT);
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT; ++i) {
            Message received = consumer.receive(3000);
            assertNotNull("Mesage was not expected but not received", received);
        }

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        try {
            LOG.info("Session commit firing after connection failed.");
            session.commit();
            fail("Session commit should have failed with TX rolled back.");
        } catch (TransactionRolledBackException rb) {
            LOG.info("Transacted commit failed after failover: {}", rb.getMessage());
        }

        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    @Test(timeout=60000)
    public void testTxConsumerRollbackAfterFailoverGetsNoErrors() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 5;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, MSG_COUNT);
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT; ++i) {
            Message received = consumer.receive(3000);
            assertNotNull("Mesage was not expected but not received", received);
        }

        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        try {
            LOG.info("Transacted being rolled back after failover");
            session.rollback();
            LOG.info("Transacted rollback after failover");
        } catch (JMSException ex) {
            LOG.info("Caught unexpected error: {}", ex.getMessage());
            fail("Session rollback should not have failed.");
        }

        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    /*
     * Tests that if some receives happen and then a failover followed by additional
     * receives the commit will fail and no messages are left on the broker.
     */
    @Test(timeout=60000)
    public void testTxConsumerReceiveWorksAfterFailoverButCommitFails() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 10;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, MSG_COUNT);
        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT / 2; ++i) {
            Message received = consumer.receive(3000);
            assertNotNull("Mesage was not expected but not received", received);
            LOG.info("consumer received message #{} - {}", i + 1, received.getJMSMessageID());
        }

        assertEquals(MSG_COUNT, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(MSG_COUNT, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT / 2; ++i) {
            Message received = consumer.receive(3000);
            assertNotNull("Mesage was not expected but not received", received);
            LOG.info("consumer received message #{} - {}", i + 1, received.getJMSMessageID());
        }

        try {
            session.commit();
            fail("Session commit should have failed with TX rolled back.");
        } catch (TransactionRolledBackException rb) {
            LOG.info("Transacted commit failed after failover: {}", rb.getMessage());
        }

        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }
}
