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

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;

/**
 * Test various client behaviors when the connection has gone offline.
 */
public class JmsOfflineBehaviorTests extends AmqpTestSupport {

    @Test(timeout=60000)
    public void testConnectionCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        stopPrimaryBroker();
        connection.close();
    }

    @Test(timeout=60000)
    public void testSessionCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        stopPrimaryBroker();
        session.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testClientAckDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Test"));

        Message message = consumer.receive(5000);
        assertNotNull(message);
        stopPrimaryBroker();
        message.acknowledge();

        connection.close();
    }

    @Test(timeout=60000)
    public void testProducerCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        stopPrimaryBroker();
        producer.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testConsumerCloseDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        stopPrimaryBroker();
        consumer.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testSessionCloseWithOpenResourcesDoesNotBlock() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI());
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue);
        session.createProducer(queue);

        stopPrimaryBroker();
        session.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testGetRemoteURI() throws Exception {

        startNewBroker();

        URI brokerURI = new URI(getAmqpFailoverURI() + "failover.randomize=false");
        final JmsConnection connection = (JmsConnection) createAmqpConnection(brokerURI);
        connection.start();

        URI connectedURI = connection.getConnectedURI();
        assertNotNull(connectedURI);

        final List<URI> brokers = getBrokerURIs();
        assertEquals(brokers.get(0), connectedURI);

        stopPrimaryBroker();

        assertTrue("Should connect to secondary URI.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                URI current = connection.getConnectedURI();
                if (current != null && current.equals(brokers.get(1))) {
                    return true;
                }

                return false;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        connection.close();
    }

    @Test(timeout=60000)
    public void testClosedReourcesAreNotRestored() throws Exception {
        URI brokerURI = new URI(getAmqpFailoverURI() + "?failover.maxReconnectDelay=500");
        Connection connection = createAmqpConnection(brokerURI);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        session.createConsumer(queue);
        session.createProducer(queue);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        stopPrimaryBroker();
        session.close();
        restartPrimaryBroker();

        assertTrue("Should have a new connection.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }
}
