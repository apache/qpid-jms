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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionListener;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test for Message priority ordering.
 */
public class JmsConsumerPriorityDispatchTest extends AmqpTestSupport {

    private final int MSG_COUNT = 10;

    @Test(timeout = 60000)
    public void testPrefetchedMessageArePriorityOrdered() throws Exception {

        final CountDownLatch received = new CountDownLatch(MSG_COUNT);

        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        jmsConnection.setLocalMessagePriority(true);
        jmsConnection.addConnectionListener(new JmsConnectionListener() {

            @Override
            public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                received.countDown();
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }

            @Override
            public void onSessionClosed(Session session, Throwable exception) {
            }

            @Override
            public void onConsumerClosed(MessageConsumer consumer, Throwable cause) {
            }

            @Override
            public void onProducerClosed(MessageProducer producer, Throwable cause) {
            }
        });

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);
        Message message = null;

        for (int i = 0; i < MSG_COUNT; i++) {
            message = session.createTextMessage();
            producer.setPriority(i);
            producer.send(message);
        }

        // We need to make sure that all messages are in the prefetch buffer.
        assertTrue("Failed to receive all messages", received.await(10, TimeUnit.SECONDS));

        for (int i = MSG_COUNT - 1; i >= 0; i--) {
            message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getJMSPriority());
        }
    }

    @Test(timeout = 60000)
    public void testPrefetchedMessageAreNotPriorityOrdered() throws Exception {
        // We are assuming that Broker side priority support is not enabled in the create broker
        // method in AmqpTestSupport.  If that changes then this test will sometimes fail.
        final CountDownLatch received = new CountDownLatch(MSG_COUNT);

        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        jmsConnection.addConnectionListener(new JmsConnectionListener() {

            @Override
            public void onInboundMessage(JmsInboundMessageDispatch envelope) {
                received.countDown();
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }

            @Override
            public void onSessionClosed(Session session, Throwable exception) {
            }

            @Override
            public void onConsumerClosed(MessageConsumer consumer, Throwable cause) {
            }

            @Override
            public void onProducerClosed(MessageProducer producer, Throwable cause) {
            }
        });

        assertFalse("Client side priority ordering expected to be disabled for this test",
                    jmsConnection.isLocalMessagePriority());

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);
        Message message = null;

        for (int i = 0; i < 10; i++) {
            message = session.createTextMessage();
            producer.setPriority(i);
            producer.send(message);
        }

        // We need to make sure that all messages are in the prefetch buffer.
        assertTrue("Failed to receive all messages", received.await(10, TimeUnit.SECONDS));

        for (int i = 0; i < 10; i++) {
            message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getJMSPriority());
        }
    }
}
