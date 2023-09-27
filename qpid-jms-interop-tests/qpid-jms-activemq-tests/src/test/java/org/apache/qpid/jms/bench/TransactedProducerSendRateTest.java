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
package org.apache.qpid.jms.bench;

import java.util.concurrent.TimeUnit;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Compares send rate using a TX Session for QPid JMS and ActiveMQ JMS
 */
@Disabled
public class TransactedProducerSendRateTest extends AmqpTestSupport {

    private final int ITERATIONS = 20;
    private final int BATCH_SIZE = 100;

    @Override
    protected boolean isAddOpenWireConnector() {
        return true;
    }

    @Override
    protected boolean isFrameTracingEnabled() {
        return true;
    }

    @Test
    public void testSendNonPersistentTopicMessagesAMQP() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createTopic(getDestinationName());
        MessageProducer producer = session.createProducer(destination);

        // Warm
        produceMessages(session, producer);

        long totalCycleTime = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            totalCycleTime += produceMessages(session, producer);
        }

        long smoothedTime = totalCycleTime / ITERATIONS;

        LOG.info("Total time for QPid client = {}", TimeUnit.NANOSECONDS.toMillis(smoothedTime));
    }

    @Test
    public void testSendNonPersistentTopicMessagesOpenWire() throws Exception {
        jmsConnection = createActiveMQConnection();
        javax.jms.Session session = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
        javax.jms.Destination destination = session.createTopic(getDestinationName());
        javax.jms.MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Warm
        produceMessages(session, producer);

        long totalCycleTime = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            totalCycleTime += produceMessages(session, producer);
        }

        long smoothedTime = totalCycleTime / ITERATIONS;

        LOG.info("Total time for ActiveMQ client = {}", TimeUnit.NANOSECONDS.toMillis(smoothedTime));
    }

    @Test
    public void testSendNonPersistentQueueMessagesAMQP() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(destination);
        QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        // Warm
        produceMessages(session, producer);

        long totalCycleTime = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            totalCycleTime += produceMessages(session, producer);
            queueView.purge();
        }

        long smoothedTime = totalCycleTime / ITERATIONS;

        LOG.info("Total time for QPid client = {}", TimeUnit.NANOSECONDS.toMillis(smoothedTime));
    }

    @Test
    public void testSendNonPersistentQueueMessagesOpenWire() throws Exception {
        jmsConnection = createActiveMQConnection();
        javax.jms.Session session = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
        javax.jms.Destination destination = session.createQueue(getDestinationName());
        javax.jms.MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        // Warm
        produceMessages(session, producer);

        long totalCycleTime = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            totalCycleTime += produceMessages(session, producer);
            queueView.purge();
        }

        long smoothedTime = totalCycleTime / ITERATIONS;

        LOG.info("Total time for ActiveMQ client = {}", TimeUnit.NANOSECONDS.toMillis(smoothedTime));
    }

    // Send under TX - Count commit in elapsed time.
    private long produceMessages(Session session, MessageProducer producer) throws Exception {
        Message message = session.createTextMessage("payload");

        long start = System.nanoTime();
        for (int i = 0; i < BATCH_SIZE; ++i) {
            producer.send(message);
        }

        if (session.getTransacted()) {
            session.commit();
        }
        long elapsed = System.nanoTime() - start;

        return elapsed;
    }

    private long produceMessages(javax.jms.Session session, javax.jms.MessageProducer producer) throws Exception {
        javax.jms.Message message = session.createTextMessage("payload");

        long start = System.nanoTime();
        for (int i = 0; i < BATCH_SIZE; ++i) {
            producer.send(message);
        }

        if (session.getTransacted()) {
            session.commit();
        }
        long elapsed = System.nanoTime() - start;

        return elapsed;
    }
}
