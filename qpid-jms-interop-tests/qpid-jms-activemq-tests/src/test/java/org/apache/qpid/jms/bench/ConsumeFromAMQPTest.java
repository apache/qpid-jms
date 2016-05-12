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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
@Ignore
public class ConsumeFromAMQPTest extends AmqpTestSupport {

    private final int MSG_COUNT = 50 * 1000;
    private final int NUM_RUNS = 10;

    @Override
    protected boolean isForceAsyncSends() {
        return true;
    }

    @Override
    protected boolean isForceSyncSends() {
        return false;
    }

    @Override
    protected String getAmqpTransformer() {
        return "raw";
    }

    @Override
    protected boolean isForceAsyncAcks() {
        return true;
    }

    @Override
    public String getAmqpConnectionURIOptions() {
        return "jms.presettlePolicy.presettleAll=true";
    }

    @Test
    public void oneConsumedForProfile() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage();
        message.setText("hello");
        producer.send(message);
        producer.close();

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        assertEquals("Queue should have a message", 1, queueView.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message received = consumer.receive(7000);
        assertNotNull(received);
        consumer.close();

        assertEquals("Queue should have ano messages", 0, queueView.getQueueSize());
    }

    @Test
    public void testConsumeRateFromQueue() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        // Warm Up the broker.
        produceMessages(queue, MSG_COUNT);
        consumerMessages(queue, MSG_COUNT);
        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        queueView.purge();

        List<Long> sendTimes = new ArrayList<Long>();
        long cumulative = 0;

        for (int i = 0; i < NUM_RUNS; ++i) {
            produceMessages(queue, MSG_COUNT);
            long result = consumerMessages(queue, MSG_COUNT);
            sendTimes.add(result);
            cumulative += result;
            LOG.info("Time to send {} topic messages: {} ms", MSG_COUNT, result);
            queueView.purge();
        }

        long smoothed = cumulative / NUM_RUNS;
        LOG.info("Smoothed send time for {} messages: {}", MSG_COUNT, smoothed);
    }

    @Test
    public void testConsumeRateFromQueueAsync() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        // Warm Up the broker.
        produceMessages(queue, MSG_COUNT);
        consumerMessagesAsync(queue, MSG_COUNT);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());

        List<Long> sendTimes = new ArrayList<Long>();
        long cumulative = 0;

        for (int i = 0; i < NUM_RUNS; ++i) {
            produceMessages(queue, MSG_COUNT);
            long result = consumerMessagesAsync(queue, MSG_COUNT);
            sendTimes.add(result);
            cumulative += result;
            LOG.info("Time to send {} topic messages: {} ms", MSG_COUNT, result);
            queueView.purge();
        }

        long smoothed = cumulative / NUM_RUNS;
        LOG.info("Smoothed send time for {} messages: {}", MSG_COUNT, smoothed);
    }

    protected long consumerMessages(Destination destination, int msgCount) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < msgCount; ++i) {
            Message message = consumer.receive(7000);
            assertNotNull("Failed to receive message " + i, message);
        }
        long result = (System.currentTimeMillis() - startTime);

        consumer.close();
        return result;
    }

    protected long consumerMessagesAsync(Destination destination, int msgCount) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch doneLatch = new CountDownLatch(MSG_COUNT);
        long startTime = System.currentTimeMillis();
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                doneLatch.countDown();
            }
        });
        assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
        long result = (System.currentTimeMillis() - startTime);

        consumer.close();
        return result;
    }

    protected void produceMessages(Destination destination, int msgCount) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage();
        message.setText("hello");

        for (int i = 0; i < msgCount; ++i) {
            producer.send(message);
        }

        producer.close();
    }

    @Override
    protected void configureBrokerPolicies(BrokerService broker) {
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policyEntry.setPrioritizedMessages(false);
        policyEntry.setExpireMessagesPeriod(0);
        policyEntry.setEnableAudit(false);
        policyEntry.setOptimizedDispatch(false);
        policyEntry.setQueuePrefetch(1000);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
    }
}
