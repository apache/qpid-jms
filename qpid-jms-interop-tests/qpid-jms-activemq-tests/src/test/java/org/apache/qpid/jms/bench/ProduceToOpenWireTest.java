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

import java.util.ArrayList;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class ProduceToOpenWireTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(ProduceToOpenWireTest.class);

    private final int MSG_COUNT = 50 * 1000;
    private final int NUM_RUNS = 40;

    @Test
    public void singleSendProfile() throws Exception {
        connection = createActiveMQConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage();
        message.setText("hello");
        producer.send(message);
    }

    @Test
    public void testProduceRateToTopic() throws Exception {

        connection = createActiveMQConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getDestinationName());

        // Warm Up the broker.
        produceMessages(topic, MSG_COUNT);

        List<Long> sendTimes = new ArrayList<Long>();
        long cumulative = 0;

        for (int i = 0; i < NUM_RUNS; ++i) {
            long result = produceMessages(topic, MSG_COUNT);
            sendTimes.add(result);
            cumulative += result;
            LOG.info("Time to send {} topic messages: {} ms", MSG_COUNT, result);
        }

        long smoothed = cumulative / NUM_RUNS;
        LOG.info("Smoothed send time for {} messages: {}", MSG_COUNT, smoothed);
    }

    @Test
    public void testProduceRateToQueue() throws Exception {

        connection = createActiveMQConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());

        // Warm Up the broker.
        produceMessages(queue, MSG_COUNT);

        QueueViewMBean queueView = getProxyToQueue(getDestinationName());
        queueView.purge();

        List<Long> sendTimes = new ArrayList<Long>();
        long cumulative = 0;

        for (int i = 0; i < NUM_RUNS; ++i) {
            long result = produceMessages(queue, MSG_COUNT);
            sendTimes.add(result);
            cumulative += result;
            LOG.info("Time to send {} queue messages: {} ms", MSG_COUNT, result);
            queueView.purge();
        }

        long smoothed = cumulative / NUM_RUNS;
        LOG.info("Smoothed send time for {} messages: {}", MSG_COUNT, smoothed);
    }

    protected long produceMessages(Destination destination, int msgCount) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage();
        message.setText("hello");

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < msgCount; ++i) {
            producer.send(message);
        }

        long result = (System.currentTimeMillis() - startTime);

        producer.close();
        return result;
    }
}
