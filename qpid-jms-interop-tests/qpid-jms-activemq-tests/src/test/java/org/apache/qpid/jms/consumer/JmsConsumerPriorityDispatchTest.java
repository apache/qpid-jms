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
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for Message priority ordering.
 */
public class JmsConsumerPriorityDispatchTest extends AmqpTestSupport {

    private Connection connection;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createAmqpConnection();
    }

    @Test(timeout = 60000)
    public void testPrefetchedMessageArePriorityOrdered() throws Exception {
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

        // Wait for all sent to be dispatched.
        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getInFlightCount() == 10;
            }
        });

        // We need to make sure that all messages are in the prefetch buffer.
        TimeUnit.SECONDS.sleep(4);

        for (int i = 9; i >= 0; i--) {
            message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getJMSPriority());
        }
    }

    @Test(timeout = 60000)
    public void testPrefetchedMessageAreNotPriorityOrdered() throws Exception {
        // We are assuming that Broker side priority support is not enabled in the create
        // broker method in AmqpTestSupport.  If that changes then this test will sometimes
        // fail.
        ((JmsConnection) connection).setMessagePrioritySupported(false);
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

        // Wait for all sent to be dispatched.
        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getInFlightCount() == 10;
            }
        });

        // We need to make sure that all messages are in the prefetch buffer.
        TimeUnit.SECONDS.sleep(4);

        for (int i = 0; i < 10; i++) {
            message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getJMSPriority());
        }
    }
}
