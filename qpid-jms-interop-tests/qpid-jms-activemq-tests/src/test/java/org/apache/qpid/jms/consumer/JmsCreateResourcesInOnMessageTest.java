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
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;

/**
 * Test the case where messages are sent and consumers are created in onMessage.
 */
public class JmsCreateResourcesInOnMessageTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateProducerInOnMessage() throws Exception {
        Connection connection = createAmqpConnection();
        connection.start();

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);
        final Queue forwardQ = session.createQueue(name.getMethodName() + "-forwarded");
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("TEST-MESSAGE"));
        producer.close();

        final QueueViewMBean proxy = getProxyToQueue(queue.getQueueName());
        assertEquals(1, proxy.getQueueSize());

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {

                try {
                    LOG.debug("Received async message: {}", message);
                    MessageProducer producer = session.createProducer(forwardQ);
                    producer.send(message);
                    LOG.debug("forwarded async message: {}", message);
                } catch (Throwable e) {
                    LOG.debug("Caught exception: {}", e);
                    throw new RuntimeException(e.getMessage());
                }
            }
        });

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));

        final QueueViewMBean proxy2 = getProxyToQueue(forwardQ.getQueueName());
        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy2.getQueueSize() == 1;
            }
        }));

        connection.close();
    }
}
