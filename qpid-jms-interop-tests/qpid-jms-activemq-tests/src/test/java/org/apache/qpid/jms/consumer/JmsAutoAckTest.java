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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JmsAutoAckTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsAutoAckTest.class);

    @Test(timeout = 60000)
    public void testAckedMessageAreConsumed() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        sendToAmqQueue(1);

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        assertNotNull("Failed to receive any message.", consumer.receive(2000));

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testAckedMessageAreConsumedAsync() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        sendToAmqQueue(1);

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(1, proxy.getQueueSize());

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                LOG.debug("Received async message: {}", message);
            }
        });

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }
}
