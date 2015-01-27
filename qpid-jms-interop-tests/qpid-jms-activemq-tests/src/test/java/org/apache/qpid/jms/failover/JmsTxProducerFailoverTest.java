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
package org.apache.qpid.jms.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test MessageProducer behavior when in a TX and failover occurs.
 */
public class JmsTxProducerFailoverTest extends AmqpTestSupport {

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Test
    public void testTxProducerSendWorksButCommitFails() throws Exception {
        URI brokerURI = new URI("failover://("+ getBrokerAmqpConnectionURI() +")?maxReconnectDelay=1000");

        connection = createAmqpConnection(brokerURI);
        connection.start();

        final int MSG_COUNT = 20;
        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(name.getMethodName());
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        for (int i = 0; i < MSG_COUNT / 2; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        assertEquals(0, proxy.getQueueSize());

        stopPrimaryBroker();
        restartPrimaryBroker();

        proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        for (int i = MSG_COUNT / 2; i < MSG_COUNT; ++i) {
            LOG.debug("Producer sening message #{}", i + 1);
            producer.send(session.createTextMessage("Message: " + i));
        }

        try {
            session.commit();
            fail("Session commit should have failed with TX rolled back.");
        } catch (TransactionRolledBackException rb) {
            LOG.info("Transacted commit failed after failover: {}", rb.getMessage());
        }

        assertEquals(0, proxy.getQueueSize());
    }
}
