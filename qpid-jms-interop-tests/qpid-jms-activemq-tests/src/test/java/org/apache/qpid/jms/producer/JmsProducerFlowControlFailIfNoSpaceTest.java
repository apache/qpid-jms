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
package org.apache.qpid.jms.producer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests behavior of client when broker is configured to send an error back
 * when there is no space available for a send.
 */
public class JmsProducerFlowControlFailIfNoSpaceTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsProducerFlowControlFailIfNoSpaceTest.class);

    private List<Exception> exceptions;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        exceptions = new ArrayList<Exception>();
    }

    @Override
    protected void configureBrokerPolicies(BrokerService brokerService) {

        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(1);
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policy.setProducerFlowControl(true);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);

        brokerService.setDestinationPolicy(policyMap);
        brokerService.getSystemUsage().setSendFailIfNoSpace(true);
    }

    @Test
    public void testHandleSendFailIfNoSpaceSync() throws Exception {

        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        jmsConnection.setForceSyncSend(true);

        connection.setExceptionListener(new TestExceptionListener());
        connection.start();

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);

        producer.send(session.createTextMessage("Message:1"));
        try {
            producer.send(session.createTextMessage("Message:2"));
            fail("Should have failed to send message two");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception");
        }

        connection.close();
    }

    @Test
    public void testHandleSendFailIfNoSpaceAsync() throws Exception {

        connection = createAmqpConnection();

        JmsConnection jmsConnection = (JmsConnection) connection;
        jmsConnection.setForceSyncSend(false);

        connection.setExceptionListener(new TestExceptionListener());
        connection.start();

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer producer = session.createProducer(queue);

        producer.send(session.createTextMessage("Message:1"));
        producer.send(session.createTextMessage("Message:2"));

        assertTrue("Should have got an error from no space.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return !exceptions.isEmpty();
            }
        }));

        connection.close();
    }

    private class TestExceptionListener implements ExceptionListener {

        @Override
        public void onException(JMSException exception) {
            LOG.warn("Connection ExceptionListener fired: {}", exception.getMessage());
            exceptions.add(exception);
        }
    }
}
