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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
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

        assertNotNull("Failed to receive any message.", consumer.receive(3000));

        assertTrue("Queued message not consumed.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
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
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == 0;
            }
        }));
    }

    /**
     * Test use of session recovery while using an auto-ack session and
     * a message listener. Calling recover should result in delivery of the
     * current message again, followed by those that would have been received
     * afterwards.
     *
     * Send three messages. Consume the first message, then recover on the second
     * message and expect to see it again, ensure the third message is not seen
     * until after this.
     *
     * @throws Exception on error during test.
     */
    @Test(timeout = 60000)
    public void testRecoverInOnMessage() throws Exception {
        connection = createAmqpConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        sendMessages(connection, queue, 3);

        CountDownLatch latch = new CountDownLatch(1);
        AutoAckRecoverMsgListener listener = new AutoAckRecoverMsgListener(latch, session);
        consumer.setMessageListener(listener);

        connection.start();

        assertTrue("Timed out waiting for async listener", latch.await(10, TimeUnit.SECONDS));
        assertFalse("Test failed in listener, consult logs", listener.getFailed());
    }

    private static class AutoAckRecoverMsgListener implements MessageListener {
        final Session session;
        final CountDownLatch latch;
        private boolean seenFirstMessage = false;
        private boolean seenSecondMessage = false;
        private boolean seenSecondMessageTwice = false;
        private boolean complete = false;
        private boolean failed = false;

        public AutoAckRecoverMsgListener(CountDownLatch latch, Session session) {
            this.latch = latch;
            this.session = session;
        }

        @Override
        public void onMessage(Message message) {
            try {
                int msgNumProperty = message.getIntProperty(MESSAGE_NUMBER);

                if(complete ){
                    LOG.info("Test already finished, ignoring delivered message: " + msgNumProperty);
                    return;
                }

                if (msgNumProperty == 1) {
                    if (!seenFirstMessage) {
                        LOG.info("Received first message.");
                        seenFirstMessage = true;
                    } else {
                        LOG.error("Received first message again.");
                        complete(true);
                    }
                } else if (msgNumProperty == 2) {
                    if(!seenSecondMessage){
                        seenSecondMessage = true;
                        LOG.info("Received second message. Now calling recover()");
                        session.recover();
                    } else {
                        LOG.info("Received second message again as expected.");
                        seenSecondMessageTwice = true;
                        if(message.getJMSRedelivered()) {
                            LOG.info("Message was marked redelivered as expected.");
                        } else {
                            LOG.error("Message was not marked redelivered.");
                            complete(true);
                        }
                    }
                } else {
                    if (msgNumProperty != 3) {
                        LOG.error("Received unexpected message: " + msgNumProperty);
                        complete(true);
                        return;
                    }

                    if (!(seenFirstMessage && seenSecondMessageTwice)) {
                        LOG.error("Third message was not received in expected sequence.");
                        complete(true);
                        return;
                    }

                    LOG.info("Received third message.");

                    if(message.getJMSRedelivered()) {
                        LOG.error("Message was marked redelivered against expectation.");
                        complete(true);
                    } else {
                        LOG.info("Message was not marked redelivered, as expected.");
                        complete(false);
                    }
                }
            } catch (JMSException e) {
                LOG.error("Exception caught in listener", e);
                complete(true);
            }
        }

        public boolean getFailed() {
            return failed;
        }

        private void complete(boolean fail) {
            failed = fail;
            complete = true;
            latch.countDown();
        }
    }
}