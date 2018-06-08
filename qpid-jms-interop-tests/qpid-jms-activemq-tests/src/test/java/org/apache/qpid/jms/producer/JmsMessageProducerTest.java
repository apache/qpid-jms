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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Test;

/**
 * Test Various behaviors of the JMS MessageProducer implementation.
 */
public class JmsMessageProducerTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateMessageProducer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());
    }

    @Test(timeout = 30000)
    public void testProducerNotAuthorized() throws Exception {
        connection = createAmqpConnection("guest", "password");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("USERS.txQueue");
        try {
            session.createProducer(queue);
            fail("Should not be able to produce here.");
        } catch (JMSSecurityException jmsSE) {
            LOG.info("Caught expected exception");
        }
    }

    @Test(timeout = 20000)
    public void testSendMultipleMessagesPersistent() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        final int MSG_COUNT = 100;

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        byte[] payload = new byte[1024];
        Arrays.fill(payload, (byte) 255);

        for (int i = 0; i < MSG_COUNT; ++i) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            LOG.trace("sending message: {}", i);
            producer.send(message);
            LOG.trace("sent message: {}", i);
        }

        producer.close();

        assertEquals(MSG_COUNT, proxy.getQueueSize());
    }

    @Test(timeout = 20000)
    public void testSendMultipleMessagesNonPersistent() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        final int MSG_COUNT = 100;

        final QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        byte[] payload = new byte[1024];
        Arrays.fill(payload, (byte) 255);

        for (int i = 0; i < MSG_COUNT; ++i) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            LOG.trace("sending message: {}", i);
            producer.send(message);
            LOG.trace("sent message: {}", i);
        }

        assertTrue("Should all make it to the Queue.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return proxy.getQueueSize() == MSG_COUNT;
            }
        }));

        producer.close();
    }

    @Test(timeout = 20000)
    public void testPersistentSendsAreMarkedPersistent() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        Message message = session.createMessage();
        producer.send(message);

        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        message = consumer.receive(5000);
        assertNotNull(message);
        assertTrue(message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
    }

    @Test(timeout = 20000)
    public void testSendForeignMessage() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        String foreignPropKey = "myForeignMessageProp";
        String foreignPropValue = "ABC456XYZ";

        Connection activeMQConnection = createActiveMQConnection();
        try {
            Session activeMQSession = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Message foreignMessage = activeMQSession.createMessage();
            foreignMessage.setStringProperty(foreignPropKey, foreignPropValue);

            producer.send(foreignMessage);
        } finally {
            activeMQConnection.close();
        }

        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        Message receivedMessage = consumer.receive(5000);
        assertNotNull("Did not receive message as expected", receivedMessage);
        assertEquals("Unexpected property value", foreignPropValue, receivedMessage.getStringProperty(foreignPropKey));
    }

    @Test(timeout = 20000)
    public void testProducerWithNoTTLSendsMessagesWithoutTTL() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        Message message = session.createMessage();
        producer.send(message);

        assertEquals(1, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals(0, message.getJMSExpiration());
    }

    @Test(timeout = 20000)
    public void testProducerWithNoMessageIdCanBeConsumed() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDisableMessageID(true);

        QueueViewMBean proxy = getProxyToQueue(name.getMethodName());
        assertEquals(0, proxy.getQueueSize());

        for (int i = 0; i < 10; ++i) {
            Message message = session.createMessage();
            producer.send(message);
        }

        assertEquals(10, proxy.getQueueSize());

        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive(5000);
            assertNotNull(message);
            assertEquals(0, message.getJMSExpiration());
        }
    }

    private String createLargeString(int sizeInBytes) {
        byte[] base = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < sizeInBytes; i++) {
            builder.append(base[i % base.length]);
        }

        LOG.debug("Created string with size : " + builder.toString().getBytes().length + " bytes");
        return builder.toString();
    }

    @Test(timeout = 60000)
    public void testSendLargeMessage() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String queueName = name.toString();
        Queue queue = session.createQueue(queueName);

        MessageProducer producer = session.createProducer(queue);
        int messageSize = 1024 * 1024;
        String messageText = createLargeString(messageSize);
        Message m = session.createTextMessage(messageText);
        LOG.debug("Sending message of {} bytes on queue {}", messageSize, queueName);
        producer.send(m);

        MessageConsumer consumer = session.createConsumer(queue);

        Message message = consumer.receive();
        assertNotNull(message);
        assertTrue(message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        LOG.debug(">>>> Received message of length {}", textMessage.getText().length());
        assertEquals(messageSize, textMessage.getText().length());
        assertEquals(messageText, textMessage.getText());
    }

    @Test(timeout = 20000)
    public void testProducerWithTTL() throws Exception {
        doProducerWithTTLTestImpl(false, null);
    }

    @Test(timeout = 20000)
    public void testProducerWithTTLDisableTimestamp() throws Exception {
        doProducerWithTTLTestImpl(true, null);
    }

    @Test(timeout = 20000)
    public void testProducerWithTTLDisableTimestampAndNoAmqpTtl() throws Exception {
        doProducerWithTTLTestImpl(true, 0L);
    }

    private void doProducerWithTTLTestImpl(boolean disableTimestamp, Long propJMS_AMQP_TTL) throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Queue queue = session.createQueue(name.getMethodName());

        Message message = session.createMessage();
        if(propJMS_AMQP_TTL != null) {
            message.setLongProperty(AmqpMessageSupport.JMS_AMQP_TTL, propJMS_AMQP_TTL);
        }

        MessageProducer producer = session.createProducer(queue);
        if(disableTimestamp) {
            producer.setDisableMessageTimestamp(true);
        }
        producer.setTimeToLive(100);
        producer.send(message);

        TimeUnit.SECONDS.sleep(1);

        MessageConsumer consumer = session.createConsumer(queue);
        message = consumer.receive(150);
        if (message != null) {
            LOG.info("Unexpected message received: JMSExpiration = {} JMSTimeStamp = {} TTL = {}",
                    new Object[] { message.getJMSExpiration(), message.getJMSTimestamp(),
                    message.getJMSExpiration() - message.getJMSTimestamp()});
        }
        assertNull("Unexpected message received, see log for details", message);
    }
}
