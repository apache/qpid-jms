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
import static org.junit.Assert.assertNull;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class JmsZeroPrefetchTest extends AmqpTestSupport {

    @Test(timeout=60000, expected=JMSException.class)
    public void testCannotUseMessageListener() throws Exception {
        connection = createAmqpConnection();
        ((JmsConnection)connection).getPrefetchPolicy().setAll(0);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        MessageListener listener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        };

        consumer.setMessageListener(listener);
    }

    @Test(timeout = 60000)
    public void testPullConsumerWorks() throws Exception {
        connection = createAmqpConnection();
        ((JmsConnection)connection).getPrefetchPolicy().setAll(0);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello World!"));

        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive(5000);
        assertNotNull("Should have received a message!", answer);
        // check if method will return at all and will return a null
        answer = consumer.receive(1);
        assertNull("Should have not received a message!", answer);
        answer = consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    @Ignore // ActiveMQ doesn't honor link credit.
    @Test(timeout = 60000)
    public void testTwoConsumers() throws Exception {
        connection = createAmqpConnection();
        ((JmsConnection)connection).getPrefetchPolicy().setAll(0);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));

        // now lets receive it
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageConsumer consumer2 = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer1.receive(5000);
        assertNotNull(answer);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        answer = (TextMessage)consumer2.receive(5000);
        assertNotNull(answer);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");

        answer = (TextMessage)consumer2.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }
}
