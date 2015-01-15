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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Tests MessageConsumer method contracts after the MessageConsumer is closed.
 */
public class JmsMessageConsumerClosedTest extends AmqpTestSupport {

    protected MessageConsumer consumer;

    protected MessageConsumer createConsumer() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.close();
        return consumer;
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMessageSelectorFails() throws Exception {
        consumer = createConsumer();
        consumer.getMessageSelector();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMessageListenerFails() throws Exception {
        consumer = createConsumer();
        consumer.getMessageListener();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetMessageListenerFails() throws Exception {
        consumer = createConsumer();
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRreceiveFails() throws Exception {
        consumer = createConsumer();
        consumer.receive();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRreceiveTimedFails() throws Exception {
        consumer = createConsumer();
        consumer.receive(11);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testRreceiveNoWaitFails() throws Exception {
        consumer = createConsumer();
        consumer.receiveNoWait();
    }

    @Test(timeout=30000)
    public void testClose() throws Exception {
        consumer = createConsumer();
        consumer.close();
    }
}
