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

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests MessageConsumer method contracts after the MessageConsumer is closed.
 */
public class JmsMessageConsumerClosedTest extends JmsConnectionTestSupport {

    protected MessageConsumer consumer;

    protected MessageConsumer createConsumer() throws Exception {
        connection = createConnectionToMockProvider();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testMethodName);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.close();
        return consumer;
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        consumer = createConsumer();
    }

    @Test
    @Timeout(30)
    public void testGetMessageSelectorFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.getMessageSelector();
        });
    }

    @Test
    @Timeout(30)
    public void testGetMessageListenerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.getMessageListener();
        });
    }

    @Test
    @Timeout(30)
    public void testSetMessageListenerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                }
            });
        });
    }

    @Test
    @Timeout(30)
    public void testRreceiveFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.receive();
        });
    }

    @Test
    @Timeout(30)
    public void testRreceiveTimedFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.receive(11);
        });
    }

    @Test
    @Timeout(30)
    public void testRreceiveNoWaitFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            consumer.receiveNoWait();
        });
    }

    @Test
    @Timeout(30)
    public void testClose() throws Exception {
        consumer.close();
    }
}
