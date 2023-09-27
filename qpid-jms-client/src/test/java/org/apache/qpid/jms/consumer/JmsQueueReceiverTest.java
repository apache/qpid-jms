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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test the basic contract of the QueueReceiver
 */
public class JmsQueueReceiverTest extends JmsConnectionTestSupport {

    protected QueueSession session;
    protected Queue queue;
    protected QueueReceiver receiver;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        queueConnection = createQueueConnectionToMockProvider();
        session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue(_testMethodName);
        receiver = session.createReceiver(queue);
    }

    @Test
    @Timeout(30)
    public void testMultipleCloseCalls() throws Exception {
        receiver.close();
        receiver.close();
    }

    @Test
    @Timeout(30)
    public void testGetQueue() throws Exception {
        assertEquals(queue, receiver.getQueue());
    }

    @Test
    @Timeout(30)
    public void testGetMessageListener() throws Exception {
        assertNull(receiver.getMessageListener());
        receiver.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        assertNotNull(receiver.getMessageListener());
    }

    @Test
    @Timeout(30)
    public void testGetMessageSelector() throws Exception {
        assertNull(receiver.getMessageSelector());
    }
}
