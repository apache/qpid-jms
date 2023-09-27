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

import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests QueueReceiver method contracts after the QueueReceiver is closed.
 */
public class JmsQueueReceiverClosedTest extends JmsConnectionTestSupport {

    protected QueueReceiver receiver;

    protected void createTestResources() throws Exception {
        connection = createQueueConnectionToMockProvider();
        QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testMethodName);
        receiver = session.createReceiver(destination);
        receiver.close();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestResources();
    }

    @Test
    @Timeout(30)
    public void testGetMessageListenerFails() throws Exception {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> {
            receiver.getMessageListener();
        });
    }

    @Test
    @Timeout(30)
    public void testGetMessageSelectorFails() throws Exception {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> {
            receiver.getMessageSelector();
        });
    }

    @Test
    @Timeout(30)
    public void testGetTopicFails() throws Exception {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> {
            receiver.getQueue();
        });
    }
}
