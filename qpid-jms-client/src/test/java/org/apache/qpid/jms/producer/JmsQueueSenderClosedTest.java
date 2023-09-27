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

import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests QueueReceiver method contracts after the QueueReceiver is closed.
 */
public class JmsQueueSenderClosedTest extends JmsConnectionTestSupport {

    protected QueueSender sender;

    protected void createTestResources() throws Exception {
        connection = createQueueConnectionToMockProvider();
        QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testMethodName);
        sender = session.createSender(destination);
        sender.close();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestResources();
    }

    @Test
    @Timeout(30)
    public void testGetDeliveryModeFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getDeliveryMode();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDestinationFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getDestination();
        });
    }

    @Test
    @Timeout(30)
    public void testGetQueueFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getQueue();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageIDFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getDisableMessageID();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageTimestampFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getDisableMessageTimestamp();
        });
    }

    @Test
    @Timeout(30)
    public void testGetTimeToLiveFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getTimeToLive();
        });
    }

    @Test
    @Timeout(30)
    public void testGetPriorityFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            sender.getPriority();
        });
    }
}
