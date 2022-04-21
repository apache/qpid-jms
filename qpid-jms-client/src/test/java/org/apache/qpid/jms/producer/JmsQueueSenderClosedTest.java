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

import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests QueueReceiver method contracts after the QueueReceiver is closed.
 */
public class JmsQueueSenderClosedTest extends JmsConnectionTestSupport {

    protected QueueSender sender;

    protected void createTestResources() throws Exception {
        connection = createQueueConnectionToMockProvider();
        QueueSession session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(_testName.getMethodName());
        sender = session.createSender(destination);
        sender.close();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestResources();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetDeliveryModeFails() throws Exception {
        sender.getDeliveryMode();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetDestinationFails() throws Exception {
        sender.getDestination();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetQueueFails() throws Exception {
        sender.getQueue();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetDisableMessageIDFails() throws Exception {
        sender.getDisableMessageID();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetDisableMessageTimestampFails() throws Exception {
        sender.getDisableMessageTimestamp();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetTimeToLiveFails() throws Exception {
        sender.getTimeToLive();
    }

    @Test(timeout=30000, expected=jakarta.jms.IllegalStateException.class)
    public void testGetPriorityFails() throws Exception {
        sender.getPriority();
    }
}
