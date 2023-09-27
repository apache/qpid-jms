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

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Tests TopicSubscriber method contracts after the TopicSubscriber is closed.
 */
public class JmsTopicPublisherClosedTest extends JmsConnectionTestSupport {

    protected TopicPublisher publisher;

    protected void createTestResources() throws Exception {
        connection = createTopicConnectionToMockProvider();
        TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(_testMethodName);
        publisher = session.createPublisher(destination);
        publisher.close();
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
            publisher.getDeliveryMode();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDestinationFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getDestination();
        });
    }

    @Test
    @Timeout(30)
    public void testGetTopicFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getTopic();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageIDFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getDisableMessageID();
        });
    }

    @Test
    @Timeout(30)
    public void testGetDisableMessageTimestampFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getDisableMessageTimestamp();
        });
    }

    @Test
    @Timeout(30)
    public void testGetTimeToLiveFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getTimeToLive();
        });
    }

    @Test
    @Timeout(30)
    public void testGetPriorityFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getPriority();
        });
    }
}
