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

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests TopicSubscriber method contracts after the TopicSubscriber is closed.
 */
public class JmsTopicSubscriberClosedTest extends JmsConnectionTestSupport {

    protected TopicSubscriber subscriber;

    protected void createTestResources() throws Exception {
        connection = createTopicConnectionToMockProvider();
        TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(_testName.getMethodName());
        subscriber = session.createSubscriber(destination);
        subscriber.close();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestResources();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testGetNoLocalFails() throws Exception {
        subscriber.getNoLocal();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testGetMessageListenerFails() throws Exception {
        subscriber.getMessageListener();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testGetMessageSelectorFails() throws Exception {
        subscriber.getMessageSelector();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testGetTopicFails() throws Exception {
        subscriber.getTopic();
    }
}
