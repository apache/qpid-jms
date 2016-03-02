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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the basic contract of the TopicSubscriber
 */
public class JmsTopicSubscriberTest extends JmsConnectionTestSupport {

    protected TopicSession session;
    protected Topic topic;
    protected TopicSubscriber subscriber;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        topicConnection = createTopicConnectionToMockProvider();
        session = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(_testName.getMethodName());
        subscriber = session.createSubscriber(topic);
    }

    @Test(timeout = 30000)
    public void testMultipleCloseCalls() throws Exception {
        subscriber.close();
        subscriber.close();
    }

    @Test(timeout = 30000)
    public void testGetQueue() throws Exception {
        assertEquals(topic, subscriber.getTopic());
    }

    @Test(timeout = 30000)
    public void testGetMessageListener() throws Exception {
        assertNull(subscriber.getMessageListener());
        subscriber.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        assertNotNull(subscriber.getMessageListener());
    }

    @Test(timeout = 30000)
    public void testGetMessageSelector() throws Exception {
        assertNull(subscriber.getMessageSelector());
    }
}
