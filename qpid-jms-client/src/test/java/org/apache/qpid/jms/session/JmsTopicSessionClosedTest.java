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
package org.apache.qpid.jms.session;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests behaviour after a TopicSession is closed.
 */
public class JmsTopicSessionClosedTest extends JmsConnectionTestSupport {

    private TopicSession session;
    private TopicPublisher publisher;
    private TopicSubscriber subscriber;
    private Topic destination;

    protected void createTestResources() throws Exception {
        connection = createTopicConnectionToMockProvider();

        session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createTopic(_testName.getMethodName());

        publisher = session.createPublisher(destination);
        subscriber = session.createSubscriber(destination);

        // Close the session explicitly, without closing the above.
        session.close();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestResources();
    }

    @Test(timeout=30000)
    public void testSessionCloseAgain() throws Exception {
        session.close();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreatePublisher() throws Exception {
        session.createPublisher(destination);
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreateSubscriber() throws Exception {
        session.createSubscriber(destination);
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreateSubscriberWithSelector() throws Exception {
        session.createSubscriber(destination, "color = blue", false);
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreateDurableSubscriber() throws Exception {
        session.createDurableSubscriber(destination, "foo");
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreateDurableSubscriberWithSelector() throws Exception {
        session.createDurableSubscriber(destination, "foo", "color = blue", false);
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testCreateDurableConsumerWithSelector() throws Exception {
        session.createDurableConsumer(destination, "foo", "color = blue", false);
    }

    @Test(timeout=30000)
    public void testSubscriberCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        subscriber.close();
    }

    @Test(timeout=30000)
    public void testPublisherCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        publisher.close();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testSubscriberGetTopicFails() throws Exception {
        subscriber.getTopic();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testPublisherGetTopicFails() throws Exception {
        publisher.getTopic();
    }
}
