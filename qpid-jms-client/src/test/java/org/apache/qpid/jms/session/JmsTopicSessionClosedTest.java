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

import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

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
        destination = session.createTopic(_testMethodName);

        publisher = session.createPublisher(destination);
        subscriber = session.createSubscriber(destination);

        // Close the session explicitly, without closing the above.
        session.close();
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestResources();
    }

    @Test
    @Timeout(30)
    public void testSessionCloseAgain() throws Exception {
        session.close();
    }

    @Test
    @Timeout(30)
    public void testCreatePublisher() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createPublisher(destination);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSubscriber() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createSubscriber(destination);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateSubscriberWithSelector() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createSubscriber(destination, "color = blue", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableSubscriber() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createDurableSubscriber(destination, "foo");
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableSubscriberWithSelector() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createDurableSubscriber(destination, "foo", "color = blue", false);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableConsumerWithSelector() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            session.createDurableConsumer(destination, "foo", "color = blue", false);
        });
    }

    @Test
    @Timeout(30)
    public void testSubscriberCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        subscriber.close();
    }

    @Test
    @Timeout(30)
    public void testPublisherCloseAgain() throws Exception {
        // Close it again (closing the session should have closed it already).
        publisher.close();
    }

    @Test
    @Timeout(30)
    public void testSubscriberGetTopicFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            subscriber.getTopic();
        });
    }

    @Test
    @Timeout(30)
    public void testPublisherGetTopicFails() throws Exception {
        assertThrows(javax.jms.IllegalStateException.class, () -> {
            publisher.getTopic();
        });
    }
}
