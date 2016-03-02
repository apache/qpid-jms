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
package org.apache.qpid.jms.destinations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test functionality of Temporary Topics
 */
public class JmsTemporaryTopicTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsTemporaryTopicTest.class);

    @Test(timeout = 60000)
    public void testCreateTemporaryTopic() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        TemporaryTopic topic = session.createTemporaryTopic();
        session.createConsumer(topic);

        assertEquals(1, brokerService.getAdminView().getTemporaryTopics().length);
    }

    @Test(timeout = 60000)
    public void testCantConsumeFromTemporaryTopicCreatedOnAnotherConnection() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic tempTopic = session.createTemporaryTopic();
        session.createConsumer(tempTopic);

        Connection connection2 = createAmqpConnection();
        try {
            Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                session2.createConsumer(tempTopic);
                fail("should not be able to consumer from temporary topic from another connection");
            } catch (InvalidDestinationException ide) {
                // expected
            }
        } finally {
            connection2.close();
        }
    }

    @Test(timeout = 60000)
    public void testCantSendToTemporaryTopicFromClosedConnection() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic tempTopic = session.createTemporaryTopic();

        Connection connection2 = createAmqpConnection();
        try {
            Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message msg = session2.createMessage();
            MessageProducer producer = session2.createProducer(tempTopic);

            // Close the original connection
            connection.close();

            try {
                producer.send(msg);
                fail("should not be able to send to temporary topic from closed connection");
            } catch (IllegalStateException ide) {
                // expected
            }
        } finally {
            connection2.close();
        }
    }

    @Test(timeout = 60000)
    public void testCantDeleteTemporaryTopicWithConsumers() throws Exception {
        connection = createAmqpConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic tempTopic = session.createTemporaryTopic();
        MessageConsumer consumer = session.createConsumer(tempTopic);

        try {
            tempTopic.delete();
            fail("should not be able to delete temporary topic with active consumers");
        } catch (IllegalStateException ide) {
            // expected
        }

        consumer.close();

        // Now it should be allowed
        tempTopic.delete();
    }
}
