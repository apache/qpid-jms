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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsMessageGroupTest extends AmqpTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageGroupTest.class);

    @Test(timeout = 60000)
    public void testGroupedMessagesDeliveredToOnlyOneConsumer() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        // Send the messages.
        for (int i = 0; i < 4; i++) {
            TextMessage message = session.createTextMessage("message " + i);
            message.setStringProperty("JMSXGroupID", "TEST-GROUP");
            message.setIntProperty("JMSXGroupSeq", i + 1);
            LOG.info("sending message: " + message);
            producer.send(message);
        }

        // All the messages should have been sent down connection 1.. just get
        // the first 3
        for (int i = 0; i < 3; i++) {
            TextMessage m1 = (TextMessage) consumer1.receive(3000);
            assertNotNull("m1 is null for index: " + i, m1);
            assertEquals(m1.getIntProperty("JMSXGroupSeq"), i + 1);
        }

        // Setup a second connection
        Connection connection1 = createAmqpConnection();
        connection1.start();
        Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queue);

        // Close the first consumer.
        consumer1.close();

        // The last messages should now go the the second consumer.
        for (int i = 0; i < 1; i++) {
            TextMessage m1 = (TextMessage) consumer2.receive(3000);
            assertNotNull("m1 is null for index: " + i, m1);
            assertEquals(m1.getIntProperty("JMSXGroupSeq"), 4 + i);
        }

        // assert that there are no other messages left for the consumer 2
        Message m = consumer2.receive(100);
        assertNull("consumer 2 has some messages left", m);
        connection1.close();
    }
}
