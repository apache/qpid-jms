/**
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
package org.apache.qpid.jms;

import static org.junit.Assert.assertNotNull;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test basic Session functionality.
 */
public class JmsSessionTest extends AmqpTestSupport {

    @Test(timeout = 60000)
    public void testCreateSession() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        session.close();
    }

    @Test(timeout=30000)
    public void testSessionCreateProducer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        producer.close();
        session.close();
    }

    @Test(timeout=30000)
    public void testSessionCreateConsumer() throws Exception {
        connection = createAmqpConnection();
        assertNotNull(connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);

        Queue queue = session.createQueue("test.queue");
        MessageConsumer consumer = session.createConsumer(queue);

        consumer.close();
        session.close();
    }

    @Test(timeout=30000)
    public void testSessionDoubleCloseWithoutException() throws Exception {
        connection = createAmqpConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.close();
        session.close();
    }
}
