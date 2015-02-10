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
package org.apache.qpid.jms.provider.failover;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.provider.mock.MockProviderContext;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that calls into the FailoverProvider when it is not connected works
 * as expected based on the call and the resource type in question.
 */
public class FailoverProviderOfflineBehaviorTest extends QpidJmsTestCase {

    private final JmsConnectionFactory factory = new JmsConnectionFactory("failover:(mock://localhost)");

    private JmsConnection connection;

    // TODO - Should add a wait for true connection interruption.

    @Override
    @Before
    public void setUp() throws Exception {
        MockProviderContext.INSTANCE.reset();
        super.tearDown();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    @Test(timeout=60000)
    public void testConnectionCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();
        MockProviderContext.INSTANCE.shutdown();
        connection.close();
    }

    @Test(timeout=60000)
    public void testSessionCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MockProviderContext.INSTANCE.shutdown();
        session.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testProducerCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testName.getMethodName());
        MessageProducer producer = session.createProducer(queue);

        MockProviderContext.INSTANCE.shutdown();
        producer.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testConsumerCloseDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testName.getMethodName());
        MessageConsumer consumer = session.createConsumer(queue);

        MockProviderContext.INSTANCE.shutdown();
        consumer.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testSessionCloseWithOpenResourcesDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(_testName.getMethodName());
        session.createConsumer(queue);
        session.createProducer(queue);

        MockProviderContext.INSTANCE.shutdown();
        session.close();
        connection.close();
    }

    @Test(timeout=60000)
    public void testSessionRecoverDoesNotBlock() throws Exception {
        connection = (JmsConnection) factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MockProviderContext.INSTANCE.shutdown();
        session.recover();
        connection.close();
    }
}
