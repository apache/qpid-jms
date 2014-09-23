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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test Connection methods contracts when state is closed.
 */
public class JmsConnectionClosedTest extends AmqpTestSupport {

    protected Destination destination;

    protected Connection createConnection() throws Exception {
        connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createTopic("test");
        connection.close();
        return connection;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createConnection();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetClientIdFails() throws JMSException {
        connection.getClientID();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetClientIdFails() throws JMSException {
        connection.setClientID("test");
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetMetaData() throws JMSException {
        connection.getMetaData();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testGetExceptionListener() throws JMSException {
        connection.getExceptionListener();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testSetExceptionListener() throws JMSException {
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
            }
        });
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testStartFails() throws JMSException {
        connection.start();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testStopFails() throws JMSException {
        connection.stop();
    }

    @Test(timeout=30000)
    public void testClose() throws JMSException {
        connection.close();
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateConnectionConsumerFails() throws JMSException {
        connection.createConnectionConsumer(destination, "", null, 1);
    }

    @Test(timeout=30000, expected=JMSException.class)
    public void testCreateDurableConnectionConsumerFails() throws JMSException {
        connection.createDurableConnectionConsumer((Topic) destination, "id", "", null, 1);
    }
}
