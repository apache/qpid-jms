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
package org.apache.qpid.jms;

import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test Connection methods contracts when state is closed.
 */
public class JmsConnectionClosedTest extends JmsConnectionTestSupport {

    protected Destination destination;

    protected JmsConnection createConnection() throws Exception {
        connection = createConnectionToMockProvider();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createTopic("test");
        connection.close();
        return connection;
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        connection = createConnection();
    }

    @Test
    @Timeout(30)
    public void testGetClientIdFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.getClientID();
        });
    }

    @Test
    @Timeout(30)
    public void testSetClientIdFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.setClientID("test");
        });
    }

    @Test
    @Timeout(30)
    public void testGetMetaData() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.getMetaData();
        });
    }

    @Test
    @Timeout(30)
    public void testGetExceptionListener() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.getExceptionListener();
        });
    }

    @Test
    @Timeout(30)
    public void testSetExceptionListener() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                }
            });
        });
    }

    @Test
    @Timeout(30)
    public void testStartFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.start();
        });
    }

    @Test
    @Timeout(30)
    public void testStopFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.stop();
        });
    }

    @Test
    @Timeout(30)
    public void testClose() throws Exception {
        connection.close();
    }

    @Test
    @Timeout(30)
    public void testCreateConnectionConsumerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.createConnectionConsumer(destination, "", null, 1);
        });
    }

    @Test
    @Timeout(30)
    public void testCreateDurableConnectionConsumerFails() throws Exception {
        assertThrows(JMSException.class, () -> {
            connection.createDurableConnectionConsumer((Topic) destination, "id", "", null, 1);
        });
    }
}
