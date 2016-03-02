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

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Test;

/**
 * Test various contract aspects of the TopicConnection implementation
 */
public class JmsTopicConnectionTest extends JmsConnectionTestSupport {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        topicConnection = createTopicConnectionToMockProvider();
        topicConnection.start();
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateConnectionConsumerOnTopicConnection() throws JMSException{
        topicConnection.createConnectionConsumer(new JmsQueue(), "subscriptionName", (ServerSessionPool)null, 1);
    }

    @Test(timeout = 30000, expected=IllegalStateException.class)
    public void testCreateQueueSessionOnTopicConnection() throws JMSException{
        topicConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}