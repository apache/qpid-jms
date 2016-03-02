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
package org.apache.qpid.jms.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * test basic TopicPublisher functionality.
 */
public class JmsTopicPublisherTest extends AmqpTestSupport {

    @Test
    public void testCreateTopicPublisher() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        TopicConnection connection = factory.createTopicConnection();
        assertNotNull(connection);

        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session);
        Topic topic = session.createTopic(name.getMethodName());
        TopicPublisher publisher = session.createPublisher(topic);
        assertNotNull(publisher);

        TopicViewMBean proxy = getProxyToTopic(name.getMethodName());
        assertEquals(0, proxy.getEnqueueCount());
        connection.close();
    }
}
