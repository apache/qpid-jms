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
package org.apache.qpid.jms.destinations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Session;
import javax.jms.TemporaryTopic;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test functionality of Temporary Topics
 */
public class JmsTemporaryTopicTest extends AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsTemporaryTopicTest.class);

    // Temp Topics not yet supported on the Broker.
    @Ignore
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
}
