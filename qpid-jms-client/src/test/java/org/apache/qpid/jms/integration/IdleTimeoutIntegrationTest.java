/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class IdleTimeoutIntegrationTest extends QpidJmsTestCase {

    @Test(timeout = 5000)
    public void testIdleTimeoutIsAdvertisedByDefault() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectAnonymousConnect(true, greaterThan(UnsignedInteger.valueOf(0)));
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin(true);

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 5000)
    public void testAdvertisedIdleTimeoutIsHalfOfActualTimeoutValue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int configuredTimeout = 54320;
            int advertisedValue = configuredTimeout / 2;

            testPeer.expectAnonymousConnect(true, equalTo(UnsignedInteger.valueOf(advertisedValue)));
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin(true);

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?amqp.IdleTimeout=" + configuredTimeout);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }
}
