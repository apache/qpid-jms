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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.Session;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;

// TODO find a way to make the test abort immediately if the TestAmqpPeer throws an exception
public class ConnectionIntegrationTest extends QpidJmsTestCase {
    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test(timeout=10000)
    public void testCreateAndCloseConnection() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout=10000)
    public void testCreateAutoAckSession() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectBegin(true);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Session should not be null", session);
        }
    }

    @Test(timeout=5000)
    public void testConnectionMetaDataVersion() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer(IntegrationTestFixture.PORT);) {
            Connection connection = testFixture.establishConnecton(testPeer);

            ConnectionMetaData meta = connection.getMetaData();
            assertTrue("Expected non-zero provider major version", meta.getProviderMajorVersion() != 0);
        }
    }
}
