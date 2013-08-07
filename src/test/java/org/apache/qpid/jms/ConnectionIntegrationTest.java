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
package org.apache.qpid.jms;

import static org.junit.Assert.assertNull;

import org.apache.qpid.jms.impl.ConnectionImpl;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;

// TODO find a way to make the test abort immediately if the TestAmqpPeer throws an exception
// TODO add tests such as testBrokerDoesNotRespond and testBrokerSendsWrongFrame
public class ConnectionIntegrationTest
{
    private static final int PORT = 25672;

    @Test
    public void testCreateConnection() throws Exception
    {
        try(TestAmqpPeer testPeer = new TestAmqpPeer(PORT);)
        {
            testPeer.expectPlainConnect("guest", "guest", true);
            testPeer.expectClose();

            ConnectionImpl connection =  new ConnectionImpl("clientName", "localhost", PORT, "guest", "guest");
            connection.connect();

            assertNull(testPeer.getException());

            connection.close();
        }
    }
}
