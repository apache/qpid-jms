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
package org.apache.qpid.jms.provider.amqp;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test some basic functionality of the AmqpProvider
 */
public class AmqpProviderTest extends QpidJmsTestCase {

    private TestAmqpPeer testPeer;
    private URI peerURI;
    private AmqpProvider provider;

    @Override
    @Before
    public void setUp() throws Exception {
        testPeer = new TestAmqpPeer();
        peerURI = new URI("amqp://localhost:" + testPeer.getServerPort());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }

        if (testPeer != null) {
            testPeer.close();
            testPeer = null;
        }
    }

    @Test(timeout=10000)
    public void testCreate() {
        provider = new AmqpProvider(peerURI);
        assertFalse(provider.isPresettleConsumers());
    }

    @Test(timeout=10000)
    public void testConnectThrowsWhenNoPeer() throws Exception {
        provider = new AmqpProvider(peerURI);
        testPeer.close();
        try {
            provider.connect();
            fail("Should have failed to connect.");
        } catch (Exception ex) {
        }
    }

    @Test(timeout=10000)
    public void testToString() throws IOException {
        provider = new AmqpProvider(peerURI);
        provider.connect();
        assertTrue(provider.toString().contains("localhost"));
        assertTrue(provider.toString().contains(String.valueOf(peerURI.getPort())));
    }
}
