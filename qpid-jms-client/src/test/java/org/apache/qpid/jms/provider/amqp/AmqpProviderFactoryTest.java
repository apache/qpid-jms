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
package org.apache.qpid.jms.provider.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test basic functionality of the AmqpProviderFactory
 */
public class AmqpProviderFactoryTest extends QpidJmsTestCase {

    private TestAmqpPeer testPeer;
    private URI peerURI;

    @Override
    @Before
    public void setUp() throws Exception {
        testPeer = new TestAmqpPeer();
        peerURI = new URI("amqp://localhost:" + testPeer.getServerPort());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (testPeer != null) {
            testPeer.close();
            testPeer = null;
        }
    }

    @Test(timeout = 20000)
    public void testGetName() throws IOException, Exception {
        AmqpProviderFactory factory = new AmqpProviderFactory();
        assertEquals("AMQP", factory.getName());
    }

    @Test(timeout = 20000)
    public void testCreateProvider() throws IOException, Exception {
        Provider provider = AmqpProviderFactory.create(peerURI);
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
    }

    @Test(timeout = 20000, expected=IllegalArgumentException.class)
    public void testCreateProviderFailsWithBadOption() throws IOException, Exception {
        URI badOptionsURI = new URI(peerURI.toString() + "?amqp.badOption=true");
        AmqpProviderFactory.create(badOptionsURI);
    }

    @Test(timeout = 20000, expected=IOException.class)
    public void testCreateProviderFailsWithMissingScheme() throws IOException, Exception {
        URI missingSchemeURI = new URI(null, null, peerURI.getHost(), peerURI.getPort(), null, null, null);
        AmqpProviderFactory.create(missingSchemeURI);
    }

    @Test(timeout = 20000)
    public void testCreateProviderHasDefaultIdleTimeoutValue() throws IOException, Exception {
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString()));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertTrue("No default idle timeout", amqpProvider.getIdleTimeout() > 0);
    }

    @Test(timeout = 20000)
    public void testCreateProviderAppliesIdleTimeoutURIOption() throws IOException, Exception {
        int timeout = 54321;
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString() + "?amqp.idleTimeout=" + timeout));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals("idle timeout option was not applied", timeout, amqpProvider.getIdleTimeout());
    }

    @Test(timeout = 20000)
    public void testCreateProviderAppliesMaxFrameSizeURIOption() throws IOException, Exception {
        int frameSize = 274893;
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString() + "?amqp.maxFrameSize=" + frameSize));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals("maxFrameSize option was not applied", frameSize, amqpProvider.getMaxFrameSize());
    }

    @Test(timeout = 20000)
    public void testCreateProviderAppliesOptions() throws IOException, Exception {
        URI configuredURI = new URI(peerURI.toString() +
            "?amqp.traceFrames=true" +
            "&amqp.traceBytes=true" +
            "&amqp.channelMax=32");
        Provider provider = AmqpProviderFactory.create(configuredURI);
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);

        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals(true, amqpProvider.isTraceBytes());
        assertEquals(true, amqpProvider.isTraceFrames());
        assertEquals(32, amqpProvider.getChannelMax());
    }

    @Test(timeout = 20000)
    public void testCreateProviderEncodedVhost() throws IOException, Exception {
        URI configuredURI = new URI(peerURI.toString() +
            "?amqp.vhost=" + URLEncoder.encode("v+host", "utf-8"));
        Provider provider = AmqpProviderFactory.create(configuredURI);
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);

        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals("v+host", amqpProvider.getVhost());
    }
}
