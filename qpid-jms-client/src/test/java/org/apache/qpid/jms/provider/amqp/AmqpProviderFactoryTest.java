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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;

import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test basic functionality of the AmqpProviderFactory
 */
public class AmqpProviderFactoryTest extends QpidJmsTestCase {

    private TestAmqpPeer testPeer;
    private URI peerURI;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        testPeer = new TestAmqpPeer();
        peerURI = new URI("amqp://localhost:" + testPeer.getServerPort());
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        if (testPeer != null) {
            testPeer.close();
            testPeer = null;
        }
    }

    @Test
    @Timeout(20)
    public void testGetName() throws IOException, Exception {
        AmqpProviderFactory factory = new AmqpProviderFactory();
        assertEquals("AMQP", factory.getName());
    }

    @Test
    @Timeout(20)
    public void testCreateProvider() throws IOException, Exception {
        Provider provider = AmqpProviderFactory.create(peerURI);
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
    }

    @Test
    @Timeout(20)
    public void testCreateProviderFailsWithBadOption() throws IOException, Exception {
        assertThrows(IllegalArgumentException.class, () -> {
            URI badOptionsURI = new URI(peerURI.toString() + "?amqp.badOption=true");
            AmqpProviderFactory.create(badOptionsURI);
        });
    }

    @Test
    @Timeout(20)
    public void testCreateProviderFailsWithMissingScheme() throws IOException, Exception {
        assertThrows(IOException.class, () -> {
            URI missingSchemeURI = new URI(null, null, peerURI.getHost(), peerURI.getPort(), null, null, null);
            AmqpProviderFactory.create(missingSchemeURI);
        });
    }

    @Test
    @Timeout(20)
    public void testCreateProviderHasDefaultIdleTimeoutValue() throws IOException, Exception {
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString()));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertTrue(amqpProvider.getIdleTimeout() > 0, "No default idle timeout");
    }

    @Test
    @Timeout(20)
    public void testCreateProviderAppliesIdleTimeoutURIOption() throws IOException, Exception {
        int timeout = 54321;
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString() + "?amqp.idleTimeout=" + timeout));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals(timeout, amqpProvider.getIdleTimeout(), "idle timeout option was not applied");
    }

    @Test
    @Timeout(20)
    public void testCreateProviderAppliesMaxFrameSizeURIOption() throws IOException, Exception {
        int frameSize = 274893;
        Provider provider = AmqpProviderFactory.create(new URI(peerURI.toString() + "?amqp.maxFrameSize=" + frameSize));
        assertNotNull(provider);
        assertTrue(provider instanceof AmqpProvider);
        AmqpProvider amqpProvider = (AmqpProvider) provider;

        assertEquals(frameSize, amqpProvider.getMaxFrameSize(), "maxFrameSize option was not applied");
    }

    @Test
    @Timeout(20)
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

    @Test
    @Timeout(20)
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
