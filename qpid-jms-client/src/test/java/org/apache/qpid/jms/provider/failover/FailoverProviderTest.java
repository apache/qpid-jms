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
package org.apache.qpid.jms.provider.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 * Test behavior of the FailoverProvider
 */
public class FailoverProviderTest {

    private final IdGenerator connectionIdGenerator = new IdGenerator();

    private List<URI> uris;

    @Before
    public void setUp() throws Exception {
        uris = new ArrayList<URI>();

        uris.add(new URI("mock://192.168.2.1:5672"));
        uris.add(new URI("mock://192.168.2.2:5672"));
        uris.add(new URI("mock://192.168.2.3:5672"));
        uris.add(new URI("mock://192.168.2.4:5672"));

        MockProviderFactory.resetStatistics();
    }

    @Test
    public void testCreateProvider() {
        FailoverProvider provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());
    }

    @Test(timeout = 30000)
    public void testConnectToMock() throws Exception {
        FailoverProvider provider = new FailoverProvider(uris, Collections.<String, String>emptyMap());
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, provider.isRandomize());
        assertNull(provider.getRemoteURI());

        final CountDownLatch connected = new CountDownLatch(1);

        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
                connected.countDown();
            }
        });

        provider.connect();

        ProviderFuture request = new ProviderFuture();
        provider.create(createConnectionInfo(), request);

        request.sync(10, TimeUnit.SECONDS);

        assertTrue(request.isComplete());

        provider.close();

        assertEquals(1, MockProviderFactory.AGGRAGATED_PROVIDER_STATS.getProvidersCreated());
        assertEquals(1, MockProviderFactory.AGGRAGATED_PROVIDER_STATS.getConnectionAttempts());
    }

    protected JmsConnectionInfo createConnectionInfo() {
        JmsConnectionId id = new JmsConnectionId(connectionIdGenerator.generateId());
        JmsConnectionInfo info = new JmsConnectionInfo(id);

        return info;
    }
}
