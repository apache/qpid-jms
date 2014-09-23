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
package org.apache.qpid.jms.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.failover.FailoverProvider;
import org.apache.qpid.jms.provider.failover.FailoverProviderFactory;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Test basic functionality of the FailoverProvider class.
 */
public class FailoverProviderTest extends AmqpTestSupport {

    @Test(timeout=60000)
    public void testFailoverCreate() throws Exception {
        URI brokerURI = new URI("failover:" + getBrokerAmqpConnectionURI());
        Provider asyncProvider = FailoverProviderFactory.createAsync(brokerURI);
        assertNotNull(asyncProvider);
        FailoverProvider provider = (FailoverProvider) asyncProvider;
        assertNotNull(provider);
    }

    @Test(timeout=60000)
    public void testFailoverURIConfiguration() throws Exception {
        URI brokerURI = new URI("failover://(" + getBrokerAmqpConnectionURI() + ")" +
                                "?maxReconnectDelay=1000&useExponentialBackOff=false" +
                                "&maxReconnectAttempts=10&startupMaxReconnectAttempts=20");
        Provider asyncProvider = FailoverProviderFactory.createAsync(brokerURI);
        assertNotNull(asyncProvider);
        FailoverProvider provider = (FailoverProvider) asyncProvider;
        assertNotNull(provider);

        assertEquals(1000, provider.getMaxReconnectDelay());
        assertFalse(provider.isUseExponentialBackOff());
        assertEquals(10, provider.getMaxReconnectAttempts());
        assertEquals(20, provider.getStartupMaxReconnectAttempts());
    }

    @Test(timeout=60000)
    public void testStartupReconnectAttempts() throws Exception {
        URI brokerURI = new URI("failover://(amqp://localhost:61616)" +
                                "?maxReconnectDelay=100&startupMaxReconnectAttempts=5");
        Provider asyncProvider = FailoverProviderFactory.createAsync(brokerURI);
        assertNotNull(asyncProvider);
        FailoverProvider provider = (FailoverProvider) asyncProvider;
        assertNotNull(provider);

        assertEquals(100, provider.getMaxReconnectDelay());
        assertEquals(5, provider.getStartupMaxReconnectAttempts());

        final CountDownLatch failed = new CountDownLatch(1);

        provider.setProviderListener(new DefaultProviderListener() {

            @Override
            public void onConnectionFailure(IOException ex) {
                failed.countDown();
            }
        });

        provider.connect();

        assertTrue(failed.await(2, TimeUnit.SECONDS));
    }
}
