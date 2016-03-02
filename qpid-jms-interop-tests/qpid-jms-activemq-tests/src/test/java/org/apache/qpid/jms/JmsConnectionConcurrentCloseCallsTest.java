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
package org.apache.qpid.jms;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Session;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

public class JmsConnectionConcurrentCloseCallsTest extends AmqpTestSupport {

    private JmsConnection connection;
    private ExecutorService executor;
    private final int size = 200;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        executor = Executors.newFixedThreadPool(20);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ex) {}

        if (executor != null) {
            executor.shutdownNow();
        }

        super.tearDown();
    }

    @Test(timeout=200000)
    public void testCloseMultipleTimes() throws Exception {
        connection = (JmsConnection) createAmqpConnection();
        connection.start();
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertTrue(connection.isStarted());
        assertFalse(connection.isClosed());

        final CountDownLatch latch = new CountDownLatch(size);

        for (int i = 0; i < size; i++) {
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        connection.close();
                        assertFalse(connection.isStarted());
                        assertTrue(connection.isClosed());
                        latch.countDown();
                    } catch (Throwable e) {
                        LOG.warn("Caught an exception: {}", e);
                    }
                }
            });
        }

        boolean zero = latch.await(200, TimeUnit.SECONDS);
        assertTrue("Should complete all", zero);

        // should not fail calling again
        connection.close();

        assertFalse(connection.isStarted());
        assertTrue(connection.isClosed());
    }
}
