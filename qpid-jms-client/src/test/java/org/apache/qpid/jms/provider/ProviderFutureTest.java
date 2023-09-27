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
package org.apache.qpid.jms.provider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ProviderFutureTest {

    private ProviderFutureFactory futuresFactory;

    public static Collection<Object> data() {
        return Arrays.asList(new Object[] {
                 "conservative", "balanced", "progressive" }
           );
    }

    public void initProviderFutureTest(String futureTypeName) {
        Map<String, String> options = new HashMap<>();
        options.put("futureType", futureTypeName);

        futuresFactory = ProviderFutureFactory.create(options);
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    public void testIsComplete(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();

        assertFalse(future.isComplete());
        future.onSuccess();
        assertTrue(future.isComplete());
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testOnSuccess(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();

        future.onSuccess();
        try {
            future.sync();
        } catch (Exception cause) {
            fail("Should throw an error");
        }
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(90)
    public void testTimedSync(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();

        try {
            assertFalse(future.sync(1, TimeUnit.SECONDS));
        } catch (Exception cause) {
            fail("Should throw an error");
        }
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testOnFailure(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();
        ProviderException ex = new ProviderException("Failed");

        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (ProviderException cause) {
            assertSame(cause, ex);
        }
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testOnSuccessCallsSynchronization(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        ProviderFuture future = futuresFactory.createFuture(new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
                syncCalled.set(true);
            }

            @Override
            public void onPendingFailure(ProviderException cause) {
            }
        });

        future.onSuccess();
        try {
            future.sync(5, TimeUnit.SECONDS);
        } catch (ProviderException cause) {
            fail("Should throw an error");
        }

        assertTrue(syncCalled.get(), "Synchronization not called");
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testOnFailureCallsSynchronization(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        ProviderFuture future = futuresFactory.createFuture(new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
            }

            @Override
            public void onPendingFailure(ProviderException cause) {
                syncCalled.set(true);
            }
        });

        ProviderException ex = new ProviderException("Failed");

        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (ProviderException cause) {
            assertSame(cause, ex);
        }

        assertTrue(syncCalled.get(), "Synchronization not called");
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testSuccessfulStateIsFixed(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();
        ProviderException ex = new ProviderException("Failed");

        future.onSuccess();
        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
        } catch (ProviderException cause) {
            fail("Should throw an error");
        }
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testFailedStateIsFixed(String futureTypeName) {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();
        ProviderException ex = new ProviderException("Failed");

        future.onFailure(ex);
        future.onSuccess();
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (ProviderException cause) {
            assertSame(cause, ex);
        }
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testSyncHandlesInterruption(String futureTypeName) throws InterruptedException {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.sync();
                } catch (Exception cause) {
                    if (cause.getCause() instanceof InterruptedException) {
                        interrupted.set(true);
                    }
                } finally {
                    done.countDown();
                }
            }
        });
        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }

    @MethodSource("data")
    @ParameterizedTest(name = "{index}: futureType={0}")
    @Timeout(10)
    public void testTimedSyncHandlesInterruption(String futureTypeName) throws InterruptedException {
        initProviderFutureTest(futureTypeName);
        ProviderFuture future = futuresFactory.createFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.sync(20, TimeUnit.SECONDS);
                } catch (ProviderException cause) {
                    if (cause.getCause() instanceof InterruptedException) {
                        interrupted.set(true);
                    }
                } finally {
                    done.countDown();
                }
            }
        });
        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }
}
