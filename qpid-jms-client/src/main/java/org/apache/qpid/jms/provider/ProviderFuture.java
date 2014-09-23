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
package org.apache.qpid.jms.provider;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.util.IOExceptionSupport;

/**
 * Asynchronous Provider Future class.
 */
public class ProviderFuture implements AsyncResult {

    protected final CountDownLatch latch = new CountDownLatch(1);
    protected Throwable error;
    protected final AsyncResult watcher;

    public ProviderFuture() {
        this.watcher = null;
    }

    public ProviderFuture(AsyncResult watcher) {
        this.watcher = watcher;
    }

    @Override
    public boolean isComplete() {
        return latch.getCount() == 0;
    }

    @Override
    public void onFailure(Throwable result) {
        error = result;
        latch.countDown();
        if (watcher != null) {
            watcher.onFailure(error);
        }
    }

    @Override
    public void onSuccess() {
        latch.countDown();
        if (watcher != null) {
            watcher.onSuccess();
        }
    }

    /**
     * Timed wait for a response to a Provider operation.
     *
     * @param amount
     *        The amount of time to wait before abandoning the wait.
     * @param unit
     *        The unit to use for this wait period.
     *
     * @return the result of this operation or null if the wait timed out.
     *
     * @throws IOException if an error occurs while waiting for the response.
     */
    public void sync(long amount, TimeUnit unit) throws IOException {
        try {
            latch.await(amount, unit);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw IOExceptionSupport.create(e);
        }
        failOnError();
    }

    /**
     * Waits for a response to some Provider requested operation.
     *
     * @return the response from the Provider for this operation.
     *
     * @throws IOException if an error occurs while waiting for the response.
     */
    public void sync() throws IOException {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw IOExceptionSupport.create(e);
        }
        failOnError();
    }

    private void failOnError() throws IOException {
        Throwable cause = error;
        if (cause != null) {
            throw IOExceptionSupport.create(cause);
        }
    }
}
