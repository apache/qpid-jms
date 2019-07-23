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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

public class WrappedAsyncResultTest {

    private final ProviderFutureFactory futuresFactory = ProviderFutureFactory.create(Collections.emptyMap());

    @Test
    public void testCreateWithNull() {
        try {
            new WrappedAsyncResult(null) {};
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testGetWrapped() {
        ProviderFuture future = futuresFactory.createFuture();
        WrappedAsyncResult wrapped = new WrappedAsyncResult(future) {};

        assertSame(wrapped.getWrappedRequest(), future);
    }

    @Test
    public void testOnSuccessPassthrough() {
        ProviderFuture future = futuresFactory.createFuture();
        WrappedAsyncResult wrapped = new WrappedAsyncResult(future) {};

        assertFalse(future.isComplete());
        assertFalse(wrapped.isComplete());
        wrapped.onSuccess();
        assertTrue(wrapped.isComplete());
        assertTrue(future.isComplete());
    }

    @Test
    public void testOnFailurePassthrough() {
        ProviderFuture future = futuresFactory.createFuture();
        WrappedAsyncResult wrapped = new WrappedAsyncResult(future) {};

        assertFalse(future.isComplete());
        assertFalse(wrapped.isComplete());
        wrapped.onFailure(new ProviderException("Error"));
        assertTrue(wrapped.isComplete());
        assertTrue(future.isComplete());
    }
}
