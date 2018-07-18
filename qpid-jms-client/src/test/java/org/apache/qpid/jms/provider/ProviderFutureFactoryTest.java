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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ProviderFutureFactoryTest {

    @Test
    public void testCreateFailsWithNullOptions() {
        try {
            ProviderFutureFactory.create(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCreateFailsWhenFutureTypeNotValid() {
        Map<String, String> options = new HashMap<>();

        options.put(ProviderFutureFactory.PROVIDER_FUTURE_TYPE_KEY, "super-fast");

        try {
            ProviderFutureFactory.create(options);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCreateFactoryWithNoConfigurationOptionsGiven() {
        ProviderFutureFactory factory = ProviderFutureFactory.create(Collections.emptyMap());

        ProviderFuture future = factory.createFuture();
        assertNotNull(future);
        assertFalse(future.isComplete());
    }

    @Test
    public void testCreateConservativeFactoryFromConfiguration() {
        Map<String, String> options = new HashMap<>();

        options.put(ProviderFutureFactory.PROVIDER_FUTURE_TYPE_KEY, "conservative");

        ProviderFutureFactory factory = ProviderFutureFactory.create(options);

        ProviderFuture future = factory.createFuture();
        assertNotNull(future);
        assertFalse(future.isComplete());

        assertTrue(future instanceof ConservativeProviderFuture);
    }

    @Test
    public void testCreateBalancedFactoryFromConfiguration() {
        Map<String, String> options = new HashMap<>();

        options.put(ProviderFutureFactory.PROVIDER_FUTURE_TYPE_KEY, "balanced");

        ProviderFutureFactory factory = ProviderFutureFactory.create(options);

        ProviderFuture future = factory.createFuture();
        assertNotNull(future);
        assertFalse(future.isComplete());

        assertTrue(future instanceof BalancedProviderFuture);
    }

    @Test
    public void testCreateProgressiveFactoryFromConfiguration() {
        Map<String, String> options = new HashMap<>();

        options.put(ProviderFutureFactory.PROVIDER_FUTURE_TYPE_KEY, "progressive");

        ProviderFutureFactory factory = ProviderFutureFactory.create(options);

        ProviderFuture future = factory.createFuture();
        assertNotNull(future);
        assertFalse(future.isComplete());

        assertTrue(future instanceof ProgressiveProviderFuture);
    }
}
