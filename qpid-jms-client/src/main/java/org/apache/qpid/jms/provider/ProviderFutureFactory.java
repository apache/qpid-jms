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

import java.util.Map;

/**
 * Factory for provider future instances that will create specific versions based on
 * configuration.
 */
public abstract class ProviderFutureFactory {

    public static final String PROVIDER_FUTURE_TYPE_KEY = "futureType";

    private static final String OS_NAME = System.getProperty("os.name");
    private static final String WINDOWS_OS_PREFIX = "Windows";
    private static final boolean IS_WINDOWS = isOsNameMatch(OS_NAME, WINDOWS_OS_PREFIX);

    private static final String CONSERVATIVE = "conservative";
    private static final String BALANCED = "balanced";
    private static final String PROGRESSIVE = "progressive";

    /**
     * Create a new Provider
     *
     * @param providerOptions
     * 		Configuration options to be consumed by this factory create method
     *
     * @return a new ProviderFutureFactory that will be used to create the desired future types.
     */
    public static ProviderFutureFactory create(Map<String, String> providerOptions) {
        String futureTypeKey = providerOptions.remove(PROVIDER_FUTURE_TYPE_KEY);

        if (futureTypeKey == null || futureTypeKey.isEmpty()) {
            if (Runtime.getRuntime().availableProcessors() < 4) {
                return new ConservativeProviderFutureFactory();
            } else if (isWindows()) {
                return new BalancedProviderFutureFactory();
            } else {
                return new ProgressiveProviderFutureFactory();
            }
        }

        switch (futureTypeKey.toLowerCase()) {
            case CONSERVATIVE:
                return new ConservativeProviderFutureFactory();
            case BALANCED:
                return new BalancedProviderFutureFactory();
            case PROGRESSIVE:
                return new ProgressiveProviderFutureFactory();
            default:
                throw new IllegalArgumentException(
                    "No ProviderFuture implementation with name " + futureTypeKey + " found");
        }
    }

    /**
     * @return a new ProviderFuture instance.
     */
    public abstract ProviderFuture createFuture();

    /**
     * @param synchronization
     * 		The {@link ProviderSynchronization} to assign to the returned {@link ProviderFuture}.
     *
     * @return a new ProviderFuture instance.
     */
    public abstract ProviderFuture createFuture(ProviderSynchronization synchronization);

    /**
     * @return a ProviderFuture that treats failures as success calls that simply complete the operation.
     */
    public abstract ProviderFuture createUnfailableFuture();

    //----- Internal support methods -----------------------------------------//

    private static boolean isWindows() {
        return IS_WINDOWS;
    }

    private static boolean isOsNameMatch(final String currentOSName, final String osNamePrefix) {
        if (currentOSName == null || currentOSName.isEmpty()) {
            return false;
        }

        return currentOSName.startsWith(osNamePrefix);
    }

    //----- ProviderFutureFactory implementation -----------------------------//

    private static class ConservativeProviderFutureFactory extends ProviderFutureFactory {

        @Override
        public ProviderFuture createFuture() {
            return new ConservativeProviderFuture();
        }

        @Override
        public ProviderFuture createFuture(ProviderSynchronization synchronization) {
            return new ConservativeProviderFuture(synchronization);
        }

        @Override
        public ProviderFuture createUnfailableFuture() {
            return new ConservativeProviderFuture() {

                @Override
                public void onFailure(ProviderException t) {
                    this.onSuccess();
                }
            };
        }
    }

    private static class BalancedProviderFutureFactory extends ProviderFutureFactory {

        @Override
        public ProviderFuture createFuture() {
            return new BalancedProviderFuture();
        }

        @Override
        public ProviderFuture createFuture(ProviderSynchronization synchronization) {
            return new BalancedProviderFuture(synchronization);
        }

        @Override
        public ProviderFuture createUnfailableFuture() {
            return new BalancedProviderFuture() {

                @Override
                public void onFailure(ProviderException t) {
                    this.onSuccess();
                }
            };
        }
    }

    private static class ProgressiveProviderFutureFactory extends ProviderFutureFactory {

        @Override
        public ProviderFuture createFuture() {
            return new ProgressiveProviderFuture();
        }

        @Override
        public ProviderFuture createFuture(ProviderSynchronization synchronization) {
            return new ProgressiveProviderFuture(synchronization);
        }

        @Override
        public ProviderFuture createUnfailableFuture() {
            return new ProgressiveProviderFuture() {

                @Override
                public void onFailure(ProviderException t) {
                    this.onSuccess();
                }
            };
        }
    }
}
