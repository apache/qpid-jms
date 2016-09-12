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
package org.apache.qpid.jms.provider.mock;

/**
 * Holds all behavioral configuration for a MockProvider
 */
public class MockProviderConfiguration {

    private boolean failOnConnect;
    private boolean failOnStart;
    private boolean failOnClose;

    private boolean delayCompletionCalls;

    public boolean isFailOnConnect() {
        return failOnConnect;
    }

    public void setFailOnConnect(boolean value) {
        this.failOnConnect = value;
    }

    public boolean isFailOnStart() {
        return failOnStart;
    }

    public void setFailOnStart(boolean value) {
        this.failOnStart = value;
    }

    public boolean isFailOnClose() {
        return failOnClose;
    }

    public void setFailOnClose(boolean value) {
        this.failOnClose = value;
    }

    public boolean isDelayCompletionCalls() {
        return delayCompletionCalls;
    }

    public void setDelayCompletionCalls(boolean delayCompletionCalls) {
        this.delayCompletionCalls = delayCompletionCalls;
    }
}
