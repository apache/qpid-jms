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
package org.apache.qpid.jms.util;

import java.util.concurrent.Executor;

/**
 * Simple executor implementation that ignores any requests to
 * execute a task.  This can be used in the case where an executor
 * should be returned or provided but the state of the application
 * or object is such that it will not process any new work and the
 * result of ignoring the request does not impact the application.
 */
public class NoOpExecutor implements Executor {

    public static final Executor INSTANCE = new NoOpExecutor();

    @Override
    public void execute(Runnable command) {

    }
}
