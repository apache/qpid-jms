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

import java.util.concurrent.ThreadFactory;

/**
 * Simple ThreadFactory object
 */
public class QpidJMSThreadFactory implements ThreadFactory {

    private String threadName;
    private boolean daemon;

    /**
     * Creates a new Thread factory that will create threads with the
     * given name and daemon state.
     *
     * @param threadName
     * 		the name that will be used for each thread created.
     * @param daemon
     * 		should the created thread be a daemon thread.
     */
    public QpidJMSThreadFactory(String threadName, boolean daemon) {
        this.threadName = threadName;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable target) {
        Thread thread = new Thread(target, threadName);
        thread.setDaemon(daemon);
        return thread;
    }
}
