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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ThreadFactory object
 */
public class QpidJMSThreadFactory implements ThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QpidJMSThreadFactory.class);

    private final String threadName;
    private final boolean daemon;
    private final AtomicReference<Thread> threadTracker;

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
        this.threadTracker = null;
    }

    /**
     * Creates a new Thread factory that will create threads with the
     * given name and daemon state.
     *
     * This constructor accepts an AtomicReference to track the Thread that
     * was last created from this factory.  This is most useful for a single
     * threaded executor where the Id of the internal execution thread needs
     * to be known for some reason.
     *
     * @param threadName
     * 		the name that will be used for each thread created.
     * @param daemon
     * 		should the created thread be a daemon thread.
     * @param threadTracker
     * 		AtomicReference that will be updated any time a new Thread is created.
     */
    public QpidJMSThreadFactory(String threadName, boolean daemon, AtomicReference<Thread> threadTracker) {
        this.threadName = threadName;
        this.daemon = daemon;
        this.threadTracker = threadTracker;
    }

    @Override
    public Thread newThread(final Runnable target) {
        Runnable runner = target;

        if (threadTracker != null) {
            runner = new Runnable() {

                @Override
                public void run() {
                    threadTracker.set(Thread.currentThread());

                    try {
                        target.run();
                    } finally {
                        threadTracker.set(null);
                    }
                }
            };
        }

        Thread thread = new Thread(runner, threadName);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread target, Throwable error) {
                LOG.warn("Thread: {} failed due to an uncaught exception: {}", target.getName(), error.getMessage());
                LOG.trace("Uncaught Stacktrace: ", error);
            }
        });

        return thread;
    }
}
