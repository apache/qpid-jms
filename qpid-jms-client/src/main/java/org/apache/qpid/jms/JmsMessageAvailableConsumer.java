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

import javax.jms.MessageConsumer;

/**
 * Marker interface used for MessageConsumer instances that support sending
 * a notification event when a message has arrived when the consumer is not
 * in asynchronous dispatch mode.
 */
public interface JmsMessageAvailableConsumer {

    /**
     * Sets the listener used to notify synchronous consumers that there is a message
     * available so that the {@link MessageConsumer#receiveNoWait()} can be called.
     *
     * @param availableListener
     *        the JmsMessageAvailableListener instance to signal.
     */
    void setAvailableListener(JmsMessageAvailableListener availableListener);

    /**
     * Gets the listener used to notify synchronous consumers that there is a message
     * available so that the {@link MessageConsumer#receiveNoWait()} can be called.
     *
     * @return the currently configured message available listener instance.
     */
    JmsMessageAvailableListener getAvailableListener();

}
