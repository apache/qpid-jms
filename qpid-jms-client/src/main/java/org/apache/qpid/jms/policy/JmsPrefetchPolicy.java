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
package org.apache.qpid.jms.policy;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;

/**
 * Interface for all Prefetch Policy implementations.  Allows for configuration of
 * MessageConsumer prefetch during creation.
 */
public interface JmsPrefetchPolicy {

    /**
     * Copy this policy into a newly allocated instance.
     *
     * @return a new JmsPrefetchPolicy that is a copy of this one.
     */
    JmsPrefetchPolicy copy();

    /**
     * Returns the prefetch value to use when creating a MessageConsumer instance.
     *
     * @param session
     *      the Session that own the MessageConsumer being created. (null for a ConnectionConsumer).
     * @param destination
     *      the Destination that the consumer will be subscribed to.
     * @param durable
     *      indicates if the subscription being created is a durable subscription (Topics only).
     * @param browser
     *      indicates if the subscription being created is a message browser (Queues only).
     *
     * @return the prefetch value to assign the MessageConsumer being created.
     */
    int getConfiguredPrefetch(JmsSession session, JmsDestination destination, boolean durable, boolean browser);

}