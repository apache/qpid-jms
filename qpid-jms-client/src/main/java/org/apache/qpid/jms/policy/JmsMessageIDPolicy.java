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
import org.apache.qpid.jms.message.JmsMessageIDBuilder;

/**
 * Interface for a policy that controls what kind of MessageID type is used for Messages
 * sent to a specific destination.
 */
public interface JmsMessageIDPolicy {

    /**
     * Copy this policy into a newly allocated instance.
     *
     * @return a new JmsMessageIDPolicy that is a copy of this one.
     */
    JmsMessageIDPolicy copy();

    /**
     * Returns the JmsMessageIDBuilder that should be used with the producer being created.
     *
     * @param session
     *      the Session that own the MessageProducer being created.
     * @param destination
     *      the Destination that the consumer will be subscribed to.
     *
     * @return the JmsMessageIDBuilder instance that is assigned to the new producer.
     */
    JmsMessageIDBuilder getMessageIDBuilder(JmsSession session, JmsDestination destination);

}
