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

/**
 * Interface for a Redelivery Policy object used to determine how many times a Message
 * can be redelivered by the client before being dropped.
 */
public interface JmsRedeliveryPolicy {

    JmsRedeliveryPolicy copy();

    /**
     * Returns the configured maximum redeliveries that a message will be
     * allowed to have before it is rejected by this client for a given destination.
     * <p>
     * A return value of less than zero is treated as if there is no maximum value
     * set.
     *
     * @param destination
     *      the destination that the subscription is redelivering from.
     *
     * @return the maximum number of redeliveries allowed before a message is rejected.
     */
    int getMaxRedeliveries(JmsDestination destination);

    /**
     * Returns the configured outcome that will be used when rejecting the
     * message by this client for the given destination when the message has
     * reached the maximum redelivery threshold.
     *
     * The outcome returned here maps to AMQP outcomes using the following
     * integer values:
     *
     * <p><ul>
     *  <li>ACCEPTED = 1
     *  <li>REJECTED = 2
     *  <li>RELEASED = 3
     *  <li>MODIFIED_FAILED = 4
     *  <li>MODIFIED_FAILED_UNDELIVERABLE = 5
     * </ul><p>
     *
     * @param destination
     *      the destination that the subscription is redelivering from.
     *
     * @return the outcome to use when rejecting messages.
     */
    int getOutcome(JmsDestination destination);

}