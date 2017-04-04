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
import org.apache.qpid.jms.message.JmsMessageSupport;

/**
 * Defines the policy used to manage redelivered and recovered Messages.
 */
public class JmsDefaultRedeliveryPolicy implements JmsRedeliveryPolicy {

    public static final int DEFAULT_MAX_REDELIVERIES = -1;
    public static final int DEFAULT_OUTCOME = JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE;

    private int maxRedeliveries;
    private int outcome;

    public JmsDefaultRedeliveryPolicy() {
        maxRedeliveries = DEFAULT_MAX_REDELIVERIES;
        outcome = DEFAULT_OUTCOME;
    }

    public JmsDefaultRedeliveryPolicy(JmsDefaultRedeliveryPolicy source) {
        maxRedeliveries = source.maxRedeliveries;
        outcome = source.outcome;
    }

    @Override
    public JmsDefaultRedeliveryPolicy copy() {
        return new JmsDefaultRedeliveryPolicy(this);
    }

    @Override
    public int getMaxRedeliveries(JmsDestination destination) {
        return maxRedeliveries;
    }

    @Override
    public int getOutcome(JmsDestination destination) {
        return outcome;
    }

    /**
     * Returns the configured default outcome that will be used when rejecting messages.
     * <p>
     * Default acknowledgement type is Modified with Undeliverable here set to true.
     *
     * @return the default outcome used when rejecting messages.
     */
    public int getOutcome() {
        return outcome;
    }

    /**
     * Set the default outcome to use when rejecting messages.
     *
     * @param outcome
     * 		the default outcome applied to a rejected delivery.
     */
    public void setOutcome(int outcome) {
        this.outcome = outcome;
    }

    /**
     * Returns the configured maximum redeliveries that a message will be
     * allowed to have before it is rejected by this client.
     *
     * @return the maximum number of redeliveries allowed before a message is rejected.
     */
    public int getMaxRedeliveries() {
        return maxRedeliveries;
    }

    /**
     * Configures the maximum number of time a message can be redelivered before it
     * will be rejected by this client.
     *
     * The default value of (-1) disables max redelivery processing.
     *
     * @param maxRedeliveries the maxRedeliveries to set
     */
    public void setMaxRedeliveries(int maxRedeliveries) {
        this.maxRedeliveries = maxRedeliveries;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + maxRedeliveries;
        result = prime * result + outcome;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        JmsDefaultRedeliveryPolicy other = (JmsDefaultRedeliveryPolicy) obj;

        return maxRedeliveries == other.maxRedeliveries && outcome == other.outcome;
    }
}
