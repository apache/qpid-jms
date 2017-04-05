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

    private enum OUTCOME {
        ACCEPTED(JmsMessageSupport.ACCEPTED),
        REJECTED(JmsMessageSupport.REJECTED),
        RELEASED(JmsMessageSupport.RELEASED),
        MODIFIED_FAILED(JmsMessageSupport.MODIFIED_FAILED),
        MODIFIED_FAILED_UNDELIVERABLE(JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE);

        private final int outcome;

        private OUTCOME(int outcome) {
            this.outcome = outcome;
        }

        public int getOutcomeOrdinal() {
            return outcome;
        }

        public static OUTCOME getMappedOutcome(int value) {
            switch (value) {
                case JmsMessageSupport.ACCEPTED:
                    return ACCEPTED;
                case JmsMessageSupport.REJECTED:
                    return REJECTED;
                case JmsMessageSupport.RELEASED:
                    return RELEASED;
                case JmsMessageSupport.MODIFIED_FAILED:
                    return MODIFIED_FAILED;
                case JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE:
                    return MODIFIED_FAILED_UNDELIVERABLE;
                default:
                    throw new IllegalArgumentException("Specified outcome is not a legal value: " + value);
            }
        }
    };

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
     * Default acknowledgement type is Modified with Failed and Undeliverable here set to true.
     *
     * @return the default outcome used when rejecting messages.
     */
    public int getOutcome() {
        return outcome;
    }

    /**
     * Set the default outcome to use when rejecting messages using an numeric value, the
     * possible values are:
     *
     * <p><ul>
     *  <li>ACCEPTED = 1
     *  <li>REJECTED = 2
     *  <li>RELEASED = 3
     *  <li>MODIFIED_FAILED = 4
     *  <li>MODIFIED_FAILED_UNDELIVERABLE = 5
     * </ul><p>
     *
     * @param outcome
     * 		the default outcome applied to a rejected delivery.
     */
    public void setOutcome(int outcome) {
        this.outcome = OUTCOME.getMappedOutcome(outcome).getOutcomeOrdinal();
    }

    /**
     * Set the default outcome to use when rejecting messages using a string value which can
     * either be the string version of the numeric outcome values or the string name of the
     * desired outcome, the string names allowed are:
     *
     * <p><ul>
     *  <li>ACCEPTED
     *  <li>REJECTED
     *  <li>RELEASED
     *  <li>MODIFIED_FAILED
     *  <li>MODIFIED_FAILED_UNDELIVERABLE
     * </ul><p>
     *
     * @param outcome
     * 		the default outcome applied to a rejected delivery.
     */
    public void setOutcome(String outcome) {
        try {
            this.outcome = OUTCOME.valueOf(outcome.toUpperCase()).getOutcomeOrdinal();
        } catch (IllegalArgumentException iae) {
            try {
                setOutcome(Integer.parseInt(outcome));
            } catch (NumberFormatException e) {
                throw iae;
            }
        }
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
