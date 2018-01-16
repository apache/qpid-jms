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
 * Policy object that allows for configuration of options that affect when
 * a JMS MessageProducer will result in AMQP presettled message sends.
 */
public class JmsDefaultPresettlePolicy implements JmsPresettlePolicy {

    private boolean presettleAll;

    private boolean presettleProducers;
    private boolean presettleTopicProducers;
    private boolean presettleQueueProducers;
    private boolean presettleTransactedProducers;

    private boolean presettleConsumers;
    private boolean presettleTopicConsumers;
    private boolean presettleQueueConsumers;

    public JmsDefaultPresettlePolicy() {
    }

    public JmsDefaultPresettlePolicy(JmsDefaultPresettlePolicy source) {
        this.presettleAll = source.presettleAll;
        this.presettleProducers = source.presettleProducers;
        this.presettleTopicProducers = source.presettleTopicProducers;
        this.presettleQueueProducers = source.presettleQueueProducers;
        this.presettleTransactedProducers = source.presettleTransactedProducers;
        this.presettleConsumers = source.presettleConsumers;
        this.presettleTopicConsumers = source.presettleTopicConsumers;
        this.presettleQueueConsumers = source.presettleQueueConsumers;
    }

    @Override
    public JmsDefaultPresettlePolicy copy() {
        return new JmsDefaultPresettlePolicy(this);
    }

    @Override
    public boolean isConsumerPresttled(JmsSession session, JmsDestination destination) {
        if (session.isTransacted()) {
            return false;
        } else if (session.isNoAcknowledge()) {
            return true;
        } else if (destination != null && (presettleAll || presettleConsumers)) {
            return true;
        } else if (destination != null && destination.isQueue() && presettleQueueConsumers) {
            return true;
        } else if (destination != null && destination.isTopic() && presettleTopicConsumers) {
            return true;
        }

        return false;
    }

    @Override
    public boolean isProducerPresttled(JmsSession session, JmsDestination destination) {
        if (presettleAll || presettleProducers) {
            return true;
        } else if (destination != null && session.isTransacted() && presettleTransactedProducers) {
            return true;
        } else if (destination != null && destination.isQueue() && presettleQueueProducers) {
            return true;
        } else if (destination != null && destination.isTopic() && presettleTopicProducers) {
            return true;
        }

        return false;
    }

    /**
     * @return the presettleAll setting for this policy
     */
    public boolean isPresettleAll() {
        return presettleAll;
    }

    /**
     * Sets the presettle all sends option.  When true all MessageProducers
     * will send their messages presettled.
     *
     * @param presettleAll
     *      the presettleAll value to apply.
     */
    public void setPresettleAll(boolean presettleAll) {
        this.presettleAll = presettleAll;
    }

    /**
     * @return the presettleProducers setting for this policy.
     */
    public boolean isPresettleProducers() {
        return presettleAll || presettleProducers;
    }

    /**
     * Sets the the presettle all sends option.  When true all MessageProducers that
     * are created will send their messages as settled.
     *
     * @param presettleProducers
     *      the presettleProducers value to apply.
     */
    public void setPresettleProducers(boolean presettleProducers) {
        this.presettleProducers = presettleProducers;
    }

    /**
     * @return the presettleTopicProducers setting for this policy
     */
    public boolean isPresettleTopicProducers() {
        return presettleAll || presettleProducers ||  presettleTopicProducers;
    }

    /**
     * Sets the presettle Topic sends option.  When true any MessageProducer that
     * is created that sends to a Topic will send its messages presettled, and any
     * anonymous MessageProducer will send Messages that are sent to a Topic as
     * presettled as well.
     *
     * @param presettleTopicProducers
     *      the presettleTopicProducers value to apply.
     */
    public void setPresettleTopicProducers(boolean presettleTopicProducers) {
        this.presettleTopicProducers = presettleTopicProducers;
    }

    /**
     * @return the presettleQueueSends setting for this policy
     */
    public boolean isPresettleQueueProducers() {
        return presettleAll || presettleProducers ||  presettleQueueProducers;
    }

    /**
     * Sets the presettle Queue sends option.  When true any MessageProducer that
     * is created that sends to a Queue will send its messages presettled, and any
     * anonymous MessageProducer will send Messages that are sent to a Queue as
     * presettled as well.
     *
     * @param presettleQueueProducers
     *      the presettleQueueSends value to apply.
     */
    public void setPresettleQueueProducers(boolean presettleQueueProducers) {
        this.presettleQueueProducers = presettleQueueProducers;
    }

    /**
     * @return the presettleTransactedSends setting for this policy
     */
    public boolean isPresettleTransactedProducers() {
        return presettleAll || presettleProducers || presettleTransactedProducers;
    }

    /**
     * Sets the presettle in transactions option.  When true any MessageProducer that is
     * operating inside of a transacted session will send its messages presettled.
     *
     * @param presettleTransactedProducers the presettleTransactedSends to set
     */
    public void setPresettleTransactedProducers(boolean presettleTransactedProducers) {
        this.presettleTransactedProducers = presettleTransactedProducers;
    }

    /**
     * @return the presettleConsumers configuration value for this policy.
     */
    public boolean isPresettleConsumers() {
        return presettleAll || presettleConsumers;
    }

    /**
     * The presettle all consumers value to apply.  When true all MessageConsumer
     * instances created will indicate that presettled messages are requested.
     *
     * @param presettleConsumers
     *      the presettleConsumers value to apply to this policy.
     */
    public void setPresettleConsumers(boolean presettleConsumers) {
        this.presettleConsumers = presettleConsumers;
    }

    /**
     * @return the presettleTopicConsumers setting for this policy.
     */
    public boolean isPresettleTopicConsumers() {
        return presettleAll || presettleConsumers ||  presettleTopicConsumers;
    }

    /**
     * The presettle Topic consumers value to apply.  When true any MessageConsumer for
     * a Topic destination will indicate that presettled messages are requested.
     *
     * @param presettleTopicConsumers
     *      the presettleTopicConsumers value to apply to this policy.
     */
    public void setPresettleTopicConsumers(boolean presettleTopicConsumers) {
        this.presettleTopicConsumers = presettleTopicConsumers;
    }

    /**
     * @return the presettleQueueConsumers setting for this policy.
     */
    public boolean isPresettleQueueConsumers() {
        return presettleAll || presettleConsumers || presettleQueueConsumers;
    }

    /**
     * The presettle Queue consumers value to apply.  When true any MessageConsumer for
     * a Queue destination will indicate that presettled messages are requested.
     *
     * @param presettleQueueConsumers
     *      the presettleQueueConsumers value to apply to this policy.
     */
    public void setPresettleQueueConsumers(boolean presettleQueueConsumers) {
        this.presettleQueueConsumers = presettleQueueConsumers;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (presettleAll ? 1231 : 1237);
        result = prime * result + (presettleConsumers ? 1231 : 1237);
        result = prime * result + (presettleProducers ? 1231 : 1237);
        result = prime * result + (presettleQueueConsumers ? 1231 : 1237);
        result = prime * result + (presettleQueueProducers ? 1231 : 1237);
        result = prime * result + (presettleTopicConsumers ? 1231 : 1237);
        result = prime * result + (presettleTopicProducers ? 1231 : 1237);
        result = prime * result + (presettleTransactedProducers ? 1231 : 1237);
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

        JmsDefaultPresettlePolicy other = (JmsDefaultPresettlePolicy) obj;
        if (presettleAll != other.presettleAll) {
            return false;
        }
        if (presettleConsumers != other.presettleConsumers) {
            return false;
        }
        if (presettleProducers != other.presettleProducers) {
            return false;
        }
        if (presettleQueueConsumers != other.presettleQueueConsumers) {
            return false;
        }
        if (presettleQueueProducers != other.presettleQueueProducers) {
            return false;
        }
        if (presettleTopicConsumers != other.presettleTopicConsumers) {
            return false;
        }
        if (presettleTopicProducers != other.presettleTopicProducers) {
            return false;
        }
        if (presettleTransactedProducers != other.presettleTransactedProducers) {
            return false;
        }

        return true;
    }
}
