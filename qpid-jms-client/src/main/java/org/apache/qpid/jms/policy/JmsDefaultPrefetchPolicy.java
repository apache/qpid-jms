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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the prefetch message policies for different types of consumers
 */
public class JmsDefaultPrefetchPolicy implements JmsPrefetchPolicy {

    public static final int MAX_PREFETCH_SIZE = Short.MAX_VALUE;
    public static final int DEFAULT_QUEUE_PREFETCH = 1000;
    public static final int DEFAULT_QUEUE_BROWSER_PREFETCH = DEFAULT_QUEUE_PREFETCH;
    public static final int DEFAULT_DURABLE_TOPIC_PREFETCH = DEFAULT_QUEUE_PREFETCH;
    public static final int DEFAULT_TOPIC_PREFETCH = DEFAULT_QUEUE_PREFETCH;

    private static final Logger LOG = LoggerFactory.getLogger(JmsDefaultPrefetchPolicy.class);

    private int queuePrefetch;
    private int queueBrowserPrefetch;
    private int topicPrefetch;
    private int durableTopicPrefetch;
    private int maxPrefetchSize = MAX_PREFETCH_SIZE;

    /**
     * Initialize default prefetch policies
     */
    public JmsDefaultPrefetchPolicy() {
        this.queuePrefetch = DEFAULT_QUEUE_PREFETCH;
        this.queueBrowserPrefetch = DEFAULT_QUEUE_BROWSER_PREFETCH;
        this.topicPrefetch = DEFAULT_TOPIC_PREFETCH;
        this.durableTopicPrefetch = DEFAULT_DURABLE_TOPIC_PREFETCH;
    }

    /**
     * Creates a new JmsPrefetchPolicy instance copied from the source policy.
     *
     * @param source
     *      The policy instance to copy values from.
     */
    public JmsDefaultPrefetchPolicy(JmsDefaultPrefetchPolicy source) {
        this.queuePrefetch = source.getQueuePrefetch();
        this.queueBrowserPrefetch = source.getQueueBrowserPrefetch();
        this.topicPrefetch = source.getTopicPrefetch();
        this.durableTopicPrefetch = source.getDurableTopicPrefetch();
        this.maxPrefetchSize = source.getMaxPrefetchSize();
    }

    @Override
    public JmsDefaultPrefetchPolicy copy() {
        return new JmsDefaultPrefetchPolicy(this);
    }

    @Override
    public int getConfiguredPrefetch(JmsSession session, JmsDestination destination, boolean durable, boolean browser) {
        int prefetch = 0;
        if (destination.isTopic()) {
            if (durable) {
                prefetch = getDurableTopicPrefetch();
            } else {
                prefetch = getTopicPrefetch();
            }
        } else {
            if (browser) {
                prefetch = getQueueBrowserPrefetch();
            } else {
                prefetch = getQueuePrefetch();
            }
        }

        return prefetch;
    }

    /**
     * @return Returns the durableTopicPrefetch.
     */
    public int getDurableTopicPrefetch() {
        return durableTopicPrefetch;
    }

    /**
     * Sets the durable topic prefetch value, this value is limited by the max
     * prefetch size setting.
     *
     * @param durableTopicPrefetch
     *        The durableTopicPrefetch to set.
     */
    public void setDurableTopicPrefetch(int durableTopicPrefetch) {
        this.durableTopicPrefetch = getMaxPrefetchLimit(durableTopicPrefetch);
    }

    /**
     * @return Returns the queuePrefetch.
     */
    public int getQueuePrefetch() {
        return queuePrefetch;
    }

    /**
     * @param queuePrefetch
     *        The queuePrefetch to set.
     */
    public void setQueuePrefetch(int queuePrefetch) {
        this.queuePrefetch = getMaxPrefetchLimit(queuePrefetch);
    }

    /**
     * @return Returns the queueBrowserPrefetch.
     */
    public int getQueueBrowserPrefetch() {
        return queueBrowserPrefetch;
    }

    /**
     * @param queueBrowserPrefetch
     *        The queueBrowserPrefetch to set.
     */
    public void setQueueBrowserPrefetch(int queueBrowserPrefetch) {
        this.queueBrowserPrefetch = getMaxPrefetchLimit(queueBrowserPrefetch);
    }

    /**
     * @return Returns the topicPrefetch.
     */
    public int getTopicPrefetch() {
        return topicPrefetch;
    }

    /**
     * @param topicPrefetch
     *        The topicPrefetch to set.
     */
    public void setTopicPrefetch(int topicPrefetch) {
        this.topicPrefetch = getMaxPrefetchLimit(topicPrefetch);
    }

    /**
     * Gets the currently configured max prefetch size value.
     * @return the currently configured max prefetch value.
     */
    public int getMaxPrefetchSize() {
        return maxPrefetchSize;
    }

    /**
     * Sets the maximum prefetch size value.
     *
     * @param maxPrefetchSize
     *        The maximum allowed value for any of the prefetch size options.
     */
    public void setMaxPrefetchSize(int maxPrefetchSize) {
        this.maxPrefetchSize = maxPrefetchSize;
    }

    /**
     * Sets the prefetch values for all options in this policy to the set limit.  If the value
     * given is larger than the max prefetch value of this policy the new limit will be capped
     * at the max prefetch value.
     *
     * @param prefetch
     *      The prefetch value to apply to all prefetch limits.
     */
    public void setAll(int prefetch) {
        this.durableTopicPrefetch = getMaxPrefetchLimit(prefetch);
        this.queueBrowserPrefetch = getMaxPrefetchLimit(prefetch);
        this.queuePrefetch = getMaxPrefetchLimit(prefetch);
        this.topicPrefetch = getMaxPrefetchLimit(prefetch);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + durableTopicPrefetch;
        result = prime * result + maxPrefetchSize;
        result = prime * result + queueBrowserPrefetch;
        result = prime * result + queuePrefetch;
        result = prime * result + topicPrefetch;
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

        JmsDefaultPrefetchPolicy other = (JmsDefaultPrefetchPolicy) obj;

        return this.queuePrefetch == other.queuePrefetch &&
               this.queueBrowserPrefetch == other.queueBrowserPrefetch &&
               this.topicPrefetch == other.topicPrefetch &&
               this.durableTopicPrefetch == other.durableTopicPrefetch;
    }

    private int getMaxPrefetchLimit(int value) {
        int result = Math.min(value, maxPrefetchSize);
        if (result < value) {
            LOG.warn("maximum prefetch limit has been reset from " + value + " to " + MAX_PREFETCH_SIZE);
        }
        return result;
    }
}
