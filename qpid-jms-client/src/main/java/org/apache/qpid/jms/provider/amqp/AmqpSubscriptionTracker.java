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
package org.apache.qpid.jms.provider.amqp;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SUB_NAME_DELIMITER;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSRuntimeException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsConsumerInfo;

/**
 * Class used to track named subscriptions on a connection to allow verifying
 * current usage and assigning appropriate link names.
 */
public class AmqpSubscriptionTracker {

    Set<String> exclusiveDurableSubs = new HashSet<>();
    Map<String, SubDetails> sharedDurableSubs = new HashMap<>();
    Map<String, SubDetails> sharedVolatileSubs = new HashMap<>();

    public String reserveNextSubscriptionLinkName(String subscriptionName, JmsConsumerInfo consumerInfo) {
        validateSubscriptionName(subscriptionName);

        if(consumerInfo == null) {
            throw new IllegalArgumentException("Consumer info must not be null.");
        }

        if (consumerInfo.isShared()) {
            if (consumerInfo.isDurable()) {
                return getSharedDurableSubLinkName(subscriptionName, consumerInfo);
            } else {
                return getSharedVolatileSubLinkName(subscriptionName, consumerInfo);
            }
        } else if (consumerInfo.isDurable()) {
            registerExclusiveDurableSub(subscriptionName);
            return subscriptionName;
        } else {
            throw new IllegalStateException("Non-shared non-durable sub link naming is not handled by the tracker.");
        }
    }

    private void validateSubscriptionName(String subscriptionName) {
        if(subscriptionName == null) {
            throw new IllegalArgumentException("Subscription name must not be null.");
        }

        if(subscriptionName.isEmpty()) {
            throw new IllegalArgumentException("Subscription name must not be empty.");
        }

        if(subscriptionName.contains(SUB_NAME_DELIMITER)) {
            throw new IllegalArgumentException("Subscription name must not contain '" + SUB_NAME_DELIMITER +"' character.");
        }
    }

    private String getSharedDurableSubLinkName(String subscriptionName, JmsConsumerInfo consumerInfo) {
        JmsDestination topic = consumerInfo.getDestination();
        String selector = consumerInfo.getSelector();

        SubDetails subDetails = null;
        if(sharedDurableSubs.containsKey(subscriptionName)) {
            subDetails = sharedDurableSubs.get(subscriptionName);

            if(subDetails.matches(topic, selector)){
                subDetails.addSubscriber(consumerInfo);
            } else {
                throw new JMSRuntimeException("Subscription details dont match existing subscriber.");
            }
        } else {
            subDetails = new SubDetails(topic, selector, consumerInfo);
        }

        sharedDurableSubs.put(subscriptionName, subDetails);

        int count = subDetails.totalSubscriberCount();

        return getDurableSubscriptionLinkName(subscriptionName, consumerInfo.isExplicitClientID(), count);
    }

    private String getDurableSubscriptionLinkName(String subscriptionName, boolean hasClientID, int count) {
        String linkName = getFirstDurableSubscriptionLinkName(subscriptionName, hasClientID);
        if(count > 1) {
            if(hasClientID) {
                linkName += SUB_NAME_DELIMITER + count;
            } else {
                linkName += count;
            }
        }

        return linkName;
    }

    public String getFirstDurableSubscriptionLinkName(String subscriptionName, boolean hasClientID) {
        validateSubscriptionName(subscriptionName);

        String receiverLinkName = subscriptionName;
        if(!hasClientID) {
            receiverLinkName += SUB_NAME_DELIMITER + "global";
        }

        return receiverLinkName;
    }

    private String getSharedVolatileSubLinkName(String subscriptionName, JmsConsumerInfo consumerInfo) {
        JmsDestination topic = consumerInfo.getDestination();
        String selector = consumerInfo.getSelector();

        SubDetails subDetails = null;
        if(sharedVolatileSubs.containsKey(subscriptionName)) {
            subDetails = sharedVolatileSubs.get(subscriptionName);

            if(subDetails.matches(topic, selector)){
                subDetails.addSubscriber(consumerInfo);
            } else {
                throw new JMSRuntimeException("Subscription details dont match existing subscriber");
            }
        } else {
            subDetails = new SubDetails(topic, selector, consumerInfo);
        }

        sharedVolatileSubs.put(subscriptionName, subDetails);

        String receiverLinkName = subscriptionName + SUB_NAME_DELIMITER;
        int count = subDetails.totalSubscriberCount();

        if (consumerInfo.isExplicitClientID()) {
            receiverLinkName += "volatile" + count;
        } else {
            receiverLinkName += "global-volatile" + count;
        }

        return receiverLinkName;
    }

    private void registerExclusiveDurableSub(String subscriptionName) {
        exclusiveDurableSubs.add(subscriptionName);
    }

    /**
     * Checks if there is an exclusive durable subscription already
     * recorded as active with the given subscription name.
     *
     * @param subscriptionName name of subscription to check
     * @return true if there is an exclusive durable sub with this name already active
     */
    public boolean isActiveExclusiveDurableSub(String subscriptionName) {
        return exclusiveDurableSubs.contains(subscriptionName);
    }

    /**
     * Checks if there is a shared durable subscription already
     * recorded as active with the given subscription name.
     *
     * @param subscriptionName name of subscription to check
     * @return true if there is a shared durable sub with this name already active
     */
    public boolean isActiveSharedDurableSub(String subscriptionName) {
        return sharedDurableSubs.containsKey(subscriptionName);
    }

    /**
     * Checks if there is either a shared or exclusive durable subscription
     * already recorded as active with the given subscription name.
     *
     * @param subscriptionName name of subscription to check
     * @return true if there is a durable sub with this name already active
     */
    public boolean isActiveDurableSub(String subscriptionName) {
        return isActiveExclusiveDurableSub(subscriptionName) || isActiveSharedDurableSub(subscriptionName);
    }

    /**
     * Checks if there is an shared volatile subscription already
     * recorded as active with the given subscription name.
     *
     * @param subscriptionName name of subscription to check
     * @return true if there is a shared volatile sub with this name already active
     */
    public boolean isActiveSharedVolatileSub(String subscriptionName) {
        return sharedVolatileSubs.containsKey(subscriptionName);
    }

    public void consumerRemoved(JmsConsumerInfo consumerInfo) {
        String subscriptionName = consumerInfo.getSubscriptionName();

        if (subscriptionName != null && !subscriptionName.isEmpty()) {
            if (consumerInfo.isShared()) {
                if (consumerInfo.isDurable()) {
                    if(sharedDurableSubs.containsKey(subscriptionName)) {
                        SubDetails subDetails = sharedDurableSubs.get(subscriptionName);
                        subDetails.removeSubscriber(consumerInfo);

                        int count = subDetails.activeSubscribers();
                        if(count < 1) {
                            sharedDurableSubs.remove(subscriptionName);
                        }
                    }
                } else {
                    if(sharedVolatileSubs.containsKey(subscriptionName)) {
                        SubDetails subDetails = sharedVolatileSubs.get(subscriptionName);
                        subDetails.removeSubscriber(consumerInfo);

                        int count = subDetails.activeSubscribers();
                        if(count < 1) {
                            sharedVolatileSubs.remove(subscriptionName);
                        }
                    }
                }
            } else if (consumerInfo.isDurable()) {
                exclusiveDurableSubs.remove(subscriptionName);
            }
        }
    }

    private static class SubDetails {
        private JmsDestination topic = null;
        private String selector = null;
        private Set<JmsConsumerInfo> subscribers = new HashSet<>();
        private int totalSubscriberCount;

        public SubDetails(JmsDestination topic, String selector, JmsConsumerInfo info) {
            if(topic == null) {
                throw new IllegalArgumentException("Topic destination must not be null");
            }

            this.topic = topic;
            this.selector = selector;
            addSubscriber(info);
        }

        public void addSubscriber(JmsConsumerInfo info) {
            if(info == null) {
                throw new IllegalArgumentException("Consumer info must not be null");
            }

            totalSubscriberCount++;
            subscribers.add(info);
        }

        public void removeSubscriber(JmsConsumerInfo info) {
            subscribers.remove(info);
        }

        public int activeSubscribers() {
            return subscribers.size();
        }

        public int totalSubscriberCount() {
            return totalSubscriberCount;
        }

        public boolean matches(JmsDestination newTopic, String newSelector) {
            if(!topic.equals(newTopic)) {
                return false;
            }

            if (selector == null) {
                return newSelector == null;
            } else {
                return selector.equals(newSelector);
            }
        }

    }
}
