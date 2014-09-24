/**
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
package org.apache.qpid.jms.provider.amqp.message;

import java.util.HashSet;
import java.util.Set;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.JmsTopic;

/**
 * A set of static utility method useful when mapping JmsDestination types to / from the AMQP
 * destination fields in a Message that's being sent or received.
 */
public class AmqpDestinationHelper {

    public static final AmqpDestinationHelper INSTANCE = new AmqpDestinationHelper();

    public static final String TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME = "x-opt-to-type";
    public static final String REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME = "x-opt-reply-type";

    static final String QUEUE_ATTRIBUTE = "queue";
    static final String TOPIC_ATTRIBUTE = "topic";
    static final String TEMPORARY_ATTRIBUTE = "temporary";

    public static final String QUEUE_ATTRIBUTES_STRING = QUEUE_ATTRIBUTE;
    public static final String TOPIC_ATTRIBUTES_STRING = TOPIC_ATTRIBUTE;
    public static final String TEMP_QUEUE_ATTRIBUTES_STRING = QUEUE_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;
    public static final String TEMP_TOPIC_ATTRIBUTES_STRING = TOPIC_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;

    // TODO - The Type Annotation seems like it could just be a byte value
    // TODO - How do we deal with the case where no type is present?

    /*
     *  One possible way to encode destination types that isn't a string.
     *
     *  public static final byte QUEUE_TYPE = 0x01;
     *  public static final byte TOPIC_TYPE = 0x02;
     *  public static final byte TEMP_MASK = 0x04;
     *  public static final byte TEMP_TOPIC_TYPE = TOPIC_TYPE | TEMP_MASK;
     *  public static final byte TEMP_QUEUE_TYPE = QUEUE_TYPE | TEMP_MASK;
     */

    /**
     * Decode the provided To address, type description, and consumer destination
     * information such that an appropriate Destination object can be returned.
     *
     * If an address and type description is provided then this will be used to
     * create the Destination. If the type information is missing, it will be
     * derived from the consumer destination if present, or default to a generic
     * destination if not.
     *
     * If the address is null then the consumer destination is returned, unless
     * the useConsumerDestForTypeOnly flag is true, in which case null will be
     * returned.
     */

    public JmsDestination getJmsDestination(AmqpJmsMessageFacade message, JmsDestination consumerDestination) {
        String to = message.getToAddress();
        String toTypeString = (String) message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        Set<String> typeSet = null;

        if (toTypeString != null) {
            typeSet = splitAttributes(toTypeString);
        }

        return createDestination(to, typeSet, consumerDestination, false);
    }

    public JmsDestination getJmsReplyTo(AmqpJmsMessageFacade message, JmsDestination consumerDestination) {
        String replyTo = message.getReplyToAddress();
        String replyToTypeString = (String) message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        Set<String> typeSet = null;

        if (replyToTypeString != null) {
            typeSet = splitAttributes(replyToTypeString);
        }

        return createDestination(replyTo, typeSet, consumerDestination, true);
    }

    private JmsDestination createDestination(String address, Set<String> typeSet, JmsDestination consumerDestination, boolean useConsumerDestForTypeOnly) {
        if (address == null) {
            return useConsumerDestForTypeOnly ? null : consumerDestination;
        }

        if (typeSet != null && !typeSet.isEmpty()) {
            if (typeSet.contains(QUEUE_ATTRIBUTE)) {
                if (typeSet.contains(TEMPORARY_ATTRIBUTE)) {
                    return new JmsTemporaryQueue(address);
                } else {
                    return new JmsQueue(address);
                }
            } else if (typeSet.contains(TOPIC_ATTRIBUTE)) {
                if (typeSet.contains(TEMPORARY_ATTRIBUTE)) {
                    return new JmsTemporaryTopic(address);
                } else {
                    return new JmsTopic(address);
                }
            }
        }

        if (consumerDestination.isQueue()) {
            if (consumerDestination.isTemporary()) {
                return new JmsTemporaryQueue(address);
            } else {
                return new JmsQueue(address);
            }
        } else if (consumerDestination.isTopic()) {
            if (consumerDestination.isTemporary()) {
                return new JmsTemporaryTopic(address);
            } else {
                return new JmsTopic(address);
            }
        }

        // fall back to a straight Destination
        // TODO - We don't have a non-abstract destination to create right now
        //        and JMS doesn't really define a true non Topic / Queue destination
        //        so how this would be handled elsewhere seems a mystery.
        return null;
    }

    public void setToAddressFromDestination(AmqpJmsMessageFacade message, JmsDestination destination) {
        String address = destination.getName();
        String typeString = toTypeAnnotation(destination);

        message.setToAddress(address);

        if (address == null || typeString == null) {
            message.removeAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        } else {
            message.setAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    public void setReplyToAddressFromDestination(AmqpJmsMessageFacade message, JmsDestination destination) {
        String replyToAddress = destination.getName();
        String typeString = toTypeAnnotation(destination);

        message.setReplyToAddress(replyToAddress);

        if (replyToAddress == null || typeString == null) {
            message.removeAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        } else {
            message.setAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    /**
     * @return the annotation type string, or null if the supplied destination
     *         is null or can't be classified
     */
    private String toTypeAnnotation(JmsDestination destination) {
        if (destination == null) {
            return null;
        }

        if (destination.isQueue()) {
            if (destination.isTemporary()) {
                return TEMP_QUEUE_ATTRIBUTES_STRING;
            } else {
                return QUEUE_ATTRIBUTES_STRING;
            }
        } else if (destination.isTopic()) {
            if (destination.isTemporary()) {
                return TEMP_TOPIC_ATTRIBUTES_STRING;
            } else {
                return TOPIC_ATTRIBUTES_STRING;
            }
        }

        return null;
    }

    public Set<String> splitAttributes(String typeString) {
        if (typeString == null) {
            return null;
        }

        HashSet<String> typeSet = new HashSet<String>();

        // Split string on commas and their surrounding whitespace
        for (String attr : typeString.split("\\s*,\\s*")) {
            // ignore empty values
            if (!attr.equals("")) {
                typeSet.add(attr);
            }
        }

        return typeSet;
    }
}
