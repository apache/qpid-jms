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
import org.apache.qpid.jms.provider.amqp.AmqpConnection;

/**
 * A set of static utility method useful when mapping JmsDestination types to / from the AMQP
 * destination fields in a Message that's being sent or received.
 */
public class AmqpDestinationHelper {

    public static final AmqpDestinationHelper INSTANCE = new AmqpDestinationHelper();

    public static final String TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME = "x-opt-to-type";
    public static final String REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME = "x-opt-reply-type";

    // For support of current byte type values
    public static final byte QUEUE_TYPE = 0x00;
    public static final byte TOPIC_TYPE = 0x01;
    public static final byte TEMP_QUEUE_TYPE = 0x02;
    public static final byte TEMP_TOPIC_TYPE = 0x03;

    // For support of old string type values
    public static final String QUEUE_ATTRIBUTE = "queue";
    public static final String TOPIC_ATTRIBUTE = "topic";
    public static final String TEMPORARY_ATTRIBUTE = "temporary";

    /**
     * Decode the provided To address, type description, and consumer destination
     * information such that an appropriate Destination object can be returned.
     *
     * If an address and type description is provided then this will be used to
     * create the Destination. If the type information is missing, it will be
     * derived from the consumer destination if present, or default to a queue
     * destination if not.
     *
     * If the address is null then the consumer destination is returned, unless
     * the useConsumerDestForTypeOnly flag is true, in which case null will be
     * returned.
     */
    public JmsDestination getJmsDestination(AmqpJmsMessageFacade message, JmsDestination consumerDestination) {
        String to = message.getToAddress();
        Byte typeByte = getTypeByte(message, TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        String name = stripPrefixIfNecessary(to, message.getConnection(), typeByte, consumerDestination);

        return createDestination(name, typeByte, consumerDestination, false);
    }

    public JmsDestination getJmsReplyTo(AmqpJmsMessageFacade message, JmsDestination consumerDestination) {
        String replyTo = message.getReplyToAddress();
        Byte typeByte = getTypeByte(message, REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        String name = stripPrefixIfNecessary(replyTo, message.getConnection(), typeByte, consumerDestination);

        return createDestination(name, typeByte, consumerDestination, true);
    }

    private String stripPrefixIfNecessary(String address, AmqpConnection conn, Byte typeByte, JmsDestination consumerDestination) {
        if (address == null) {
            return null;
        }

        if (typeByte == null) {
            String queuePrefix = conn.getQueuePrefix();
            if (queuePrefix != null && address.startsWith(queuePrefix)) {
                return address.substring(queuePrefix.length());
            }

            String topicPrefix = conn.getTopicPrefix();
            if (topicPrefix != null && address.startsWith(topicPrefix)) {
                return address.substring(topicPrefix.length());
            }
        } else if (typeByte == QUEUE_TYPE) {
            String queuePrefix = conn.getQueuePrefix();
            if (queuePrefix != null && address.startsWith(queuePrefix)) {
                return address.substring(queuePrefix.length());
            }
        } else if (typeByte == TOPIC_TYPE) {
            String topicPrefix = conn.getTopicPrefix();
            if (topicPrefix != null && address.startsWith(topicPrefix)) {
                return address.substring(topicPrefix.length());
            }
        }

        return address;
    }

    private JmsDestination createDestination(String address, Byte typeByte, JmsDestination consumerDestination, boolean useConsumerDestForTypeOnly) {
        if (address == null) {
            return useConsumerDestForTypeOnly ? null : consumerDestination;
        }

        if (typeByte != null) {
            switch (typeByte) {
            case QUEUE_TYPE:
                return new JmsQueue(address);
            case TOPIC_TYPE:
                return new JmsTopic(address);
            case TEMP_QUEUE_TYPE:
                return new JmsTemporaryQueue(address);
            case TEMP_TOPIC_TYPE:
                return new JmsTemporaryTopic(address);
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

        // fall back to a Queue Destination since we need a real JMS destination
        return new JmsQueue(address);
    }

    public void setToAddressFromDestination(AmqpJmsMessageFacade message, JmsDestination destination) {
        String address = getDestinationAddress(destination, message.getConnection());
        Object typeValue = toTypeAnnotation(destination);

        message.setToAddress(address);

        if (address == null || typeValue == null) {
            message.removeMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        } else {
            message.setMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeValue);
        }
    }

    public void setReplyToAddressFromDestination(AmqpJmsMessageFacade message, JmsDestination destination) {
        String replyToAddress = getDestinationAddress(destination, message.getConnection());
        Object typeValue = toTypeAnnotation(destination);

        message.setReplyToAddress(replyToAddress);

        if (replyToAddress == null || typeValue == null) {
            message.removeMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        } else {
            message.setMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeValue);
        }
    }

    private String getDestinationAddress(JmsDestination destination, AmqpConnection conn) {
        if (destination == null) {
            return null;
        }

        final String name = destination.getName();

        // Add prefix if necessary
        if (!destination.isTemporary()) {
            if (destination.isQueue()) {
                String queuePrefix = conn.getQueuePrefix();
                if (queuePrefix != null && !name.startsWith(queuePrefix)) {
                    return queuePrefix + name;
                }
            }

            if (destination.isTopic()) {
                String topicPrefix = conn.getTopicPrefix();
                if (topicPrefix != null && !name.startsWith(topicPrefix)) {
                    return topicPrefix + name;
                }
            }
        }

        return name;
    }

    /**
     * @return the annotation type value, or null if the supplied destination
     *         is null or can't be classified
     */
    private Object toTypeAnnotation(JmsDestination destination) {
        if (destination == null) {
            return null;
        }

        if (destination.isQueue()) {
            if (destination.isTemporary()) {
                return TEMP_QUEUE_TYPE;
            } else {
                return QUEUE_TYPE;
            }
        } else if (destination.isTopic()) {
            if (destination.isTemporary()) {
                return TEMP_TOPIC_TYPE;
            } else {
                return TOPIC_TYPE;
            }
        }

        return null;
    }

    Set<String> splitAttributesString(String typeString) {
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

    private Byte getTypeByte(AmqpJmsMessageFacade message, String annotationName) {
        Object typeAnnotation = message.getMessageAnnotation(annotationName);

        if (typeAnnotation == null) {
            // Doesn't exist, or null.
            return null;
        } else if (typeAnnotation instanceof Byte) {
            // Return the value found.
            return (Byte) typeAnnotation;
        } else {
            // Handle legacy strings.
            String typeString = String.valueOf(typeAnnotation);
            Set<String> typeSet = null;

            if (typeString != null) {
                typeSet = splitAttributesString(typeString);
            }

            if (typeSet != null && !typeSet.isEmpty()) {
                if (typeSet.contains(QUEUE_ATTRIBUTE)) {
                    if (typeSet.contains(TEMPORARY_ATTRIBUTE)) {
                        return TEMP_QUEUE_TYPE;
                    } else {
                        return QUEUE_TYPE;
                    }
                } else if (typeSet.contains(TOPIC_ATTRIBUTE)) {
                    if (typeSet.contains(TEMPORARY_ATTRIBUTE)) {
                        return TEMP_TOPIC_TYPE;
                    } else {
                        return TOPIC_TYPE;
                    }
                }
            }

            return null;
        }
    }

}
