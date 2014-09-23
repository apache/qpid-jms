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

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.proton.amqp.Symbol;

/**
 * Support class containing constant values and static methods that are
 * used to map to / from AMQP Message types being sent or received.
 */
public final class AmqpMessageSupport {

    /**
     * The Annotation name to store the destination name that the Message
     * will be sent to.  The Message should also be tagged with the appropriate
     * destination attribute to allow the receiver to determine the correct
     * destination type.
     */
    public static final String AMQP_TO_ANNOTATION = "x-opt-to-type";

    /**
     * The Annotation name to store the destination name that the sender wants
     * to receive replies on.  The Message should also be tagged with the
     * appropriate destination attribute to allow the receiver to determine the
     * correct destination type.
     */
    public static final String AMQP_REPLY_TO_ANNOTATION = "x-opt-reply-type";

    /**
     * Attribute used to mark a destination as temporary.
     */
    public static final String TEMPORARY_ATTRIBUTE = "temporary";

    /**
     * Attribute used to mark a destination as being a Queue type.
     */
    public static final String QUEUE_ATTRIBUTES = "queue";

    /**
     * Attribute used to mark a destination as being a Topic type.
     */
    public static final String TOPIC_ATTRIBUTES = "topic";

    /**
     * Convenience value used to mark a destination as a Temporary Queue.
     */
    public static final String TEMP_QUEUE_ATTRIBUTES = TEMPORARY_ATTRIBUTE + "," + QUEUE_ATTRIBUTES;

    /**
     * Convenience value used to mark a destination as a Temporary Topic.
     */
    public static final String TEMP_TOPIC_ATTRIBUTES = TEMPORARY_ATTRIBUTE + "," + TOPIC_ATTRIBUTES;

    /**
     * Attribute used to mark the Application defined correlation Id that has been
     * set for the message.
     */
    public static final String JMS_APP_CORRELATION_ID = "x-opt-app-correlation-id";

    /**
     * Attribute used to mark the JMSType value set on the message.
     */
    public static final String JMS_TYPE = "x-opt-jms-type";

    /**
     * Attribute used to mark the JMS Type that the message instance represents.
     */
    public static final String JMS_MSG_TYPE = "x-opt-jms-msg-type";

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a generic JMS Message
     * which has no body.
     */
    public static final byte JMS_MESSAGE = 0;

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a JMS ObjectMessage
     * which has an Object value serialized in its message body.
     */
    public static final byte JMS_OBJECT_MESSAGE = 1;

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a JMS MapMessage
     * which has an Map instance serialized in its message body.
     */
    public static final byte JMS_MAP_MESSAGE = 2;

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a JMS BytesMessage
     * which has a body that consists of raw bytes.
     */
    public static final byte JMS_BYTES_MESSAGE = 3;

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a JMS StreamMessage
     * which has a body that is a structured collection of primitives values.
     */
    public static final byte JMS_STREAM_MESSAGE = 4;

    /**
     * Value mapping for JMS_MSG_TYPE which indicates the message is a JMS TextMessage
     * which has a body that contains a UTF-8 encoded String.
     */
    public static final byte JMS_TEXT_MESSAGE = 5;

    public static final String JMS_AMQP_TTL = "JMS_AMQP_TTL";
    public static final String JMS_AMQP_REPLY_TO_GROUP_ID = "JMS_AMQP_REPLY_TO_GROUP_ID";
    public static final String JMS_AMQP_TYPED_ENCODING = "JMS_AMQP_TYPED_ENCODING";

    /**
     * Lookup and return the correct Proton Symbol instance based on the given key.
     *
     * @param key
     *        the String value name of the Symbol to locate.
     *
     * @return the Symbol value that matches the given key.
     */
    public static Symbol getSymbol(String key) {
        return Symbol.valueOf(key);
    }

    /**
     * Given a JMS Destination object return the correct message annotations that
     * will identify the type of Destination the name represents, Queue. Topic, etc.
     *
     * @param destination
     *        The JMS Destination to be examined.
     *
     * @return the correct message annotation values to describe the given Destination.
     */
    public static String destinationAttributes(Destination destination) {
        if (destination instanceof Queue) {
            if (destination instanceof TemporaryQueue) {
                return TEMP_QUEUE_ATTRIBUTES;
            } else {
                return QUEUE_ATTRIBUTES;
            }
        }
        if (destination instanceof Topic) {
            if (destination instanceof TemporaryTopic) {
                return TEMP_TOPIC_ATTRIBUTES;
            } else {
                return TOPIC_ATTRIBUTES;
            }
        }

        return null;
    }
}
