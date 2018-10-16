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
package org.apache.qpid.jms.provider.amqp.message;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.message.Message;

import io.netty.buffer.ByteBuf;

/**
 * Support class containing constant values and static methods that are
 * used to map to / from AMQP Message types being sent or received.
 */
public final class AmqpMessageSupport {

    /**
     * Attribute used to mark the class type of JMS message that a particular message
     * instance represents, used internally by the client.
     */
    public static final Symbol JMS_MSG_TYPE = Symbol.valueOf("x-opt-jms-msg-type");

    /**
     * Attribute used to mark the Application defined delivery time assigned to the message
     */
    public static final Symbol JMS_DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");

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
     * Content type used to mark Data sections as containing a serialized java object.
     */
    public static final Symbol SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = Symbol.valueOf("application/x-java-serialized-object");

    /**
     * Content type used to mark Data sections as containing arbitrary bytes.
     */
    public static final Symbol OCTET_STREAM_CONTENT_TYPE = Symbol.valueOf("application/octet-stream");

    // For support of old string destination type annotations
    public static final Symbol LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL = Symbol.valueOf("x-opt-to-type");
    public static final Symbol LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL = Symbol.valueOf("x-opt-reply-type");
    public static final String LEGACY_QUEUE_ATTRIBUTE = "queue";
    public static final String LEGACY_TOPIC_ATTRIBUTE = "topic";
    public static final String LEGACY_TEMPORARY_ATTRIBUTE = "temporary";

    /**
     * Safe way to access message annotations which will check internal structure and
     * either return the annotation if it exists or null if the annotation or any annotations
     * are present.
     *
     * @param key
     *        the Symbol key to use to lookup an annotation.
     * @param messageAnnotations
     *        the AMQP message annotations object that is being examined.
     *
     * @return the given annotation value or null if not present in the message.
     */
    public static Object getMessageAnnotation(Symbol key, MessageAnnotations messageAnnotations) {
        if (messageAnnotations != null && messageAnnotations.getValue() != null) {
            Map<Symbol, Object> annotations = messageAnnotations.getValue();
            return annotations.get(key);
        }

        return null;
    }

    /**
     * Check whether the content-type field of the properties section (if present) in
     * the given message matches the provided string (where null matches if there is
     * no content type present.
     *
     * @param contentType
     *        content type string to compare against, or null if none
     * @param messageContentType
     *        the content type value read from an AMQP message object.
     *
     * @return true if content type matches
     */
    public static boolean isContentType(Symbol contentType, Symbol messageContentType) {
        if (contentType == null) {
            return messageContentType == null;
        } else if (messageContentType == null) {
            return false;
        } else {
            return contentType.equals(messageContentType);
        }
    }

    /**
     * Given a byte buffer that represents an encoded AMQP Message instance,
     * decode and return the Message.
     *
     * @param encodedBytes
     *      the bytes that represent an encoded AMQP Message.
     *
     * @return a new Message instance with the decoded data.
     */
    public static Message decodeMessage(ByteBuf encodedBytes) {
        // For now we must fully decode the message to get at the annotations.
        Message protonMessage = Message.Factory.create();
        protonMessage.decode(encodedBytes.array(), 0, encodedBytes.readableBytes());
        return protonMessage;
    }

    /**
     * Given a Message instance, encode the Message to the wire level representation
     * of that Message.
     *
     * @param message
     *      the Message that is to be encoded into the wire level representation.
     *
     * @return a buffer containing the wire level representation of the input Message.
     */
    public static ReadableBuffer encodeMessage(Message message) {
        final int BUFFER_SIZE = 4096;
        byte[] encodedMessage = new byte[BUFFER_SIZE];
        int encodedSize = 0;
        while (true) {
            try {
                encodedSize = message.encode(encodedMessage, 0, encodedMessage.length);
                break;
            } catch (java.nio.BufferOverflowException e) {
                encodedMessage = new byte[encodedMessage.length * 2];
            }
        }

        return ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(encodedMessage, 0, encodedSize));
    }
}
